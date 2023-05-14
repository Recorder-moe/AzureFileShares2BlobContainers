using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using DataSizeUnits;
using Serilog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace AzureFileShares2BlobContainers.Services;

public static class ABSService
{
    private static ILogger Logger => Helper.Log.Logger;


    public static async Task UploadToBlobContainerAsync(BlobContainerClient blobContainerClient,
                                                         string filename,
                                                         Stream stream,
                                                         CancellationToken cancellation)
    {
        if (cancellation.IsCancellationRequested) return;

        if (null == stream)
        {
            throw new ArgumentNullException(nameof(stream), $"Stream is null while uploading to Blob Storage. {filename}");
        }
        var videoId = filename.Split('.')[0];

        var blobClient = blobContainerClient.GetBlobClient($"/videos/{filename}");
        if (blobClient.Exists(cancellation))
        {
            Logger.Warning("Blob already exists {filename}", blobClient.Name);
        }

        try
        {
            Logger.Information("Start streaming {filename} from azure file share to azure blob storage", filename);

            long fileSize = stream.Length;

            double percentage = 0;
            var stopWatch = new Stopwatch();

            var metaTags = new Dictionary<string, string>()
            {
                { "id", videoId },
                { "fileSize", fileSize.ToString() }
            };

            var accessTier = Environment.GetEnvironmentVariable("VideoBlobTier") == "Hot"
                ? AccessTier.Hot
                : AccessTier.Cool;

            stream.Seek(0, SeekOrigin.Begin);
            stopWatch.Start();
            _ = await blobClient.UploadAsync(
                content: stream,
                httpHeaders: new BlobHttpHeaders { ContentType = "video/mp4" },
                accessTier: accessTier,
                metadata: metaTags,
                progressHandler: new Progress<long>(progress =>
                {
                    if (fileSize == 0 || (long)stopWatch.Elapsed.TotalSeconds == 0) return;

                    double _percentage = Math.Floor((double)progress / (double)fileSize * 100);
                    if (_percentage != percentage)
                    {
                        percentage = _percentage;
                        Logger.Verbose("{filename} Uploading...{progress}%, at speed {speed}/s",
                                      filename,
                                      _percentage,
                                      new DataSize((long)(progress / stopWatch.Elapsed.TotalSeconds)).Normalize().ToString());
                    }
                }),
                cancellationToken: cancellation);
            stopWatch.Stop();
            _ = await blobClient.SetTagsAsync(metaTags, cancellationToken: cancellation);
            Logger.Information("Finish Upload {filename} to azure storage, at speed {speed}/s",
                filename,
                new DataSize((long)(fileSize / stopWatch.Elapsed.TotalSeconds)).Normalize().ToString());

            return;
        }
        catch (Exception e)
        {
            Logger.Error("Upload Failed: {filename}", Path.GetFileName(filename));
            Logger.Error("{errorMessage}", e.Message);
            throw;
        }
    }
}
