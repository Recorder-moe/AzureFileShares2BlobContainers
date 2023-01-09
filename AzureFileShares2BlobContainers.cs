using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;
using DataSizeUnits;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.OpenApi.Models;
using Serilog;
using Serilog.Context;
using Serilog.Events;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;

namespace AzureFileShares2BlobContainers;

public class AzureFileShares2BlobContainers
{
    private static ILogger _logger;
    private readonly string[] _extensions = new string[]
    {
        ".jpg",
        ".jpeg",
        ".png",
        ".webp",
        ".mp4",
        ".webm",
        ".mkv",
        ".info.json",
        ".live_chat.json",
        ".description"
    };

    public static ILogger Logger
    {
        get
        {
            if (null == _logger
                || _logger.GetType() != typeof(Serilog.Core.Logger))
            {
                _logger = MakeLogger();
            }
            return _logger;
        }
        set => _logger = value;
    }

    public AzureFileShares2BlobContainers()
    {
#if DEBUG
        Serilog.Debugging.SelfLog.Enable(msg => Console.WriteLine(msg));
#endif

        Logger.Verbose("Starting up...");
    }

    private string[] GetFileNames(string videoId) => _extensions.Select(p => videoId + p).ToArray();

    public static ILogger MakeLogger()
    {
        Serilog.Debugging.SelfLog.Enable(msg => Console.WriteLine(msg));

        var logger = new LoggerConfiguration()
                        .MinimumLevel.Verbose()
                        .MinimumLevel.Override("Microsoft", LogEventLevel.Fatal)
                        .MinimumLevel.Override("Microsoft.Hosting.Lifetime", LogEventLevel.Fatal)
                        .MinimumLevel.Override("System", LogEventLevel.Fatal)
                        .WriteTo.Console(outputTemplate: "{Timestamp:HH:mm:ss} [{Level:u3}] {Message:lj} <{SourceContext}>{NewLine}{Exception}",
                                         restrictedToMinimumLevel: LogEventLevel.Verbose)
                        .WriteTo.Seq(serverUrl: Environment.GetEnvironmentVariable("Seq_ServerUrl"),
                                     apiKey: Environment.GetEnvironmentVariable("Seq_ApiKey"),
                                     restrictedToMinimumLevel: LogEventLevel.Verbose)
                        .Enrich.FromLogContext()
                        .CreateLogger();
        return logger;
    }

    /// <summary>
    /// Entrypoint!
    /// </summary>
    /// <param name="req"></param>
    /// <param name="blobContainerClient"></param>
    /// <returns></returns>
    [FunctionName("AzureFileShares2BlobContainers")]
    [OpenApiOperation(operationId: "Run", tags: new[] { "name" })]
    [OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query)]
    [OpenApiParameter(name: "videoId", In = ParameterLocation.Query, Required = true, Type = typeof(string), Description = "The **VideoId** to process")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "text/plain", bodyType: typeof(string), Description = "The OK response")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.InternalServerError, contentType: "text/html", bodyType: typeof(string), Description = "The Error response")]
    public async Task<IActionResult> RunAsync(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
        [Blob("livestream-recorder")] BlobContainerClient blobContainerClient)
    {
        try
        {
            var videoId = req.GetQueryParameterDictionary()["videoId"];
            _ = LogContext.PushProperty("videoId", videoId);
            Logger.Verbose("API triggered! {apiname}", nameof(RunAsync));

            var files = GetFileNames(videoId);
            var shareDirectoryClient = await GetFileShareClientAsync();

            var tasks = new List<Task>();
            foreach (var filename in files)
            {
                CancellationTokenSource cancellation = new();
                tasks.Add(Task.Run(async () =>
                {
                    using (Stream stream = await GetStreamFromFileShareAsync(shareDirectoryClient, filename, cancellation))
                    {
                        await UploadToBlobContainerAsync(blobContainerClient, filename, stream, cancellation.Token);
                    }
                    await DeleteFromFileShareAsync(shareDirectoryClient, filename, cancellation.Token);
                }));
            }

            await Task.WhenAll(tasks);
            Logger.Information("Finish task {videoId}", videoId);
        }
        catch (Exception e)
        {
            Logger.Error("Unhandled exception in {apiname}: {exception}", nameof(RunAsync), e);
            return new InternalServerErrorResult();
        }
        return new OkResult();
    }

    private static async Task<ShareDirectoryClient> GetFileShareClientAsync()
    {
        // Get the connection string from app settings
        string connectionString = Environment.GetEnvironmentVariable("AzureStorage");

        var shareClient = new ShareClient(connectionString, "livestream-recorder");

        // Ensure that the share exists
        if (!await shareClient.ExistsAsync())
        {
            Logger.Fatal("Share not exists: {fileShareName}!!", shareClient.Name);
            throw new Exception("File Share does not exist.");
        }

        // Get a reference to the directory
        ShareDirectoryClient rootdirectory = shareClient.GetRootDirectoryClient();
        return rootdirectory;
    }

    private static async Task<Stream> GetStreamFromFileShareAsync(ShareDirectoryClient shareDirectoryClient,
                                                          string filename,
                                                          CancellationTokenSource cancellationTokenSource)
    {
        var shareFileClient = shareDirectoryClient.GetFileClient(filename);

        // Skip if the file is not exists
        if (!await shareFileClient.ExistsAsync())
        {
            Logger.Debug("Share File not exists, skip: {filename}", filename);
            cancellationTokenSource.Cancel();
            return null;
        }

        Logger.Information("Share File exists: {sharename} {path}", shareFileClient.ShareName, shareFileClient.Path);

        try
        {
            // Open stream
            var stream = await shareFileClient.OpenReadAsync(cancellationToken: cancellationTokenSource.Token);
            Logger.Information("Get file stream. {filename} {filelength}", filename, stream.Length);
            return stream;
        }
        catch (ShareFileModifiedException e)
        {
            Logger.Error("Share File is currently being modified: {filename}", filename);
            Logger.Error("{error}: {errorMessage}", nameof(e), e.Message);
            cancellationTokenSource.Cancel();
            return null;
        }
    }

    private static async Task UploadToBlobContainerAsync(BlobContainerClient blobContainerClient,
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

        var blobClient = blobContainerClient.GetBlobClient(filename);
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

            stream.Seek(0, SeekOrigin.Begin);
            stopWatch.Start();
            _ = await blobClient.UploadAsync(
                content: stream,
                httpHeaders: new BlobHttpHeaders { ContentType = MimeMapping.MimeUtility.GetMimeMapping(filename) },
                accessTier: AccessTier.Cool,
                metadata: metaTags,
                progressHandler: new Progress<long>(progress =>
                {
                    double _percentage = Math.Round(((double)progress) / fileSize * 100);
                    if (_percentage != percentage)
                    {
                        percentage = _percentage;
                        Logger.Debug("{filename} Uploading...{progress}%, at speed {speed}/s",
                                      filename,
                                      _percentage,
                                      new DataSize(progress / (long)stopWatch.Elapsed.TotalSeconds).Normalize().ToString());
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

    private static async Task DeleteFromFileShareAsync(ShareDirectoryClient shareDirectoryClient, string filename, CancellationToken cancellation)
    {
        if (cancellation.IsCancellationRequested) return;

        var shareFileClient = shareDirectoryClient.GetFileClient(filename);
        var response = await shareFileClient.DeleteIfExistsAsync(cancellationToken: cancellation);
        if (response.Value)
        {
            Logger.Information("File {filename} deleted from File Share {fileShareName}", filename, shareDirectoryClient.ShareName);
        }
    }
}