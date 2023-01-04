using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.OpenApi.Models;
using Serilog;
using Serilog.Events;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace AzureFileShares2BlobContainers
{
    public class AzureFileShares2BlobContainers
    {
        private readonly ILogger _logger;
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
            ".live_chat.json"
        };

//#if !DEBUG
        private readonly string _tempDir = @"C:\home\data";
//#else
//        private readonly string _tempDir = Path.GetTempPath();
//#endif

        public AzureFileShares2BlobContainers()
        {
#if DEBUG
            Serilog.Debugging.SelfLog.Enable(msg => Console.WriteLine(msg));
#endif

            _logger = new LoggerConfiguration()
                            .MinimumLevel.Override("Microsoft", LogEventLevel.Fatal)
                            .MinimumLevel.Override("Microsoft.Hosting.Lifetime", LogEventLevel.Fatal)
                            .MinimumLevel.Override("System", LogEventLevel.Fatal)
                            .WriteTo.Console(outputTemplate: "{Timestamp:HH:mm:ss} [{Level:u3}] {Message:lj} <{SourceContext}>{NewLine}{Exception}",
                                             restrictedToMinimumLevel: LogEventLevel.Verbose)
                            .WriteTo.Seq(serverUrl: Environment.GetEnvironmentVariable("Seq_ServerUrl"),
                                         apiKey: Environment.GetEnvironmentVariable("Seq_ApiKey"),
                                         restrictedToMinimumLevel: LogEventLevel.Debug)
                            .CreateLogger();

            _logger.Verbose("Starting up...");
        }

        private string[] GetFileNames(string videoId) => _extensions.Select(p => videoId + p).ToArray();

        [FunctionName("AzureFileShares2BlobContainers")]
        [OpenApiOperation(operationId: "Run", tags: new[] { "name" })]
        [OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query)]
        [OpenApiParameter(name: "videoId", In = ParameterLocation.Query, Required = true, Type = typeof(string), Description = "The **VideoId** to process")]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "text/plain", bodyType: typeof(string), Description = "The OK response")]
        public async Task<IActionResult> RunAsync(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            [Blob("livestream-recorder"), StorageAccount("AzureStorage")] BlobContainerClient blobContainerClient)
        {
            var videoId = req.GetQueryParameterDictionary()["videoId"];
            var shareDirectoryClient = await ConnectFileShareAsync();
            await DownloadFromFileShareAsync(shareDirectoryClient, videoId);
            await UploadToBlobContainerAsync(blobContainerClient, videoId);
            await DeleteFilesFromFileShareAsync(shareDirectoryClient, videoId);

            return new OkResult();
        }

        private async Task<ShareDirectoryClient> ConnectFileShareAsync()
        {
            // Get the connection string from app settings
            string connectionString = Environment.GetEnvironmentVariable("AzureStorage");

            var shareClient = new ShareClient(connectionString, "livestream-recorder");

            // Ensure that the share exists
            if (!await shareClient.ExistsAsync())
            {
                _logger.Fatal("Share not exists: {fileShareName}", shareClient.Name);
                throw new Exception("File Share does not exist");
            }

            _logger.Debug("File Share exists: {sharename}", shareClient.Name);

            // Get a reference to the directory
            ShareDirectoryClient rootdirectory = shareClient.GetRootDirectoryClient();
            return rootdirectory;
        }

        private async Task DownloadFromFileShareAsync(ShareDirectoryClient shareDirectoryClient, string videoId)
        {
            var files = GetFileNames(videoId);

            foreach (var filename in files)
            {
                var shareFileClient = shareDirectoryClient.GetFileClient(filename);
                // Ensure that the file exists
                if (!await shareFileClient.ExistsAsync()) continue;

                _logger.Information("Share File exists: {sharename} {path}",shareFileClient.ShareName, shareFileClient.Path);

                // Download the file
                ShareFileDownloadInfo download = await shareFileClient.DownloadAsync();

                // Save the data to a local file, overwrite if the file already exists
                using FileStream stream = File.OpenWrite(Path.Combine(_tempDir, filename));
                await download.Content.CopyToAsync(stream);
                await stream.FlushAsync();
                stream.Close();

                // Display where the file was saved
                _logger.Information("File downloaded: {filepath}", stream.Name);
            }
        }

        private async Task UploadToBlobContainerAsync(BlobContainerClient blobContainerClient, string videoId)
        {
            var files = GetFileNames(videoId);
            foreach (var filename in files)
            {
                var filepath = Path.Combine(_tempDir, filename);
                if (!File.Exists(filepath)) continue;

                var blobClient = blobContainerClient.GetBlobClient(filename);
                if (blobClient.Exists())
                {
                    _logger.Warning("Blob already exists {filename}", blobClient.Name);
                }

                try
                {
                    using FileStream fs = new(filepath, FileMode.Open, FileAccess.Read);
                    _logger.Information("Start Upload {filepath} to azure storage", filepath);

                    long fileSize = new FileInfo(filepath).Length;

                    double percentage = 0;

                    _ = await blobClient.UploadAsync(
                        content: fs,
                        httpHeaders: new BlobHttpHeaders { ContentType = MimeMapping.MimeUtility.GetMimeMapping(filename) },
                        accessTier: AccessTier.Cool,
                        metadata: new Dictionary<string, string>() { { "id", videoId }, { "fileSize", fileSize.ToString() } },
                        progressHandler: new Progress<long>(progress =>
                        {
                            double _percentage = Math.Round(((double)progress) / fileSize * 100);
                            if (_percentage != percentage)
                            {
                                percentage = _percentage;
                                _logger.Verbose("{filename} Uploading...{progress}%", filename, _percentage);
                            }
                        })
                    );
                    _ = await blobClient.SetTagsAsync(new Dictionary<string, string>() { { "id", videoId } });
                    _logger.Information("Finish Upload {filepath} to azure storage", filepath);

                    return;
                }
                catch (Exception e)
                {
                    _logger.Error("Upload Failed: {filepath}", Path.GetFileName(filepath));
                    _logger.Error("{errorMessage}", e.Message);
                    throw;
                }
            }
        }

        private async Task DeleteFilesFromFileShareAsync(ShareDirectoryClient shareDirectoryClient, string videoId)
        {
            var files = GetFileNames(videoId);
            foreach (var filename in files)
            {
                var shareFileClient = shareDirectoryClient.GetFileClient(filename);
                var response = await shareFileClient.DeleteIfExistsAsync();
                if (response.Value)
                {
                    _logger.Information("File {filename} deleted from File Share {fileShareName}", filename, shareDirectoryClient.ShareName);
                }
            }
        }
    }
}

