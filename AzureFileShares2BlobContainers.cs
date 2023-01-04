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
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
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
        private readonly ILogger<AzureFileShares2BlobContainers> _logger;
        private readonly string[] _extensions;

        public AzureFileShares2BlobContainers(ILogger<AzureFileShares2BlobContainers> log,
            IConfiguration configuration)
        {
            _logger = log;
            _extensions = configuration.GetSection("FileExtensions")
                                       .Get<string[]>()
                                       .Select(p => p.StartsWith('.') ? p : $".{p}")
                                       .ToArray();
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
                _logger.LogError("Share not exists: {fileShareName}", shareClient.Name);
                throw new Exception("File Share does not exist");
            }

            _logger.LogDebug($"Share exists: {shareClient.Name}");

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

                _logger.LogInformation($"File exists: {shareFileClient.Name}");

                // Download the file
                ShareFileDownloadInfo download = await shareFileClient.DownloadAsync();

                // Save the data to a local file, overwrite if the file already exists
                using FileStream stream = File.OpenWrite(shareFileClient.Name);
                await download.Content.CopyToAsync(stream);
                await stream.FlushAsync();
                stream.Close();

                // Display where the file was saved
                _logger.LogInformation($"File downloaded: {stream.Name}");
            }
        }

        private async Task UploadToBlobContainerAsync(BlobContainerClient blobContainerClient, string videoId)
        {
            var files = GetFileNames(videoId);
            foreach (var filename in files)
            {
                if (!File.Exists(filename)) continue;

                var blobClient = blobContainerClient.GetBlobClient(filename);
                if (blobClient.Exists())
                {
                    _logger.LogWarning("Blob already exists {filename}", blobClient.Name);
                }

                try
                {
                    using FileStream fs = new(blobClient.Name, FileMode.Open, FileAccess.Read);
                    _logger.LogInformation("Start Upload {path} to azure storage", blobClient.Name);

                    long fileSize = new FileInfo(blobClient.Name).Length;

                    double percentage = 0;

                    _ = await blobClient.UploadAsync(
                        content: fs,
                        httpHeaders: new BlobHttpHeaders { ContentType = MimeMapping.MimeUtility.GetMimeMapping(blobClient.Name) },
                        accessTier: AccessTier.Cool,
                        metadata: new Dictionary<string, string>() { { "id", videoId }, { "fileSize", fileSize.ToString() } },
                        progressHandler: new Progress<long>(progress =>
                        {
                            double _percentage = Math.Round(((double)progress) / fileSize * 100);
                            if (_percentage != percentage)
                            {
                                percentage = _percentage;
                                _logger.LogTrace("Uploading...{progress}% {path}", _percentage, blobClient.Name);
                            }
                        })
                    );
                    _ = await blobClient.SetTagsAsync(new Dictionary<string, string>() { { "id", videoId } });
                    _logger.LogInformation("Finish Upload {path} to azure storage", blobClient.Name);

                    return;
                }
                catch (Exception e)
                {
                    _logger.LogError("Upload Failed: {fileName}", Path.GetFileName(blobClient.Name));
                    _logger.LogError("{errorMessage}", e.Message);
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
                    _logger.LogInformation("File {filename} deleted from File Share {fileShareName}", filename, shareDirectoryClient.ShareName);
                }
                else
                {
                    _logger.LogWarning("File {filename} not found from File Share {fileShareName}", filename, shareDirectoryClient.ShareName);
                }
            }
        }
    }
}

