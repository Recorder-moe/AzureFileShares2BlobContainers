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
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace AzureFileShares2BlobContainers
{
    public class AzureFileShares2BlobContainers
    {
        private readonly ILogger<AzureFileShares2BlobContainers> _logger;

        public AzureFileShares2BlobContainers(ILogger<AzureFileShares2BlobContainers> log)
        {
            _logger = log;
        }

        [FunctionName("AzureFileShares2BlobContainers")]
        [OpenApiOperation(operationId: "Run", tags: new[] { "name" })]
        [OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query)]
        [OpenApiParameter(name: "videoId", In = ParameterLocation.Query, Required = true, Type = typeof(string), Description = "The **VideoId** to process")]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "text/plain", bodyType: typeof(string), Description = "The OK response")]
        public async Task<IActionResult> RunAsync(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            [Blob("livestream-recorder"), StorageAccount("AzureStorage")] BlobContainerClient blobContainerClient)
        {
            var videoId = req.GetQueryParameterDictionary()["videoId"];
            var shareDirectoryClient = await ConnectFileShareAsync();
            await GetFilesAsync(shareDirectoryClient, videoId);
            await UploadToBlobContainerAsync(blobContainerClient, videoId);
            await DeleteFilesFromFileShareAsync(shareDirectoryClient, videoId);

            return new OkResult();
        }

        private static string[] GetFileNames(string videoId) => new[] { $"{videoId}.mp4", $"{videoId}.json", $"{videoId}.chat.json" };

        private async Task<ShareDirectoryClient> ConnectFileShareAsync()
        {
            // Get the connection string from app settings
            string connectionString = Environment.GetEnvironmentVariable("FileShareConnextionString");

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

        private async Task GetFilesAsync(ShareDirectoryClient shareDirectoryClient, string videoId)
        {
            var files = GetFileNames(videoId);

            foreach (var filename in files)
            {
                var shareFileClient = shareDirectoryClient.GetFileClient(filename);
                // Ensure that the file exists
                if (await shareFileClient.ExistsAsync())
                {
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
        }

        private async Task UploadToBlobContainerAsync(BlobContainerClient blobContainerClient, string videoId)
        {
            var blobs = new List<BlobClient>(){
                 blobContainerClient.GetBlobClient($"{videoId}.mp4")
            };
            foreach (var blobClient in blobs)
            {
                if (blobClient.Exists())
                {
                    _logger.LogWarning("Blob already exists {filename}", blobClient.Name);
                }

                try
                {
                    _logger.LogInformation("Start to upload {filename} to blob storage {name}", blobClient.Name, blobContainerClient.Name);
                    var response = await blobClient.UploadAsync(blobClient.Name,
                                                                new BlobUploadOptions
                                                                {
                                                                    HttpHeaders = new BlobHttpHeaders
                                                                    {
                                                                        ContentType = MimeMapping.MimeUtility.GetMimeMapping(blobClient.Name)
                                                                    }
                                                                });
                    _logger.LogInformation("Upload file {filename} to azure finish.", blobClient.Name);
                    _logger.LogDebug("The blob last modified time is {lastModified}", response.Value.LastModified);
                }
                finally { File.Delete(blobClient.Name); }

                Dictionary<string, string> metadata = new()
                {
                    { "videoId", videoId }
                };
                await blobClient.SetMetadataAsync(metadata);
                return;
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

