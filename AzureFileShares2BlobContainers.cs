using Azure.Storage.Blobs;
using AzureFileShares2BlobContainers.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.OpenApi.Models;
using Serilog;
using Serilog.Context;
using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;

namespace AzureFileShares2BlobContainers;

public class AzureFileShares2BlobContainers
{
    private static ILogger Logger => Helper.Log.Logger;

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

            var filename = videoId + ".mp4";
            var shareDirectoryClient = await AFSService.GetFileShareClientAsync();

            CancellationTokenSource cancellation = new();
            using (Stream stream = await AFSService.GetStreamFromFileShareAsync(shareDirectoryClient, filename, cancellation))
            {
                await ABSService.UploadToBlobContainerAsync(blobContainerClient, filename, stream, cancellation.Token);
            }
            await AFSService.DeleteFromFileShareAsync(shareDirectoryClient, filename, cancellation.Token);

            Logger.Information("Finish task {videoId}", videoId);
        }
        catch (Exception e)
        {
            Logger.Error("Unhandled exception in {apiname}: {exception}", nameof(RunAsync), e);
            return new InternalServerErrorResult();
        }
        return new OkResult();
    }
}