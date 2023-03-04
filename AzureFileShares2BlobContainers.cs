using Azure.Storage.Blobs;
using AzureFileShares2BlobContainers.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Serilog;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace AzureFileShares2BlobContainers;

public class AzureFileShares2BlobContainers
{
    private static ILogger Logger => Helper.Log.Logger;

    [FunctionName("AzureFileShares2BlobContainers")]
    [OpenApiOperation(operationId: "Run", tags: new[] { "name" })]
    [OpenApiParameter(name: "videoId", In = ParameterLocation.Query, Required = true, Type = typeof(string), Description = "The **VideoId** to process")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.Accepted, contentType: "application/json", bodyType: typeof(string), Description = "The response when the function is accepted.")]
    public async Task<IActionResult> RunAsync(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
        [DurableClient] IDurableOrchestrationClient starter)
    {
        var videoId = req.GetQueryParameterDictionary()["videoId"];
        Logger.Information("API triggered! {apiname}", nameof(RunAsync));

        var instanceId = await starter.StartNewAsync<string>("RunOrchestratorAsync", videoId);

        return starter.CreateCheckStatusResponse(req, instanceId);
    }

    [FunctionName("RunOrchestratorAsync")]
    public async Task<string> RunOrchestratorAsync(
        [OrchestrationTrigger] IDurableOrchestrationContext context)
    {
        var videoId = context.GetInput<string>();

        var filename = videoId + ".mp4";

        await context.CallActivityAsync(nameof(CopyFileFromFileShareToBlobStorage), filename);
        await context.CallActivityAsync(nameof(DeleteFileFromFileShare), filename);

        Logger.Information("Finish task {videoId}", videoId);

        return "Done";
    }

    [FunctionName(nameof(CopyFileFromFileShareToBlobStorage))]
    public async Task CopyFileFromFileShareToBlobStorage(
        [ActivityTrigger] string filename,
        [Blob("livestream-recorder")] BlobContainerClient blobContainerClient)
    {
        CancellationTokenSource cancellation = new();
        var shareDirectoryClient = await AFSService.GetFileShareClientAsync();

        using (var stream = await AFSService.GetStreamFromFileShareAsync(shareDirectoryClient, filename, cancellation))
        {
            await ABSService.UploadToBlobContainerAsync(blobContainerClient, filename, stream, cancellation.Token);
        }

        Logger.Information("Copied {filename} to blob container", filename);
    }

    [FunctionName(nameof(DeleteFileFromFileShare))]
    public async Task DeleteFileFromFileShare(
        [ActivityTrigger] string filename)
    {
        var shareDirectoryClient = await AFSService.GetFileShareClientAsync();

        await AFSService.DeleteFromFileShareAsync(shareDirectoryClient, filename);

        Logger.Information("Deleted {filename} from file share", filename);
    }
}