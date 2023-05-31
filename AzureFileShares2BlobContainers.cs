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
using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace AzureFileShares2BlobContainers;

public class AzureFileShares2BlobContainers
{
    private static ILogger Logger => Helper.Log.Logger;

    [FunctionName("AzureFileShares2BlobContainers")]
    [OpenApiOperation(operationId: "Run", tags: new[] { "name" })]
    [OpenApiParameter(name: "filename", In = ParameterLocation.Query, Required = true, Type = typeof(string), Description = "The filename to process")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.Accepted, contentType: "application/json", bodyType: typeof(string), Description = "The response when the function is accepted.")]
    public async Task<IActionResult> RunAsync(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
        [DurableClient] IDurableOrchestrationClient starter)
    {
        var filename = req.GetQueryParameterDictionary()["filename"];
        Logger.Information("API triggered! {apiname}", nameof(RunAsync));

        var instanceId = await starter.StartNewAsync<string>("RunOrchestratorAsync", filename);

        return starter.CreateCheckStatusResponse(req, instanceId);
    }

    [FunctionName("RunOrchestratorAsync")]
    public async Task<string> RunOrchestratorAsync(
        [OrchestrationTrigger] IDurableOrchestrationContext context)
    {
        var filenamePrefix = context.GetInput<string>();

        await context.CallActivityAsync(nameof(CopyFileFromFileShareToBlobStorage), filenamePrefix);
        await context.CallActivityAsync(nameof(DeleteFileFromFileShare), filenamePrefix);

        Logger.Information("Finish task {filename}", filenamePrefix);

        return "Done";
    }

    [FunctionName(nameof(CopyFileFromFileShareToBlobStorage))]
    public async Task CopyFileFromFileShareToBlobStorage(
        [ActivityTrigger] string filenamePrefix,
        [Blob("livestream-recorder")] BlobContainerClient blobContainerClient)
    {
        CancellationTokenSource cancellation = new();
        var file = await AFSService.GetShareFileItem(filenamePrefix, cancellation);

        using (var stream = await AFSService.GetStreamFromFileShareAsync(file, cancellation))
        {
            await ABSService.UploadToBlobContainerAsync(blobContainerClient, file.Name, stream, cancellation.Token);
        }

        Logger.Information("Copied {filename} to blob container", filenamePrefix);
    }

    [FunctionName(nameof(DeleteFileFromFileShare))]
    public async Task DeleteFileFromFileShare(
        [ActivityTrigger] string filename)
    {
        var file = await AFSService.GetShareFileItem(filename);
        await AFSService.DeleteFromFileShareAsync(file);

        Logger.Information("Deleted {filename} from file share", filename);
    }
}