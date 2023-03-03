using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;
using Serilog;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace AzureFileShares2BlobContainers.Services;

public static class AFSService
{
    private static ILogger Logger => Helper.Log.Logger;

    public static async Task<ShareDirectoryClient> GetFileShareClientAsync()
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

    public static async Task<Stream> GetStreamFromFileShareAsync(ShareDirectoryClient shareDirectoryClient,
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

        Logger.Debug("Share File exists: {sharename} {path}", shareFileClient.ShareName, shareFileClient.Path);

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

    public static async Task DeleteFromFileShareAsync(ShareDirectoryClient shareDirectoryClient, string filename, CancellationToken cancellation)
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
