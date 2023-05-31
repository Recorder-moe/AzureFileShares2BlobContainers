using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;
using Serilog;
using System;
using System.IO;
using System.Linq;
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
        string shareName = Environment.GetEnvironmentVariable("FileShareName");

        var shareClient = new ShareClient(connectionString, shareName);

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

    public static async Task<Stream> GetStreamFromFileShareAsync(ShareFileItem shareFileItem,
                                                                 CancellationTokenSource cancellationTokenSource)
    {
        var shareFileClient = (await GetFileShareClientAsync()).GetFileClient(shareFileItem.Name);

        Logger.Debug("Share File exists: {sharename} {path}", shareFileClient.ShareName, shareFileClient.Path);

        try
        {
            // Open stream
            var stream = await shareFileClient.OpenReadAsync(cancellationToken: cancellationTokenSource.Token);
            Logger.Information("Get file stream. {filename} {filelength}", shareFileClient.Name, stream.Length);
            return stream;
        }
        catch (ShareFileModifiedException e)
        {
            Logger.Error("Share File is currently being modified: {filename}", shareFileClient.Name);
            Logger.Error("{error}: {errorMessage}", nameof(e), e.Message);
            cancellationTokenSource.Cancel();
            return null;
        }
    }

    public static async Task DeleteFromFileShareAsync(ShareFileItem shareFileItem)
    {
        var shareFileClient = (await GetFileShareClientAsync()).GetFileClient(shareFileItem.Name);
        var response = await shareFileClient.DeleteIfExistsAsync();
        if (response.Value)
        {
            Logger.Information("File {filename} deleted from File Share", shareFileClient.Name);
        }
    }

    internal static async Task<ShareFileItem> GetShareFileItem(string filenamePrefix,
                                                               CancellationTokenSource cancellationTokenSource = default)
    {
        ShareFileItem shareFileItem = (await GetFileShareClientAsync()).GetFilesAndDirectories(filenamePrefix).FirstOrDefault(p => !p.IsDirectory);

        // Skip if the file is not exists
        if (null == shareFileItem)
        {
            Logger.Debug("Share File not exists, skip: {filename}", filenamePrefix);
            cancellationTokenSource?.Cancel();
        }
        return shareFileItem;
    }
}
