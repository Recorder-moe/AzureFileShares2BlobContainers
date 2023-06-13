# AzureFileShares2BlobContainers

AzureFileShares2BlobContainers is an Azure Functions project built with .NET 6 that facilitates the transfer of recorded video files from Azure Files to Azure Blob Storage. This Azure Function is called by the LivestreamRecorderService and should deployed in the same Azure region as the Azure Storage of this project. This ensures that the files are transferred within the same region, avoiding additional outbound data transfer costs.

## Prerequisites

To run this project locally or deploy it to Azure, you need the following prerequisites:

- [.NET 6 SDK](https://dotnet.microsoft.com/download/dotnet/6.0)
- Azure Storage account with both Azure Files and Azure Blob Storage services set up

## Getting Started

1. Clone the repository or download the source code.
2. Open the project in your preferred development environment (e.g., Visual Studio, Visual Studio Code).
3. Update the `local.settings.json` file with the connection strings for your Azure Storage account:

   ```json
   {
     "IsEncrypted": false,
     "Values": {
       "AzureWebJobsStorage": "<AzureWebJobsStorage_connection_string>",
       "AzureWebJobsDashboard": "<AzureWebJobsDashboard_connection_string>"
     }
   }
   ```

4. Build the project to restore NuGet packages and compile the code.
5. Run the project locally or deploy it to Azure Functions.
6. Use the LivestreamRecorderService or any other service to trigger the AzureFileShares2BlobContainers function by making a `POST` request with the `filename` query parameter.

## Functionality

- **HTTP Trigger:** This function (`RunAsync`) is triggered by an HTTP POST request. It expects a `filename` query parameter specifying the name of the file to process.
- The functions log information using Serilog.

## License

This project is licensed under the [MIT License](LICENSE).
