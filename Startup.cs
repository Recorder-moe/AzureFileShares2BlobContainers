using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using Serilog.Events;

[assembly: FunctionsStartup(typeof(AzureFileShares2BlobContainers.Startup))]

namespace AzureFileShares2BlobContainers;

public class Startup : FunctionsStartup
{
    public override void Configure(IFunctionsHostBuilder builder)
    {
        var configuration = BuildConfiguration(builder.GetContext().ApplicationRootPath);
        builder.Services.Configure<IConfiguration>(configuration);

        Serilog.Core.LoggingLevelSwitch controlLevelSwitch = new();
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Override("Microsoft", LogEventLevel.Fatal)
            .MinimumLevel.Override("Microsoft.Hosting.Lifetime", LogEventLevel.Fatal)
            .MinimumLevel.ControlledBy(controlLevelSwitch)
            .WriteTo.Seq(serverUrl: "http://logserver.maki0419.com:5432",
                         apiKey: "",
                         controlLevelSwitch: controlLevelSwitch
                         )
            .CreateLogger();

        builder.Services.AddLogging(lb => lb.AddSerilog(Log.Logger, true));
    }

    private static IConfiguration BuildConfiguration(string applicationRootPath)
    {
        var config =
            new ConfigurationBuilder()
                .SetBasePath(applicationRootPath)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddJsonFile("settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

        return config;
    }
}
