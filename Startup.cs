using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Serilog;
using System;
using System.IO;

#if DEBUG
Serilog.Debugging.SelfLog.Enable(msg => Console.WriteLine(msg));
#endif

IConfiguration configuration = new ConfigurationBuilder()
                .SetBasePath(Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location))
                .AddEnvironmentVariables()
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddJsonFile("settings.json", optional: true, reloadOnChange: true)
                .Build();

Log.Logger = new LoggerConfiguration().ReadFrom.Configuration(configuration)
                                      .CreateBootstrapLogger();

Log.Information("Starting up...");
try
{
    var host = Host.CreateDefaultBuilder()
        .ConfigureFunctionsWorkerDefaults()
        .UseSerilog((context, config) => config.ReadFrom.Configuration(configuration))
        .Build();

    host.RunAsync();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Unhandled exception");
}
finally
{
    Log.Information("Shut down complete");
    Log.CloseAndFlush();
}

