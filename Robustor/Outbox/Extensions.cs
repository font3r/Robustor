using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Robustor;

public static class Extensions
{
    public static void AddOutbox(this IServiceCollection services, IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString(Variables.Configuration.DefaultConnection);
        //Migrations.InitOutbox(connectionString!);
        
        services.AddTransient<IDbConnection>(_ => new SqlConnection(connectionString));
        services.AddScoped<IOutboxRepository, OutboxRepository>();
        services.AddHostedService<OutboxBackgroundService>();
    }
}