using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Robustor.Core;

namespace Robustor.Outbox;

public static class Extensions
{
    public static IServiceCollection AddOutbox(this IServiceCollection services, IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString(Variables.Configuration.DefaultConnection);
        //Migrations.InitOutbox(connectionString!);
        
        services.AddTransient<IDbConnection>(_ => new SqlConnection(connectionString));
        services.AddTransient<IOutboxMessageProducer, OutboxMessageProducer>();
        services.AddScoped<IOutboxRepository, OutboxRepository>();
        services.AddHostedService<OutboxBackgroundService>();
        
        return services;
    }
}