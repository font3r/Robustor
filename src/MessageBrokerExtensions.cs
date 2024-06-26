using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Robustor;

public static class MessageBrokerExtensions
{
    public static IServiceCollection AddKafkaMessageBroker(this IServiceCollection services, IConfiguration configuration)
    {
        services.TryAddSingleton<IMessageProducer, MessageProducer>();
        services.TryAddSingleton<IMessageConsumer, MessageConsumer>();
        
        services.TryAddSingleton<IAdministratorClient, AdministratorClient>();

        services.AddOptions<KafkaConfiguration>()
            .Bind(configuration.GetSection(Variables.KafkaConfigurationSection));
        
        return services;
    }
}