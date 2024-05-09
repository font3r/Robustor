using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Robustor;

public static class MessageBrokerExtensions
{
    public static IServiceCollection AddKafkaMessageBroker(this IServiceCollection services)
    {
        services.TryAddSingleton<IMessageProducer, MessageProducer>();
        services.TryAddSingleton<IMessageConsumer, MessageConsumer>();
        
        services.TryAddSingleton<IAdministratorClient, AdministratorClient>();
        
        return services;
    }
}