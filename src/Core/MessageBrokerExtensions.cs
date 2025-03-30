using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Robustor.Core;

public static class MessageBrokerExtensions
{
    public static IServiceCollection AddKafkaMessageBroker(this IServiceCollection services, IConfiguration configuration)
    {
        services.TryAddSingleton<IInternalMessageProducer, InternalMessageProducer>();
        services.TryAddSingleton<IAdministratorClient, AdministratorClient>();

        services.Configure<KafkaConfiguration>(
            configuration.GetSection(Variables.KafkaConfigurationSection));
        
        return services;
    }

    public static IServiceCollection WithProducer<TMessage>(this IServiceCollection services) 
        where TMessage : IMessageData
    {
        services.TryAddSingleton<IMessageProducer<TMessage>, MessageProducer<TMessage>>();
        
        return services;
    }

    public static IServiceCollection WithConsumer<TMessage>(this IServiceCollection services)
        where TMessage : IMessageData
    {
        services.TryAddSingleton<IMessageConsumer<TMessage>, MessageConsumer<TMessage>>();
        
        return services;
    }
}