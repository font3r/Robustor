using Robustor;

namespace Consumer;

public class KafkaConsumerBackgroundService(
    IMessageConsumer messageConsumer,
    IServiceScopeFactory serviceScopeFactory,
    ILogger<KafkaConsumerBackgroundService> logger) 
        : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await messageConsumer.Consume<OrderCreated>(
            new TopicConfiguration { RetryCount = 5, Partitions = 20 }, 
            async (message, cancellationToken) =>
            {
                await using var scope = serviceScopeFactory.CreateAsyncScope();
                var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();

                var failure = Random.Shared.Next(0, 101 / (Variables.FailurePercentage + 1)) == 0;
                if (failure) return MessageContext.Error("test error");
                
                context.Add(new Order { Id = message.Data.Id });
                await context.SaveChangesAsync(cancellationToken);
                
                return MessageContext.Success();
            },
            stoppingToken);
    }
}