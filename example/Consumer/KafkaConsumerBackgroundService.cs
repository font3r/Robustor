using Robustor;
using Robustor.Core;

namespace Consumer;

public class KafkaConsumerBackgroundService(
    IMessageConsumer<OrderCreated> messageConsumer,
    IServiceScopeFactory serviceScopeFactory) 
        : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await messageConsumer.Consume(
            new TopicConfiguration { RetryCount = 5, Partitions = 20 }, 
            async (message, cancellationToken) =>
            {
                await using var scope = serviceScopeFactory.CreateAsyncScope();
                var context = scope.ServiceProvider.GetRequiredService<TestDbContext>();

                var failure = Random.Shared.Next(0, 101 / (Variables.FailurePercentage + 1)) == 0;
                if (failure) return MessageContext.Error("test error");
                
                context.Add(new Order { Id = message.Id });
                await context.SaveChangesAsync(cancellationToken);
                
                return MessageContext.Success();
            },
            stoppingToken);
    }
}