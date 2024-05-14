using Robustor;

namespace Consumer;

public class KafkaConsumerBackgroundService(
    IMessageConsumer messageConsumer, 
    ILogger<KafkaConsumerBackgroundService> logger) 
        : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await messageConsumer.Consume<OrderCreated>(
            new TopicConfiguration { RetryCount = 5, Partitions = 5 }, 
            async message =>
            {
                await Task.CompletedTask;

                return Random.Shared.Next(0, 101 / (Variables.FailurePercentage + 1)) == 0 
                    ? MessageContext.Error("test error") 
                    : MessageContext.Success();
            },
            stoppingToken);
    }
}