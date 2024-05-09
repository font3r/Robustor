using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Robustor;

public interface IMessageConsumer
{
    Task Consume<T>(TopicConfiguration topicConfiguration, Func<BaseMessage<T>, Task> handle, 
        CancellationToken cancellationToken)
            where T : IMessageData;
}

public sealed class MessageConsumer(
    IAdministratorClient administratorClient,
    IOptions<KafkaConfiguration> kafkaConfiguration,
    ILogger<MessageConsumer> logger)
        : IMessageConsumer
{
    public async Task Consume<T>(
        TopicConfiguration topicConfiguration,
        Func<BaseMessage<T>, Task> handle,
        CancellationToken cancellationToken)
            where T : IMessageData
    {
        var topic = TopicNamingHelper.GetTopicName<T>(kafkaConfiguration.Value.TopicPrefix);
        
        await administratorClient.CreateTopic(topic, topicConfiguration);
        
        var config = new ConsumerConfig
        {
            BootstrapServers = string.Concat(kafkaConfiguration.Value.BootstrapServers, ","),
            AutoOffsetReset = AutoOffsetReset.Earliest,
            GroupId = kafkaConfiguration.Value.ConsumerGroup,
            ClientId = "robustor-client-id-test"
            
            // TODO: sounds usefull
            // GroupInstanceId =  
        };
        
        // Main thread
        _ = Task.Run(async () =>
        {
            var semaphore = new SemaphoreSlim(10, 10);
            var consumer = new ConsumerBuilder<Null, string>(config)
                .SetErrorHandler(HandleError)
                .Build();

            consumer.Subscribe(topic); // TODO: Handle all topics for retry
            Console.WriteLine($"Subscribed to topic {topic}");

            while (!cancellationToken.IsCancellationRequested)
            {
                await semaphore.WaitAsync(cancellationToken);
                var consumeResult = consumer.Consume(cancellationToken);

                // Worker thread
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await handle(JsonSerializer.Deserialize<BaseMessage<T>>(
                            consumeResult.Message.Value));
                    }
                    catch (ConsumeException consumeException)
                    {
                        Console.WriteLine($"Exception during consume {consumeException.Message}");
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }, cancellationToken);
            }
        }, cancellationToken);
    }
    
    private void HandleError(IConsumer<Null, string> consumer, Error error)
    {
        logger.LogError("Error during consumer on topic {Topic}, reason {Reason}, code {Code}",
            consumer.Subscription.First(), error.Reason, error.Code);
    }
}