using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Robustor;

public interface IMessageConsumer
{
    Task Consume<T>(TopicConfiguration topicConfiguration, Func<BaseMessage<T>, Task<MessageContext>> handle, 
        CancellationToken cancellationToken)
            where T : IMessageData;
}

public sealed class MessageConsumer(
    IInternalMessageProducer messageProducer,
    IAdministratorClient administratorClient,
    IOptions<KafkaConfiguration> kafkaConfiguration,
    ILogger<MessageConsumer> logger)
        : IMessageConsumer
{
    public async Task Consume<T>(
        TopicConfiguration topicConfiguration,
        Func<BaseMessage<T>, Task<MessageContext>> handle,
        CancellationToken cancellationToken)
            where T : IMessageData
    {
        var topics = TopicNamingHelper.GetResilienceTopics<T>(kafkaConfiguration.Value.TopicPrefix,
            topicConfiguration.RetryCount).ToList();
        
        await administratorClient.CreateTopics(topics, topicConfiguration);
        
        // Main thread
        _ = Task.Run(async () =>
        {
            var semaphore = new SemaphoreSlim(10, 10);
            var consumer = GetConsumer();

            consumer.Subscribe(topics[..^1]); // TODO: Administrator client creates DLQ topic but we should not subscribe to it
            logger.LogInformation("Subscribed to topic {Topics}", string.Join(", ", topics));

            while (!cancellationToken.IsCancellationRequested)
            {
                await semaphore.WaitAsync(cancellationToken);
                var consumeResult = consumer.Consume(cancellationToken);

                // Worker thread
                _ = Task.Run(async () =>
                {
                    try
                    {
                        logger.LogInformation("Received message from topic {Topic}", consumeResult.Topic);
                        
                        var baseMessage = JsonSerializer.Deserialize<BaseMessage<T>>(consumeResult.Message.Value);
                        var messageContext = await handle(baseMessage);

                        if (messageContext.Result is MessageStatus.Error)
                        {
                            var retryCount = GetRetry(consumeResult.Message.Headers);
                            var nextRetry = retryCount + 1;

                            if (nextRetry <= topicConfiguration.RetryCount)
                                await messageProducer.ProduceRetry(baseMessage.Data, nextRetry, messageContext);    
                            else
                                await messageProducer.ProduceToDlq(baseMessage.Data, messageContext);
                        }
                    }
                    catch (ConsumeException consumeException)
                    {
                        // TODO: Retry and dlq
                        logger.LogError(consumeException, "Exception during consume");
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }, cancellationToken);
            }
        }, cancellationToken);
    }

    private static int GetRetry(Headers messageHeaders)
    {
        var retryHeaderRaw = messageHeaders.SingleOrDefault(x => x.Key == Variables.MessageHeaders.Retry);
        if (retryHeaderRaw is null) return default;

        if (!int.TryParse(new ReadOnlySpan<byte>(retryHeaderRaw.GetValueBytes()), out var retry))
            throw new Exception("Unable to parse retry header");

        return retry;
    }

    private IConsumer<Null, string> GetConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = kafkaConfiguration.Value.ConnectionString,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            GroupId = kafkaConfiguration.Value.ConsumerGroup,
            ClientId = "robustor-client-id-test",
            AllowAutoCreateTopics = false
            
            // TODO: sounds usefull
            // GroupInstanceId =  
        };
        
        var consumer = new ConsumerBuilder<Null, string>(config)
            .SetErrorHandler(HandleError)
            .Build();

        return consumer;
    }
    
    private void HandleError(IConsumer<Null, string> consumer, Error error)
    {
        logger.LogError("Error during consumer on topic {Topic}, reason {Reason}, code {Code}",
            consumer.Subscription.First(), error.Reason, error.Code);
    }
}