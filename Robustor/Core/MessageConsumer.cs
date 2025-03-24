using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Robustor;

public interface IMessageConsumer
{
    Task Consume<T>(
        TopicConfiguration topicConfiguration, 
        Func<BaseMessage<T>, CancellationToken, Task<MessageContext>> handle, 
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
        Func<BaseMessage<T>, CancellationToken, Task<MessageContext>> handle,
        CancellationToken cancellationToken)
            where T : IMessageData
    {
        var topics = TopicNamingHelper.GetResilienceTopics<T>(kafkaConfiguration.Value.TopicPrefix,
            topicConfiguration.RetryCount);
        
        await administratorClient.CreateTopics(topics.Keys, topicConfiguration);
        
        // Main thread
        _ = Task.Run(async () =>
        {
            var semaphore = new SemaphoreSlim(Variables.MaximumWorkersCount, Variables.MaximumWorkersCount);
            var consumer = GetConsumer();

            var topicsToSubscribe = topics
                .Where(x => x.Value is not TopicType.Dlq)
                .Select(x => x.Key)
                .ToList();
            
            consumer.Subscribe(topicsToSubscribe);
            logger.LogInformation("Subscribed to topic {Topics}", string.Join(", ", topicsToSubscribe));

            while (!cancellationToken.IsCancellationRequested)
            {
                await semaphore.WaitAsync(cancellationToken);
                var consumeResult = consumer.Consume(cancellationToken);

                // Worker thread
                _ = Task.Run(async () =>
                {
                    try
                    {
                        logger.LogDebug("Received message from topic {Topic}", consumeResult.Topic);

                        if (topics[consumeResult.Topic] is TopicType.Retry)
                        {
                            var retryDelay = Variables.BaseMessageRetryDelay * GetRetry(consumeResult.Message.Headers);
                            logger.LogDebug("Delaying handle for {RetryDelay}", retryDelay);
                            
                            await Task.Delay(retryDelay, cancellationToken);
                        }

                        var baseMessage = JsonSerializer.Deserialize<BaseMessage<T>>(consumeResult.Message.Value);
                        var messageContext = await handle(baseMessage, cancellationToken); // TODO: Add new token per process

                        if (messageContext.Result is MessageStatus.Error)
                        {
                            await HandleRetry(consumeResult.Message.Headers, topicConfiguration,
                                baseMessage.Message, messageContext);
                        }
                    }
                    catch (JsonException jsonException) // TODO: Which exceptions should we retry?
                    {
                        // TODO: Deserialization exceptions won't be able to pass standard logic with deserialization
                        
                        //await HandleRetry(consumeResult.Message.Headers, topicConfiguration, 
                        //    baseMessage.Data, messageContext);
                    }
                    catch (ConsumeException consumeException)
                    {
                        logger.LogError(consumeException, "Exception during consume");
                        throw;
                    }
                    finally
                    {
                        logger.LogCritical("Semaphore state {Count}", semaphore.CurrentCount);
                        semaphore.Release();
                    }
                }, cancellationToken);
            }
        }, cancellationToken);
    }

    private async Task HandleRetry<T>(Headers messageHeaders, TopicConfiguration topicConfiguration, 
        T baseMessageData, MessageContext messageContext) 
            where T : IMessageData
    {
        var retryCount = GetRetry(messageHeaders);
        var nextRetry = retryCount + 1;

        if (nextRetry <= topicConfiguration.RetryCount)
            await messageProducer.ProduceRetry(baseMessageData, nextRetry, messageContext);    
        else
            await messageProducer.ProduceToDlq(baseMessageData, messageContext);
    }

    private static int GetRetry(Headers messageHeaders)
    {
        var retryHeaderRaw = messageHeaders.SingleOrDefault(x => x.Key == Variables.MessageHeaders.Retry);
        if (retryHeaderRaw is null) return 0;

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
            AllowAutoCreateTopics = false
            
            // TODO: sounds usefull
            // ClientId = "robustor-client-id-test",
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