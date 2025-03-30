using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Robustor.Core;

public interface IMessageConsumer<out TMessage>
    where TMessage : IMessageData
{
    Task Consume(
        TopicConfiguration topicConfiguration,
        Func<TMessage, CancellationToken, Task<MessageContext>> handle,
        CancellationToken cancellationToken);
}

public sealed class MessageConsumer<TMessage>(
    IInternalMessageProducer messageProducer,
    IAdministratorClient administratorClient,
    IOptions<KafkaConfiguration> kafkaConfiguration,
    ILogger<MessageConsumer<TMessage>> logger)
        : IMessageConsumer<TMessage>
        where TMessage : IMessageData
{
    public async Task Consume(
        TopicConfiguration topicConfiguration,
        Func<TMessage, CancellationToken, Task<MessageContext>> handle,
        CancellationToken cancellationToken)
    {
        var topics = TopicNamingHelper.GetResilienceTopics<TMessage>(
            kafkaConfiguration.Value.TopicPrefix, topicConfiguration.RetryCount);
        
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
                        await HandleMessage(consumeResult, topics, handle, topicConfiguration, cancellationToken);
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

    private async Task HandleMessage(ConsumeResult<Guid, TMessage> consumeResult,
        IDictionary<string, TopicType> topics,
        Func<TMessage, CancellationToken, Task<MessageContext>> handle,
        TopicConfiguration topicConfiguration,
        CancellationToken cancellationToken)
    {
        logger.LogDebug("Received message from topic {Topic}", consumeResult.Topic);

        if (topics[consumeResult.Topic] is TopicType.Retry)
        {
            var retryDelay = Variables.BaseMessageRetryDelay * GetRetry(consumeResult.Message.Headers);
            logger.LogDebug("Delaying handle for {RetryDelay}", retryDelay);
                            
            await Task.Delay(retryDelay, cancellationToken);
        }

        var message = consumeResult.Message.Value;
        // extract message headers
        //var headers = consumeResult.Message.Headers;
        var messageContext = await handle(message, cancellationToken); // TODO: Add new token per process

        if (messageContext.Result is MessageStatus.Error)
            await HandleRetry(consumeResult.Message.Headers, topicConfiguration, message, messageContext, 
                cancellationToken);
    }

    private async Task HandleRetry(Headers messageHeaders, TopicConfiguration topicConfiguration, 
        TMessage message, MessageContext messageContext, CancellationToken cancellationToken) 
    {
        var retryCount = GetRetry(messageHeaders);
        var nextRetry = retryCount + 1;

        if (nextRetry <= topicConfiguration.RetryCount)
            await messageProducer.ProduceRetry(message, nextRetry, messageContext, cancellationToken);    
        else
            await messageProducer.ProduceToDlq(message, messageContext, cancellationToken);
    }

    private static int GetRetry(Headers messageHeaders)
    {
        var retryHeaderRaw = messageHeaders.SingleOrDefault(x => x.Key == Variables.MessageHeaders.ErrorRetry);
        if (retryHeaderRaw is null) return 0;

        if (!int.TryParse(new ReadOnlySpan<byte>(retryHeaderRaw.GetValueBytes()), out var retry))
            throw new Exception("Unable to parse retry header");

        return retry;
    }

    private IConsumer<Guid, TMessage> GetConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = kafkaConfiguration.Value.ConnectionString,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            GroupId = kafkaConfiguration.Value.ConsumerGroup,
            AllowAutoCreateTopics = false,
            Acks = Acks.All  
        };
        
        var consumer = new ConsumerBuilder<Guid, TMessage>(config)
            .SetKeyDeserializer(new GuidKeySerializer())
            .SetValueDeserializer(new BaseMessageSerializer<TMessage>())
            .SetErrorHandler(HandleError)
            .Build();

        return consumer;
    }
    
    private void HandleError(IConsumer<Guid, TMessage> consumer, Error error)
    {
        logger.LogError("Error during consumer on topic {Topic}, reason {Reason}, code {Code}",
            consumer.Subscription.First(), error.Reason, error.Code);
    }
}