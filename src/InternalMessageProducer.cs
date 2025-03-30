using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Robustor.Core;

namespace Robustor;

public interface IInternalMessageProducer
{
    Task ProduceRetry<T>(T message, int retry, MessageContext messageContext, CancellationToken cancellationToken)
        where T : IMessageData;
    Task ProduceToDlq<T>(T message, MessageContext messageContext, CancellationToken cancellationToken)
        where T : IMessageData;
}

public sealed class InternalMessageProducer(
    IOptions<KafkaConfiguration> kafkaConfiguration,
    ILogger<InternalMessageProducer> logger)
    : IInternalMessageProducer
{
    private readonly ConcurrentDictionary<string, IProducer<string, string>> _producers = new();
    
    public async Task ProduceRetry<T>(T message, int retry, MessageContext messageContext, 
        CancellationToken cancellationToken) 
        where T : IMessageData
    {
        var topic = TopicNamingHelper.GetRetryTopicName<T>(kafkaConfiguration.Value.TopicPrefix, retry);
        
        try
        {
            var delivery = await GetProducer(topic).ProduceAsync(topic,
                new Message<string, string>
                {
                    Headers = [
                        new Header(Variables.MessageHeaders.ErrorRetry, 
                            Encoding.UTF8.GetBytes(retry.ToString())),
                        new Header(Variables.MessageHeaders.ErrorMessage, 
                            Encoding.UTF8.GetBytes(messageContext.ErrorMessage)),
                        new Header(Variables.MessageHeaders.ErrorCode,
                            Encoding.UTF8.GetBytes(messageContext.ErrorCode ?? string.Empty))
                    ],
                    Value = JsonSerializer.Serialize(new BaseMessage<T>(message)) // TODO: Should retry create new base message?
                }, cancellationToken);

            logger.LogInformation("Successful delivery to topic {Topic}, partition {Partition}, offset {Offset}",
                topic, delivery.Partition.Value, delivery.Offset.Value);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Delivery failed");
            throw;
        }
    }

    public async Task ProduceToDlq<T>(T message, MessageContext messageContext, CancellationToken cancellationToken) 
        where T : IMessageData
    {
        var topic = TopicNamingHelper.GetDlqTopicName<T>(kafkaConfiguration.Value.TopicPrefix);
        
        try
        {
            var delivery = await GetProducer(topic).ProduceAsync(topic,
                new Message<string, string>
                {
                    Headers = [
                        new Header(Variables.MessageHeaders.ErrorMessage, 
                            Encoding.UTF8.GetBytes(messageContext.ErrorMessage)),
                        new Header(Variables.MessageHeaders.ErrorCode,
                            Encoding.UTF8.GetBytes(messageContext.ErrorCode ?? string.Empty))
                    ],
                    Value = JsonSerializer.Serialize(new BaseMessage<T>(message)) // TODO: Should DQL create new base message?
                }, cancellationToken);

            logger.LogInformation("Successful delivery to topic {Topic}, partition {Partition}, offset {Offset}",
                topic, delivery.Partition.Value, delivery.Offset.Value);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Delivery failed");
            throw;
        }
    }

    private IProducer<string, string> GetProducer(string topic)
    {
        try
        {
            var entered = Monitor.TryEnter(this);
            if (!entered)
                throw new Exception("Unable to obtain lock on producer");

            if (_producers.TryGetValue(topic, out var storedProducer))
                return storedProducer;

            var config = new ProducerConfig
            {
                BootstrapServers = kafkaConfiguration.Value.ConnectionString,
                Acks = Acks.All,
                AllowAutoCreateTopics = false
            };
        
            var producer = new ProducerBuilder<string, string>(config)
                .SetErrorHandler(HandleError)
                .Build();

            logger.LogInformation("Adding producer to internal store");
            if (!_producers.TryAdd(topic, producer))
                throw new Exception("Unable to add producer to store");

            return producer;
        }
        finally
        {
            Monitor.Exit(this);
        }
    }
    
    private void HandleError(IProducer<string, string> producer, Error error)
    {
        logger.LogError("Producer error {Name}, reason {Reason}, code {Code}",
            producer.Name, error.Reason, error.Code);
    }
}