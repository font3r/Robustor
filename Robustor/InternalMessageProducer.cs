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
    Task Produce(string topic, Guid key, string message, CancellationToken cancellationToken);
}

public sealed class InternalMessageProducer(
    IOptions<KafkaConfiguration> kafkaConfiguration,
    ILogger<InternalMessageProducer> logger)
    : IInternalMessageProducer
{
    public async Task ProduceRetry<T>(T message, int retry, MessageContext messageContext, 
        CancellationToken cancellationToken) 
        where T : IMessageData
    {
        var topic = TopicNamingHelper.GetRetryTopicName<T>(kafkaConfiguration.Value.TopicPrefix, retry);
        
        try
        {
            var delivery = await CreateProducer().ProduceAsync(topic,
                new Message<Guid, string>
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
            var delivery = await CreateProducer().ProduceAsync(topic,
                new Message<Guid, string>
                {
                    Headers = [
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

    public async Task Produce(string topic, Guid key, string message, CancellationToken cancellationToken)
    {
        try
        {
            var delivery = await CreateProducer().ProduceAsync(topic,
                new Message<Guid, string>
                {
                    Key = key,
                    // TODO: fill headers from outbox 
                    // Headers = 
                    Value = message
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

    private IProducer<Guid, string> CreateProducer()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = kafkaConfiguration.Value.ConnectionString,
            Acks = Acks.All,
            AllowAutoCreateTopics = false
        };
        
        // TODO: Should producers be reused if they are build?
        // TODO: Outbox producer is different from standard (eg. missing custom serializer) but at the same
        //      time value serializer won't be applied because data is already serialized 
        var producer = new ProducerBuilder<Guid, string>(config)
            .SetKeySerializer(new GuidKeySerializer())
            .SetErrorHandler(HandleError)
            .Build();

        return producer;
    }
    
    private void HandleError(IProducer<Guid, string> producer, Error error)
    {
        logger.LogError("Producer error {Name}, reason {Reason}, code {Code}",
            producer.Name, error.Reason, error.Code);
    }
}