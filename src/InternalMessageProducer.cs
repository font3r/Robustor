using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Robustor;

public interface IInternalMessageProducer
{
    Task ProduceRetry<T>(T message, int retry, MessageContext messageContext)
        where T : IMessageData;
    Task ProduceToDlq<T>(T message, MessageContext messageContext)
        where T : IMessageData;
}

public sealed class InternalMessageProducer(
    IOptions<KafkaConfiguration> kafkaConfiguration,
    ILogger<MessageProducer> logger)
    : IInternalMessageProducer
{
    public async Task ProduceRetry<T>(T message, int retry, MessageContext messageContext) where T : IMessageData
    {
        var topic = TopicNamingHelper.GetRetryTopicName<T>(kafkaConfiguration.Value.TopicPrefix, retry);
        
        try
        {
            var delivery = await CreateProducer().ProduceAsync(topic,
                new Message<Null, string>
                {
                    Headers = [
                        new Header(Variables.MessageHeaders.Retry, 
                            Encoding.UTF8.GetBytes(retry.ToString())),
                        new Header(Variables.MessageHeaders.ErrorMessage, 
                            Encoding.UTF8.GetBytes(messageContext.ErrorMessage)),
                        new Header(Variables.MessageHeaders.ErrorCode,
                            Encoding.UTF8.GetBytes(messageContext.ErrorCode ?? string.Empty))
                    ],
                    Value = JsonSerializer.Serialize(new BaseMessage<T>(message)) // TODO: Should retry create new base message?
                });

            logger.LogInformation("Successful delivery to topic {Topic}, partition {Partition}, offset {Offset}",
                topic, delivery.Partition.Value, delivery.Offset.Value);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Delivery failed");
            throw;
        }
    }

    public async Task ProduceToDlq<T>(T message, MessageContext messageContext) where T : IMessageData
    {
        var topic = TopicNamingHelper.GetDlqTopicName<T>(kafkaConfiguration.Value.TopicPrefix);
        
        try
        {
            var delivery = await CreateProducer().ProduceAsync(topic,
                new Message<Null, string>
                {
                    Headers = [
                        new Header(Variables.MessageHeaders.ErrorMessage, 
                            Encoding.UTF8.GetBytes(messageContext.ErrorMessage)),
                        new Header(Variables.MessageHeaders.ErrorCode,
                            Encoding.UTF8.GetBytes(messageContext.ErrorCode ?? string.Empty))
                    ],
                    Value = JsonSerializer.Serialize(new BaseMessage<T>(message)) // TODO: Should retry create new base message?
                });

            logger.LogInformation("Successful delivery to topic {Topic}, partition {Partition}, offset {Offset}",
                topic, delivery.Partition.Value, delivery.Offset.Value);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Delivery failed");
            throw;
        }
    }
    
    private IProducer<Null, string> CreateProducer()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = kafkaConfiguration.Value.ConnectionString,
            AllowAutoCreateTopics = false
        };
        
        // TODO: Should producers be reused if they are build?
        var producer = new ProducerBuilder<Null, string>(config)
            .SetErrorHandler(HandleError)
            .Build();

        return producer;
    }
    
    private void HandleError(IProducer<Null, string> producer, Error error)
    {
        logger.LogError("Producer error {Name}, reason {Reason}, code {Code}",
            producer.Name, error.Reason, error.Code);
    }
}