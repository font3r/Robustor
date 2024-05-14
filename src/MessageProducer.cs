using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Robustor;

public interface IMessageProducer
{
    Task Produce<T>(T message)
        where T : IMessageData;
}

public sealed class MessageProducer(
    IOptions<KafkaConfiguration> kafkaConfiguration,
    ILogger<MessageProducer> logger)
        : IMessageProducer
{
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
    
    public async Task Produce<T>(T message)
        where T : IMessageData
    {
        var topic = TopicNamingHelper.GetTopicName<T>(kafkaConfiguration.Value.TopicPrefix);

        try
        {
            var delivery = await CreateProducer().ProduceAsync(topic,
                new Message<Null, string>
                {
                    Value = JsonSerializer.Serialize(new BaseMessage<T>(message))
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

    private void HandleError(IProducer<Null, string> producer, Error error)
    {
        logger.LogError("Producer error {Name}, reason {Reason}, code {Code}",
            producer.Name, error.Reason, error.Code);
    }
}