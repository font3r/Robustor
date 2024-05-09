using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Robustor;

public interface IMessageProducer
{
    Task Produce<T>(T message, TopicConfiguration configuration)
        where T : IMessageData;
}

public sealed class MessageProducer(
    IAdministratorClient administratorClient,
    IOptions<KafkaConfiguration> kafkaConfiguration,
    ILogger<MessageProducer> logger)
        : IMessageProducer
{
    public async Task Produce<T>(T message, TopicConfiguration configuration)
        where T : IMessageData
    {
        await administratorClient.CreateTopic<T>(configuration);
        
        var config = new ProducerConfig
        {
            BootstrapServers = kafkaConfiguration.Value.ConnectionString
        };
        
        // TODO: Should producers be reused if they are build?
        var producer = new ProducerBuilder<Null, string>(config)
            .SetErrorHandler(HandleError)
            .Build();

        try
        {
            var topic = TopicNamingHelper.GetTopicName<T>(kafkaConfiguration.Value.TopicPrefix);
            
            var delivery = await producer.ProduceAsync(topic,
                new Message<Null, string>
                {
                    Headers = new Headers
                    {
                        { "id", Guid.NewGuid().ToByteArray() }  
                    },
                    Value = JsonSerializer.Serialize(new BaseMessage<T>(message))
                });

            logger.LogInformation("Successful delivery on topic {Topic}, partition {Partition}, offset {Offset}",
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