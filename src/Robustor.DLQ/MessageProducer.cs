using System.Collections.Concurrent;
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
    private readonly ConcurrentDictionary<string, IProducer<Null, string>> _producers = new();
    
    private IProducer<Null, string> GetProducer(string topic)
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
                AllowAutoCreateTopics = false
            };

            var producer = new ProducerBuilder<Null, string>(config)
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
    
    public async Task Produce<T>(T message)
        where T : IMessageData
    {
        var topic = TopicNamingHelper.GetTopicName<T>(kafkaConfiguration.Value.TopicPrefix);

        try
        {
            var delivery = await GetProducer(topic).ProduceAsync(topic,
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