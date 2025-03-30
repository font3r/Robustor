using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Robustor.Outbox;

public interface IMessageProducer
{
    Task Produce(string topic, string key, string message, CancellationToken cancellationToken);
}

public class OutboxMessageProducer(
    IOptions<KafkaConfiguration> kafkaConfiguration,
    ILogger<OutboxMessageProducer> logger) 
    : IMessageProducer
{
    public async Task Produce(string topic, string key, string message, CancellationToken cancellationToken)
    {
        try
        {
            var delivery = await CreateProducer().ProduceAsync(topic,
                new Message<string, string>
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

    private IProducer<string, string> CreateProducer()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = kafkaConfiguration.Value.ConnectionString,
            Acks = Acks.All,
            AllowAutoCreateTopics = false
        };

        // TODO: Outbox producer is different from standard (eg. missing custom serializer) but at the same
        //      time value serializer won't be applied because data is already serialized 
        var producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler(HandleError)
            .Build();

        return producer;
    }

    private void HandleError(IProducer<string, string> producer, Error error)
    {
        logger.LogError("Producer error {Name}, reason {Reason}, code {Code}",
            producer.Name, error.Reason, error.Code);
    }
}