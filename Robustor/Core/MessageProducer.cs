using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Robustor.Core;

public interface IMessageProducer<in TMessage>
{
    Task Produce(string topic, string message);
    Task Produce(Guid key, TMessage message);
}

public sealed class MessageProducer<TMessage>(
    IOptions<KafkaConfiguration> kafkaConfiguration,
    ILogger<MessageProducer<TMessage>> logger)
    : IMessageProducer<TMessage>
    where TMessage : IMessageData
{
    private readonly ConcurrentDictionary<string, IProducer<Guid, TMessage>> _producers = new();
    
    private IProducer<Guid, TMessage> GetProducer(string topic)
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

            var producer = new ProducerBuilder<Guid, TMessage>(config)
                .SetPartitioner(topic, IdBasedPartitioner.Partitioner)
                .SetKeySerializer(new GuidKeySerializer())
                .SetValueSerializer(new BaseMessageSerializer<TMessage>())
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

    /// <summary>
    /// Produces message to topic
    /// </summary>
    /// <param name="key">message if, if null uses message id</param>
    /// <param name="message">message payload</param>
    public async Task Produce(Guid key, TMessage message)
    {
        var topic = TopicNamingHelper.GetTopicName<TMessage>(kafkaConfiguration.Value.TopicPrefix);

        try
        {
            var delivery = await GetProducer(topic)
                .ProduceAsync(topic, new Message<Guid, TMessage>
                {
                    Key = key,
                    Value = message,
                    Timestamp = new Timestamp(DateTimeOffset.UtcNow),
                    Headers =
                    [
                        new Header(Variables.MessageHeaders.Id, 
                            Guid.NewGuid().ToByteArray(true)),
                        new Header(Variables.MessageHeaders.Type, 
                            Encoding.UTF8.GetBytes(message.GetType().ToString())),
                        new Header(Variables.MessageHeaders.TraceContext, 
                            Encoding.UTF8.GetBytes(Activity.Current?.Id ?? new Activity("message.publisher").Id!)),
                        new Header(Variables.MessageHeaders.EventOccured, 
                            Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToString()))
                    ]
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
    
    public async Task Produce(string topic, string message)
    {
        try
        {
            var config = new ProducerConfig
            {
                BootstrapServers = kafkaConfiguration.Value.ConnectionString,
                AllowAutoCreateTopics = false
            };
            
            var tempProducer = new ProducerBuilder<Null, string>(config)
                //.SetErrorHandler(HandleError)
                .Build();
            
            var delivery = await tempProducer.ProduceAsync(topic,
                new Message<Null, string> { Value = message });

            logger.LogInformation("Successful delivery to topic {Topic}, partition {Partition}, offset {Offset}",
                topic, delivery.Partition.Value, delivery.Offset.Value);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Delivery failed");
            throw;
        }
    }

    private void HandleError(IProducer<Guid, TMessage> producer, Error error)
    {
        logger.LogError("Producer error {Name}, reason {Reason}, code {Code}",
            producer.Name, error.Reason, error.Code);
    }
}