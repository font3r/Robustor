using System.Collections.Concurrent;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Robustor;

public interface IAdministratorClient
{
    Task CreateTopic(string topic, TopicConfiguration topicConfiguration);
}

public class AdministratorClient : IAdministratorClient
{
    private readonly KafkaConfiguration _kafkaConfiguration;
    private readonly ILogger<AdministratorClient> _logger;
    
    private readonly IAdminClient _adminClient;
    private readonly ConcurrentBag<string> _declaredTopics = [];
    
    public AdministratorClient(IOptions<KafkaConfiguration> kafkaConfiguration, 
        ILogger<AdministratorClient> logger)
    {
        _kafkaConfiguration = kafkaConfiguration.Value;
        _logger = logger;
        
        var config = new AdminClientConfig
        {
            BootstrapServers = kafkaConfiguration.Value.ConnectionString
        };

        _adminClient = new AdminClientBuilder(config)
            .SetLogHandler(HandleError)
            .Build();
    }
    
    public async Task CreateTopic(string topic, TopicConfiguration topicConfiguration)
    {
        if (_declaredTopics.Contains(topic))
            return;
        
        var topicExists = await TopicExists(topic);
        if (topicExists)
        {
            _declaredTopics.Add(topic);
            return;
        }
        
        var replicationFactor = await GetReplicationFactor();

        try
        {
            _logger.LogInformation("Creating topic {Topic}", topic);
            
            await _adminClient.CreateTopicsAsync(
                new List<TopicSpecification>
                {
                    new()
                    {
                        Name = string.Concat(_kafkaConfiguration.TopicPrefix, topic),
                        NumPartitions = topicConfiguration.Partitions,
                        ReplicationFactor = (short)replicationFactor
                    }
                },
                new CreateTopicsOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(10),
                    OperationTimeout = TimeSpan.FromSeconds(10)
                });
            
            _declaredTopics.Add(topic);
        }
        catch (CreateTopicsException ex)
        {
            _logger.LogError(ex, "Exception during creating topic {Topic}", topic);
            throw;
        }
    }
    
    private async Task<bool> TopicExists(string topic)
    {
        try
        {
            var describeTopics = await _adminClient.DescribeTopicsAsync(
                TopicCollection.OfTopicNames([topic]),
                new DescribeTopicsOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(10),
                });

            return describeTopics.TopicDescriptions.First().Name == topic;
        }
        catch (DescribeTopicsException ex)
        {
            var describedTopic = ex.Results.TopicDescriptions.FirstOrDefault();
            if (describedTopic is null) throw;
            
            if (describedTopic.Error.IsError && describedTopic.Error.Code is ErrorCode.UnknownTopicOrPart)
                return false;

            throw;
        }
    }
    
    private async Task<int> GetReplicationFactor()
    {
        try
        {
            var describeResult = await _adminClient.DescribeClusterAsync(
                new DescribeClusterOptions
                {
                    RequestTimeout = TimeSpan.FromSeconds(10)
                });

            return describeResult.Nodes.Count;
        }
        catch (KafkaException ex)
        {
            _logger.LogError(ex, "Exception during fetching cluster info");
            throw;
        }
    }

    private void HandleError(IAdminClient client, LogMessage log)
    {
        _logger.LogError("Error occured in admin client {Level} {Name} {Message} - {Facility}",
            log.Level, log.Name, log.Message, log.Facility);
    }
}