using System.Collections.Concurrent;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Robustor;

public interface IAdministratorClient
{
    Task CreateTopics(IEnumerable<string> topics, TopicConfiguration topicConfiguration);
}

public class AdministratorClient : IAdministratorClient
{
    private readonly ILogger<AdministratorClient> _logger;
    
    private readonly IAdminClient _adminClient;
    private readonly ConcurrentBag<string> _declaredTopics = [];
    
    public AdministratorClient(IOptions<KafkaConfiguration> kafkaConfiguration, 
        ILogger<AdministratorClient> logger)
    {
        _logger = logger;
        
        var config = new AdminClientConfig
        {
            BootstrapServers = kafkaConfiguration.Value.ConnectionString
        };

        _adminClient = new AdminClientBuilder(config)
            .SetLogHandler(HandleError)
            .Build();
    }
    
    public async Task CreateTopics(IEnumerable<string> topics, TopicConfiguration topicConfiguration)
    {
        var topicsToDescribe = topics.Where(topic => !_declaredTopics.Contains(topic)).ToList();
        if (topicsToDescribe.Count == 0) return;

        var topicsToCreate = new List<string>();
        var topicExistenceResult = await DescribeTopic(topicsToDescribe);
        foreach (var (describedTopic, exists) in topicExistenceResult)
        {
            if (exists)
            {
                _declaredTopics.Add(describedTopic);
                _logger.LogInformation("Topic {Topic} already exists on Kafka, skipping creation", describedTopic);
            }
            else
            {
                topicsToCreate.Add(describedTopic);
            }
        }

        if (topicsToCreate.Count == 0)
            return;
        
        var replicationFactor = await GetReplicationFactor();

        try
        {
            _logger.LogInformation("Creating topics {Topics}", string.Join(',', topicsToCreate));
            
            await _adminClient.CreateTopicsAsync(
                topicsToCreate.Select(newTopic => new TopicSpecification
                {
                    Name = newTopic,
                    NumPartitions = topicConfiguration.Partitions,
                    ReplicationFactor = (short)replicationFactor
                }),
                new CreateTopicsOptions
                {
                    RequestTimeout = Variables.GlobalRequestTimeout,
                    OperationTimeout = Variables.GlobalOperationTimeout
                });
            
            topicsToCreate.ForEach(declaredTopic => _declaredTopics.Add(declaredTopic));
        }
        catch (CreateTopicsException ex)
        {
            foreach (var createTopicReport in ex.Results)
            {
                _logger.LogError(ex, "Exception during creating topic {Topic}, error {Error}, code {ErrorCode}", 
                    createTopicReport.Topic, createTopicReport.Error.Reason, createTopicReport.Error.Code);    
            }

            throw;
        }
    }
    
    private async Task<IDictionary<string, bool>> DescribeTopic(IReadOnlyCollection<string> topics)
    {
        try
        {
            await _adminClient.DescribeTopicsAsync(
                TopicCollection.OfTopicNames(topics),
                new DescribeTopicsOptions
                {
                    RequestTimeout = Variables.GlobalRequestTimeout,
                });

            return topics.ToDictionary(x => x, _ => true);
        }
        catch (DescribeTopicsException ex)
        {
            var describedTopics = new Dictionary<string, bool>();
            foreach (var topicDescription in ex.Results.TopicDescriptions)
            {
                if (topicDescription.Error.IsError && topicDescription.Error.Code is ErrorCode.UnknownTopicOrPart)
                    describedTopics.Add(topicDescription.Name, false);
                else
                {
                    // TODO: Revalidate approach
                    throw new Exception($"Topic {topicDescription.Name} in unknown state!");
                }
            }

            return describedTopics;
        }
    }
    
    private async Task<int> GetReplicationFactor()
    {
        try
        {
            var describeResult = await _adminClient.DescribeClusterAsync(
                new DescribeClusterOptions
                {
                    RequestTimeout = Variables.GlobalRequestTimeout
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