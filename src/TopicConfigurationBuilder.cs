namespace Robustor;

public class TopicConfiguration
{
    public required int Partitions { get; init; }
    public required int RetryCount { get; init; }
}

public class TopicConfigurationBuilder
{
    
}