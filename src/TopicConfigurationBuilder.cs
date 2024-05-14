namespace Robustor;

public class TopicConfiguration
{
    // TODO: Required only for initial create, who has to provide configuration?
    public int Partitions { get; init; }
    
    // TODO: What if there's mismatch between producer and consumer configuration, maybe we should perform initial scan for topics?
    public required int RetryCount { get; init; }
}