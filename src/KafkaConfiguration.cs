namespace Robustor;

public class KafkaConfiguration
{
    public required string TopicPrefix { get; init; }
    public required string ConsumerGroup { get; init; }
    public required IEnumerable<string> BootstrapServers { get; init; }

    public string ConnectionString => string.Join(',', BootstrapServers);
}