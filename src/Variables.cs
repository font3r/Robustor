namespace Robustor;

public static class Variables
{
    public const string KafkaConfigurationSection = "Kafka";
    
    public const char TopicSeparator = '_';
    public const string CommandSuffix = "Command";
    public const string EventSuffix = "Event";

    // TODO: Move to options
    public static readonly TimeSpan GlobalRequestTimeout = TimeSpan.FromSeconds(10);
    public static readonly TimeSpan GlobalOperationTimeout = TimeSpan.FromSeconds(10);

    public static string RetrySuffix(int retry) => $"retry_{retry}";
    public const string DlqSuffix = "dlq";
    
    // TODO: Development options
    public const int FailurePercentage = 100;

    public static class MessageHeaders
    {
        public const string Retry = "retry";
        public const string ErrorMessage = "error.message";
        public const string ErrorCode = "error.code";
    }
}