namespace Robustor;

internal static class Variables
{
    internal const string KafkaConfigurationSection = "Kafka";
    
    internal const char TopicSeparator = '_';
    internal const string CommandSuffix = "Command";
    internal const string EventSuffix = "Event";

    // TODO: Move to options
    internal static readonly TimeSpan GlobalRequestTimeout = TimeSpan.FromSeconds(10);
    internal static readonly TimeSpan GlobalOperationTimeout = TimeSpan.FromSeconds(10);
    internal static readonly TimeSpan BaseMessageRetryDelay = TimeSpan.FromSeconds(1);

    internal const int MaximumWorkersCount = 250;

    internal static string RetrySuffix(int retry) => $"retry_{retry}";
    internal const string DlqSuffix = "dlq";

    internal static class MessageHeaders
    {
        internal const string Retry = "retry";
        internal const string ErrorMessage = "error.message";
        internal const string ErrorCode = "error.code";
    }
}