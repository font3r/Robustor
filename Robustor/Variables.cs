namespace Robustor;

internal static class Variables
{
    internal const string KafkaConfigurationSection = "Kafka";
    
    internal const char TopicSeparator = '_';
    internal const string CommandSuffix = "Command";
    internal const string EventSuffix = "Event";

    internal static readonly TimeSpan GlobalRequestTimeout = TimeSpan.FromSeconds(10);
    internal static readonly TimeSpan GlobalOperationTimeout = TimeSpan.FromSeconds(10);
    internal static readonly TimeSpan BaseMessageRetryDelay = TimeSpan.FromSeconds(1);

    internal const int MaximumWorkersCount = 250;

    internal static string RetrySuffix(int retry) => $"retry_{retry}";
    internal const string DlqSuffix = "dlq";

    internal static class MessageHeaders
    {
        internal const string ErrorRetry = "error.retry";
        internal const string ErrorMessage = "error.message";
        internal const string ErrorCode = "error.code";
        
        internal const string Id = "message.id";
        internal const string Type = "message.type";
        internal const string TraceContext = "message.trace_context";
        internal const string EventOccured = "message.event_occured";
    }
    
    public static class Configuration
    {
        public const int DefaultPageSize = 100;
        public const string DefaultConnection = "DefaultConnection";
    }
    
    public static class Queries
    {
        public static string AddMessage()
            => """
                INSERT INTO Outbox (Id, Topic, Type, TraceContext, Message, CreatedAt)
                VALUES (@Id, @Topic, @Type, @TraceContext, @Message, @CreatedAt)
               """;
        
        public static string GetMessages()
            => """
                    SELECT Id, Topic, Type, TraceContext, Message, CreatedAt 
                    FROM Outbox 
                    ORDER BY CreatedAt 
                    OFFSET 0 ROWS FETCH NEXT @limit ROWS ONLY;
                """;

        public static string DeleteMessages()
            => """
                    DELETE FROM Outbox WHERE Id IN @Ids
               """;
    }
}