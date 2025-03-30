namespace Robustor.Outbox;

public sealed class OutboxMessage
{
    public Guid Id { get; init; }
    public string Topic { get; init; } = null!;
    public string Type { get; init; } = null!;
    public string TraceContext { get; init; } = null!;
    public string Message { get; init; } = null!;
    public DateTimeOffset CreatedAt { get; init; }
}