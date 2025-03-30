namespace Robustor.Core;

public sealed record BaseMessage<T>(T Message)
    where T : IMessageData
{
    public Guid Id { get; init; }
    public string Type { get; init; } = null!;
    public string? TraceContext { get; init; }
    public DateTimeOffset EventOccured { get; init; }
}