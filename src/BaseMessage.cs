using System.Diagnostics;

namespace Robustor;

public sealed record BaseMessage<T>(T Data)
    where T : IMessageData
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public string TraceId { get; init; } = Activity.Current?.TraceId.ToString();
    public DateTimeOffset EventOccured { get; init; } = DateTimeOffset.UtcNow;
}