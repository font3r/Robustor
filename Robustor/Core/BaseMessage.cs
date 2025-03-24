using System.Diagnostics;

namespace Robustor;

public sealed record BaseMessage<T>(T Message)
    where T : IMessageData
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public string? TraceContext { get; init; } = Activity.Current?.Id;
    public DateTimeOffset EventOccured { get; init; } = DateTimeOffset.Now;
}