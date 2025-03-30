using Robustor.Core;

public sealed record OrderCreated(Guid Id) : IMessageData;