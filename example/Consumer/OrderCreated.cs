using Robustor;

public sealed record OrderCreated(Guid Id) : IMessageData;