namespace Robustor;

public interface IOutboxRepository
{
    Task Add<T>(BaseMessage<T> baseMessage)
        where T : IMessageData;
    Task<IEnumerable<OutboxMessage>> Get();
    Task Delete(IEnumerable<Guid> ids);
}