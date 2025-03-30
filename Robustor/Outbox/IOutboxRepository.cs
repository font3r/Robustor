using Robustor.Core;

namespace Robustor.Outbox;

public interface IOutboxRepository
{
    Task Add<T>(string topic, BaseMessage<T> baseMessage)
        where T : IMessageData;
    Task<IEnumerable<OutboxMessage>> Get();
    Task Delete(IEnumerable<Guid> ids);
}