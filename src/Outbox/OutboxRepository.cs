using System.Data;
using System.Text.Json;
using Dapper;
using Robustor.Core;

namespace Robustor.Outbox;

public class OutboxRepository(
    IDbConnection dbConnection) : IOutboxRepository
{
    public async Task Add<T>(string topic, BaseMessage<T> baseMessage)
        where T : IMessageData
    {
        await dbConnection.ExecuteAsync(
            Variables.Queries.AddMessage(),
            new
            {
                Id = baseMessage.Id,
                Topic = topic,
                TraceContext = baseMessage.TraceContext,
                Message = JsonSerializer.Serialize(baseMessage),
                CreatedAt = DateTimeOffset.Now
            });
    }
    
    public async Task<IEnumerable<OutboxMessage>> Get()
        => await dbConnection.QueryAsync<OutboxMessage>(
            Variables.Queries.GetMessages(), 
            new { limit = Variables.Configuration.DefaultPageSize });
    
    public async Task Delete(IEnumerable<Guid> ids)
        => await dbConnection.ExecuteAsync(
            Variables.Queries.DeleteMessages(),
            new { Ids = ids });
}