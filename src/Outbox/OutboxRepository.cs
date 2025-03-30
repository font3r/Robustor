using System.Data;
using System.Text.Json;
using Dapper;
using Microsoft.Extensions.Options;

namespace Robustor;

public class OutboxRepository(
    IOptions<KafkaConfiguration> kafkaConfiguration,
    IDbConnection dbConnection) : IOutboxRepository
{
    public async Task Add<T>(BaseMessage<T> baseMessage)
        where T : IMessageData
    {
        await dbConnection.ExecuteAsync(
            Variables.Queries.AddMessage(),
            new
            {
                Id = baseMessage.Id,
                // TODO: move config from repo
                Topic = TopicNamingHelper.GetTopicName<T>(kafkaConfiguration.Value.TopicPrefix),
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