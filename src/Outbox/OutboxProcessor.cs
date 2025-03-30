using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Robustor.Outbox;

public class OutboxBackgroundService(IServiceScopeFactory serviceScopeFactory) 
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await using var scope = serviceScopeFactory.CreateAsyncScope();
            var repo = scope.ServiceProvider.GetRequiredService<IOutboxRepository>();
            var messages = (await repo.Get()).ToList();
            
            if (messages.Count == 0)
                continue;

            var producer = scope.ServiceProvider.GetRequiredService<IInternalMessageProducer>();

            foreach (var message in messages)
            {
                await producer.Produce(message.Topic, Guid.NewGuid(), message.Message, stoppingToken);
            }

            await repo.Delete(messages.Select(x => x.Id));
            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }
}