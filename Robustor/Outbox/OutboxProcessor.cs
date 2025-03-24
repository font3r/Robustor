using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Robustor;

public class OutboxBackgroundService(IServiceScopeFactory serviceScopeFactory) 
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await using var scope = serviceScopeFactory.CreateAsyncScope();
            
            var repo = scope.ServiceProvider.GetRequiredService<IOutboxRepository>();
            var producer = scope.ServiceProvider.GetRequiredService<IMessageProducer>();
            
            var messages = (await repo.Get()).ToList();
            foreach (var message in messages)
                await producer.Produce(message.Topic, message.Message);
            
            await repo.Delete(messages.Select(x => x.Id));
            
            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }
}