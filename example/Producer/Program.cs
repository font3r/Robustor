using Robustor;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKafkaMessageBroker(builder.Configuration);

var app = builder.Build();

app.MapPost("/messages", async (IMessageProducer messageProducer) =>
{
    await messageProducer.Produce(new OrderCreated(Guid.NewGuid()));
});

app.Run();

public sealed record OrderCreated(Guid Id) : IMessageData;