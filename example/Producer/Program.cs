using Microsoft.AspNetCore.Mvc;
using Robustor;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKafkaMessageBroker(builder.Configuration);

var app = builder.Build();

app.MapPost("/messages", async ([FromQuery] int count, IMessageProducer messageProducer) =>
{
    for (var i = 0; i < count; i++)
    {
        await messageProducer.Produce(new OrderCreated(Guid.NewGuid()));    
    }
});

app.Run();

public sealed record OrderCreated(Guid Id) : IMessageData;