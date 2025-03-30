using Microsoft.AspNetCore.Mvc;
using Robustor.Core;
using Robustor.Outbox;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKafkaMessageBroker(builder.Configuration)
    .AddOutbox(builder.Configuration);

var app = builder.Build();

app.MapPost("/messages", async ([FromQuery] int count, IOutboxRepository outboxRepository) =>
{
    for (var i = 0; i < count; i++)
    {
        await outboxRepository.Add("robustor_order_created", 
            new BaseMessage<OrderCreated>(new OrderCreated(Guid.NewGuid())));
    }
});

app.MapGet("/outbox", async (IOutboxRepository outboxRepository) =>
    TypedResults.Ok(await outboxRepository.Get()));

app.Run();

public sealed record OrderCreated(Guid Id) : IMessageData;