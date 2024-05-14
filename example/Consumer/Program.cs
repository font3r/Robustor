using Consumer;
using Robustor;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKafkaMessageBroker(builder.Configuration);
builder.Services.AddHostedService<KafkaConsumerBackgroundService>();

var app = builder.Build();

app.Run();

public sealed record OrderCreated(Guid Id) : IMessageData;