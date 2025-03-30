using System.Text.Json;
using Confluent.Kafka;

namespace Robustor.Core;

public class GuidKeySerializer : ISerializer<Guid>, IDeserializer<Guid>
{
    public byte[] Serialize(Guid data, SerializationContext context) => data.ToByteArray(true);
    public Guid Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) => new (data);
}

public class BaseMessageSerializer<TMessage>
    : ISerializer<TMessage>, IDeserializer<TMessage>
    where TMessage : IMessageData
{
    public byte[] Serialize(TMessage data, SerializationContext context)
        => JsonSerializer.SerializeToUtf8Bytes(data);

    public TMessage Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        var message = JsonSerializer.Deserialize<TMessage>(data);
        if (message is null)
            throw new ArgumentNullException(nameof(message), "Incoming message is null");
        
        return message;
    }
}

public static class IdBasedPartitioner
{
    public static Partition Partitioner(string topic, int partitionCount, ReadOnlySpan<byte> keyData, bool keyIsNull)
        => new(1);
}