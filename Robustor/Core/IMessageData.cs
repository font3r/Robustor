namespace Robustor.Core;

public interface IMessageData
{
    string GetTypeName() => GetType().Name;
}