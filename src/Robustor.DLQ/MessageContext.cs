namespace Robustor;

public class MessageContext
{
    public MessageStatus Result { get; private set; }
    public string ErrorMessage { get; private set; }
    public string ErrorCode { get; private set; }

    public static MessageContext Success()
        => new()
        {
            Result = MessageStatus.Success
        };

    public static MessageContext Error(string message, string code = null)
        => new()
        {
            Result = MessageStatus.Error,
            ErrorMessage = message,
            ErrorCode = code
        };
}