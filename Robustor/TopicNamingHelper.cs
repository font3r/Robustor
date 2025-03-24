namespace Robustor;

internal static class TopicNamingHelper
{
    public static string GetTopicName<T>(string prefix)
        => string.Concat(ToSnakeCase(string.Concat(prefix, TrimName(typeof(T).Name))));
    
    public static string GetRetryTopicName<T>(string prefix, int retry)
        => string.Concat(GetTopicName<T>(prefix), Variables.TopicSeparator, Variables.RetrySuffix(retry));
    
    public static string GetDlqTopicName<T>(string prefix)
        => string.Concat(GetTopicName<T>(prefix), Variables.TopicSeparator, Variables.DlqSuffix);

    public static IDictionary<string, TopicType> GetResilienceTopics<T>(string prefix, int retries)
    {
        var topics = new Dictionary<string, TopicType>
        {
            { GetTopicName<T>(prefix), TopicType.Main }
        };

        for (var retry = 1; retry <= retries; retry++)
            topics.Add(GetRetryTopicName<T>(prefix, retry), TopicType.Retry);

        topics.Add(GetDlqTopicName<T>(prefix), TopicType.Dlq);

        return topics;
    }
    
    private static IEnumerable<char> ToSnakeCase(string value)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (char.IsUpper(value[i]) && i != 0) 
                yield return Variables.TopicSeparator;

            yield return char.ToLower(value[i]);
        }   
    }

    private static string TrimName(string value)
    {
        if (value.EndsWith(Variables.CommandSuffix))
            return value.TrimEnd(Variables.CommandSuffix.ToCharArray());
        
        if (value.EndsWith(Variables.EventSuffix))
            return value.TrimEnd(Variables.EventSuffix.ToCharArray());

        return value;
    }
}