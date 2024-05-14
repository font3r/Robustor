namespace Robustor;

public static class TopicNamingHelper
{
    public static string GetTopicName<T>(string prefix)
        => string.Concat(ToSnakeCase(string.Concat(prefix, TrimName(typeof(T).Name))));
    
    public static string GetRetryTopicName<T>(string prefix, int retry)
        => string.Concat(GetTopicName<T>(prefix), Variables.TopicSeparator, Variables.RetrySuffix(retry));
    
    public static string GetDlqTopicName<T>(string prefix)
        => string.Concat(GetTopicName<T>(prefix), Variables.TopicSeparator, Variables.DlqSuffix);
    
    public static int? GetRetryFromTopicName(string topic, TopicConfiguration topicConfiguration)
    {
        for (var retry = 1; retry <= topicConfiguration.RetryCount; retry++)
        {
            if (topic.EndsWith(Variables.RetrySuffix(retry)))
                return retry;    
        }

        return null;
    }

    public static IEnumerable<string> GetResilienceTopics<T>(string prefix, int retries)
    {
        yield return GetTopicName<T>(prefix);
        
        for (var retry = 1; retry <= retries; retry++)
            yield return GetRetryTopicName<T>(prefix, retry);

        yield return GetDlqTopicName<T>(prefix);
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