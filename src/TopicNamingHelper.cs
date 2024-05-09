namespace Robustor;

public static class TopicNamingHelper
{
    public static string GetTopicName<T>(string prefix)
        => string.Concat(ToSnakeCase(string.Concat(prefix, TrimName(typeof(T).Name))));

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