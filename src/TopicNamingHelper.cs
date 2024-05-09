namespace Robustor;

public static class TopicNamingHelper
{
    public static string GetTopicName<T>(string prefix)
    {
        var typeName = nameof(T);
        
        return ToSnakeCase(string.Concat(prefix, Variables.TopicSeparator, typeName)).ToString();
    }

    private static IEnumerable<char> ToSnakeCase(string value)
    {
        foreach (var @char in value)
        {
            if (char.IsUpper(@char)) 
                yield return '_';

            yield return char.ToLower(@char);
        }
    }
}