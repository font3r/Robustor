using Shouldly;
using Xunit;

namespace Robustor.UnitTests;

public record OrderCreatedEvent;
public record CreateOrderCommand;

public class TopicNamingHelperTests
{
    [Theory]
    [InlineData("test", "test_order_created")]
    [InlineData("dev", "dev_order_created")]
    [InlineData("dev-pl", "dev-pl_order_created")]
    public void GetTopic_ShouldReturnValidTopicNameForEvent(string prefix, string expectedTopicName)
    {
        var topicName = TopicNamingHelper.GetTopicName<OrderCreatedEvent>(prefix);
        
        topicName.ShouldBe(expectedTopicName);
    }
    
    [Theory]
    [InlineData("test", "test_create_order")]
    [InlineData("dev", "dev_create_order")]
    [InlineData("dev-pl", "dev-pl_create_order")]
    public void GetTopic_ShouldReturnValidTopicNameForCommand(string prefix, string expectedTopicName)
    {
        var topicName = TopicNamingHelper.GetTopicName<CreateOrderCommand>(prefix);
        
        topicName.ShouldBe(expectedTopicName);
    }
}
