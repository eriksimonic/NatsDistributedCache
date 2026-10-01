using NatsDistributedCache.Internal;

namespace NatsDistributedCache.UnitTests;

public class CacheKeysTests
{
    [Theory]
    [InlineData("orders")]
    [InlineData("orders.42")]
    [InlineData("orders.42.lines")]
    [InlineData("a-b_c=d/e")]
    public void Valid_keys_pass(string key) => CacheKeys.Validate(key);

    [Theory]
    [InlineData("")]
    [InlineData("user:42")]
    [InlineData("orders 42")]
    [InlineData("orders.*")]
    [InlineData("orders.>")]
    [InlineData("ä")]
    public void Invalid_characters_are_rejected(string key) =>
        Assert.Throws<ArgumentException>(() => CacheKeys.Validate(key));

    [Theory]
    [InlineData(".orders")]
    [InlineData("orders.")]
    [InlineData("orders..42")]
    public void Empty_segments_are_rejected(string key) =>
        Assert.Throws<ArgumentException>(() => CacheKeys.Validate(key));

    [Theory]
    [InlineData("orders._s2")]
    [InlineData("_sx")]
    [InlineData("a._s")]
    public void Schema_segment_prefix_is_reserved(string key) =>
        Assert.Throws<ArgumentException>(() => CacheKeys.Validate(key));

    [Fact]
    public void Segments_containing_s_elsewhere_are_fine() => CacheKeys.Validate("orders.x_s2.s_1");

    [Fact]
    public void Keys_longer_than_256_characters_are_rejected()
    {
        CacheKeys.Validate(new string('a', 256));
        Assert.Throws<ArgumentException>(() => CacheKeys.Validate(new string('a', 257)));
    }

    [Fact]
    public void Null_key_throws_argument_null() => Assert.Throws<ArgumentNullException>(() => CacheKeys.Validate(null!));

    [Fact]
    public void Internal_key_appends_the_schema_version()
    {
        Assert.Equal("orders.42._s1", CacheKeys.Internal("orders.42", 1));
        Assert.Equal("orders.42._s7", CacheKeys.Internal("orders.42", 7));
        Assert.Equal("orders.42", CacheKeys.UserKeyOf("orders.42._s7"));
    }

    [Theory]
    [InlineData("orders")]
    [InlineData("orders-eu-1")]
    public void Valid_store_prefixes_pass(string prefix) => CacheKeys.ValidateStorePrefix(prefix);

    [Theory]
    [InlineData("")]
    [InlineData("orders_eu")]
    [InlineData("orders.eu")]
    [InlineData("abcdefghijklmnopqrstuvwxyz0123456")]
    public void Invalid_store_prefixes_are_rejected(string prefix) =>
        Assert.Throws<ArgumentException>(() => CacheKeys.ValidateStorePrefix(prefix));
}
