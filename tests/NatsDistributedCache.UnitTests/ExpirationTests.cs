using NatsDistributedCache.Internal;

namespace NatsDistributedCache.UnitTests;

public class ExpirationTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 30, 12, 0, 0, TimeSpan.Zero);
    private static readonly NatsCacheOptions Options = new() { Prefix = "t" };

    private static Expiration With(double u) => new(Options, new FixedRandom(u));

    [Theory]
    [InlineData(0.0, 540)]   // factor 1 - j
    [InlineData(0.5, 600)]   // factor 1
    [InlineData(0.9999999, 660)] // factor -> 1 + j
    public void L2_ttl_jitter_spans_plus_minus_j(double u, int expectedSeconds)
    {
        var plan = With(u).PlanWrite(CacheEntryOptions.Default, Now)!.Value;
        Assert.Equal(expectedSeconds, plan.L2Ttl.TotalSeconds, precision: 0);
    }

    [Fact]
    public void Nats_ttl_is_l2_ttl_plus_grace_rounded_up_to_seconds()
    {
        // Defaults: FactoryTimeout 10 s, lease = FactoryTimeout + 2 s = 12 s, grace 22 s (design section 6).
        var plan = With(0.5).PlanWrite(CacheEntryOptions.Default, Now)!.Value;
        Assert.Equal(TimeSpan.FromSeconds(12), plan.LeaseTtl);
        Assert.Equal(TimeSpan.FromSeconds(622), plan.NatsTtl);
    }

    [Fact]
    public void Absolute_expiration_only_shortens_and_caps_at_the_safety_margin()
    {
        // JWT: exp in 15 min, margin 60 s -> lives 12.6 to 14 min (design section 6).
        var o = CacheEntryOptions.Default with { L2Ttl = TimeSpan.FromMinutes(14), AbsoluteExpiration = Now.AddMinutes(15) };
        Assert.Equal(TimeSpan.FromMinutes(14), With(0.0).PlanWrite(o, Now)!.Value.L2Ttl);
        Assert.Equal(12.6, With(0.9999999).PlanWrite(o, Now)!.Value.L2Ttl.TotalMinutes, precision: 2);

        var longTtl = o with { L2Ttl = TimeSpan.FromHours(1) };
        Assert.Equal(TimeSpan.FromMinutes(14), With(0.0).PlanWrite(longTtl, Now)!.Value.L2Ttl);
    }

    [Fact]
    public void Absolute_expiration_inside_the_margin_is_not_cached()
    {
        var o = CacheEntryOptions.Default with { AbsoluteExpiration = Now.AddSeconds(30) };
        Assert.Null(With(0.5).PlanWrite(o, Now));
    }

    [Fact]
    public void L1_ttl_is_capped_by_the_remaining_l2_life()
    {
        var e = With(0.5);
        Assert.Equal(TimeSpan.FromSeconds(30), e.PlanL1(CacheEntryOptions.Default, Now, Now.AddMinutes(10)));
        Assert.Equal(TimeSpan.FromSeconds(5), e.PlanL1(CacheEntryOptions.Default, Now, Now.AddSeconds(5)));
    }

    [Fact]
    public void Lease_defaults_to_factory_timeout_plus_2s_clamped_to_max()
    {
        var e = With(0.5);
        Assert.Equal(TimeSpan.FromSeconds(12), e.LeaseTtl(CacheEntryOptions.Default));
        Assert.Equal(TimeSpan.FromSeconds(60), e.LeaseTtl(CacheEntryOptions.Default with { FactoryTimeout = TimeSpan.FromSeconds(90) }));
        Assert.Equal(TimeSpan.FromSeconds(20), e.LeaseTtl(CacheEntryOptions.Default with { LeaseTtl = TimeSpan.FromSeconds(20) }));
    }

    [Fact]
    public void Explicit_lease_above_max_throws()
    {
        var o = CacheEntryOptions.Default with { LeaseTtl = TimeSpan.FromSeconds(61) };
        Assert.Throws<ArgumentOutOfRangeException>(() => With(0.5).LeaseTtl(o));
    }

    [Fact]
    public void Ttl_plus_grace_above_the_bucket_max_age_throws()
    {
        var o = CacheEntryOptions.Default with { L2Ttl = TimeSpan.FromHours(24) };
        Assert.Throws<ArgumentOutOfRangeException>(() => With(0.5).PlanWrite(o, Now));
    }

    [Theory]
    [InlineData(-0.1)]
    [InlineData(1.0)]
    [InlineData(double.NaN)]
    public void Jitter_ratio_must_be_in_0_to_1(double j)
    {
        var o = CacheEntryOptions.Default with { JitterRatio = j };
        Assert.Throws<ArgumentOutOfRangeException>(() => With(0.5).PlanWrite(o, Now));
    }

    [Fact]
    public void Jitter_is_uniform_over_the_window()
    {
        // With a real random source, 10 000 draws spread over [540 s, 660 s] with every 12 s bucket used.
        var e = new Expiration(Options, SharedRandomSource.Instance);
        var buckets = new int[10];
        for (var i = 0; i < 10_000; i++)
        {
            var s = e.PlanWrite(CacheEntryOptions.Default, Now)!.Value.L2Ttl.TotalSeconds;
            Assert.InRange(s, 540, 660);
            buckets[Math.Min(9, (int)((s - 540) / 12))]++;
        }

        Assert.All(buckets, b => Assert.InRange(b, 800, 1200));
    }
}
