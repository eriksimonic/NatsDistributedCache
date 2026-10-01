using System.Text;
using Microsoft.Extensions.Time.Testing;
using NATS.Client.JetStream.Models;
using NatsDistributedCache.Internal;

namespace NatsDistributedCache.UnitTests;

/// <summary>Boundaries and helpers the mutation run (Stryker) showed no test pinned down.</summary>
public sealed class ComponentGapTests
{
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero));

    private L1Item Item(string value, ulong rev, long size = 10) => new L1Item<string>(value, rev, _time.GetUtcNow().AddMinutes(1), size);

    // ------------------------------------------------------------------ KeySignals

    [Fact]
    public void Key_signal_completes_on_its_own_pulse_only_and_is_then_replaced()
    {
        var s = new KeySignals();
        var a = s.Next("orders.1._s1");
        var b = s.Next("users.1._s1");
        Assert.Same(a, s.Next("orders.1._s1")); // one signal per key until pulsed

        s.Pulse("orders.1._s1");

        Assert.True(a.IsCompleted);
        Assert.False(b.IsCompleted);
        Assert.NotSame(a, s.Next("orders.1._s1"));
    }

    [Fact]
    public void Prefix_and_all_pulses_reach_the_right_keys()
    {
        var s = new KeySignals();
        var o1 = s.Next("orders.1._s1");
        var o2 = s.Next("orders.2._s1");
        var u = s.Next("users.1._s1");

        s.PulsePrefix("orders.");
        Assert.True(o1.IsCompleted && o2.IsCompleted);
        Assert.False(u.IsCompleted);

        s.PulseAll();
        Assert.True(u.IsCompleted);
        Assert.Equal(0, s.Count);
    }

    [Fact]
    public void Forget_drops_only_the_callers_own_pending_signal()
    {
        var s = new KeySignals();
        var mine = s.Next("k");
        s.Forget("k", Task.CompletedTask); // someone else's (or an old) signal: keep the entry
        Assert.Equal(1, s.Count);

        s.Forget("k", mine);
        Assert.Equal(0, s.Count);

        var pulsed = s.Next("k");
        s.Pulse("k");
        var next = s.Next("k");
        s.Forget("k", pulsed); // completed and already replaced: the new waiter's signal stays
        Assert.Equal(1, s.Count);
        Assert.False(next.IsCompleted);
    }

    [Fact]
    public void Notify_subjects_follow_the_prefix()
    {
        Assert.Equal("app.notify.orders.1._s1", NotifySubjects.ForKey("app", "orders.1._s1"));
        Assert.Equal("app.notify.orders", NotifySubjects.ForTag("app", "orders"));
        Assert.Equal("app.notify._clear", NotifySubjects.Clear("app"));
    }

    // ------------------------------------------------------------------ OutageJournal

    [Fact]
    public void Journal_with_only_a_prefix_is_not_empty()
    {
        var j = new OutageJournal(10);
        j.RecordPrefix("orders", _time.GetUtcNow());
        Assert.False(j.IsEmpty);
    }

    [Fact]
    public void Journal_holds_exactly_its_capacity_keeps_the_first_write_and_reports_overflow_once()
    {
        var t0 = _time.GetUtcNow();
        var j = new OutageJournal(2);
        j.RecordKey("a", t0);
        j.RecordKey("b", t0);
        Assert.False(j.Overflowed);
        j.RecordKey("a", t0.AddSeconds(5)); // known key at capacity: kept, latest time wins
        Assert.False(j.Overflowed);

        j.RecordKey("c", t0);
        Assert.True(j.Overflowed);

        var (keys, _, overflowed) = j.Drain();
        Assert.True(overflowed);
        Assert.Equal(["a", "b"], keys.Keys.OrderBy(k => k));
        Assert.Equal(t0.AddSeconds(5), keys["a"]);
        Assert.False(j.Drain().Overflowed); // reset by the drain
    }

    [Fact]
    public void Journal_keeps_the_latest_write_whatever_the_order()
    {
        var t0 = _time.GetUtcNow();
        var j = new OutageJournal(10);
        j.RecordKey("a", t0);
        j.RecordKey("a", t0.AddSeconds(5));
        j.RecordKey("a", t0.AddSeconds(2));
        Assert.Equal(t0.AddSeconds(5), j.Drain().Keys["a"]);
    }

    // ------------------------------------------------------------------ L1Store

    [Fact]
    public void L1_rejects_an_entry_larger_than_its_size_limit()
    {
        using var l1 = new L1Store(100, _time);
        l1.Set("big", Item("x", 1, size: 200), TimeSpan.FromMinutes(1));
        l1.Set("small", Item("y", 1, size: 50), TimeSpan.FromMinutes(1));

        Assert.False(l1.TryGet("big", out _));
        Assert.True(l1.TryGet("small", out _));
    }

    [Fact]
    public void L1_refuses_a_zero_ttl()
    {
        using var l1 = new L1Store(1_000, _time);
        Assert.False(l1.Set("k", Item("v", 1), TimeSpan.Zero));
        Assert.False(l1.TryGet("k", out _));
    }

    [Fact]
    public void Prefix_floor_applies_to_its_prefix_only_and_ends_exactly_at_its_ttl()
    {
        using var l1 = new L1Store(1_000, _time);
        l1.EvictPrefix("orders.", 5, TimeSpan.FromSeconds(10));
        Assert.Equal(5UL, l1.Floor("orders.1._s1"));
        Assert.Equal(0UL, l1.Floor("users.1._s1"));

        _time.Advance(TimeSpan.FromSeconds(10));
        Assert.Equal(0UL, l1.Floor("orders.1._s1"));
    }

    [Fact]
    public void Floor_survives_a_newer_entry_for_the_same_key()
    {
        using var l1 = new L1Store(1_000, _time);
        l1.Evict("k", 5, TimeSpan.FromMinutes(1));
        Assert.True(l1.Set("k", Item("v6", 6), TimeSpan.FromSeconds(1)));
        _time.Advance(TimeSpan.FromSeconds(2)); // the entry expires, the floor does not

        Assert.Equal(5UL, l1.Floor("k"));
        Assert.False(l1.Set("k", Item("v4", 4), TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public void Entry_is_gone_exactly_at_its_expiry()
    {
        using var l1 = new L1Store(1_000, _time);
        l1.Set("k", new L1Item<string>("v", 1, _time.GetUtcNow().AddSeconds(10), 10), TimeSpan.FromMinutes(1));
        _time.Advance(TimeSpan.FromSeconds(10));
        Assert.False(l1.TryGet("k", out _));
    }

    [Fact]
    public void Evict_without_a_revision_or_ttl_sets_no_floor()
    {
        using var l1 = new L1Store(1_000, _time);
        l1.Evict("a", 5, TimeSpan.Zero);
        l1.Evict("b", 0, TimeSpan.FromMinutes(1));
        Assert.Equal(0UL, l1.Floor("a"));
        Assert.Equal(0UL, l1.Floor("b"));
    }

    [Fact]
    public void Invalidate_and_raise_floor_set_the_high_water_revision()
    {
        using var l1 = new L1Store(1_000, _time);
        l1.Invalidate("a", 7, TimeSpan.FromMinutes(1));
        l1.RaiseFloor("b", 9, TimeSpan.FromMinutes(1));

        Assert.Equal(7UL, l1.Floor("a"));
        Assert.Equal(9UL, l1.Floor("b"));
        Assert.False(l1.Set("a", Item("old", 6), TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task Evicted_keys_leave_the_index_but_replaced_ones_stay()
    {
        using var l1 = new L1Store(1_000, _time);
        l1.Set("a", Item("v1", 1), TimeSpan.FromMinutes(1));
        l1.Set("a", Item("v2", 2), TimeSpan.FromMinutes(1)); // replaced
        l1.Set("b", Item("v", 1), TimeSpan.FromMinutes(1));
        await Task.Delay(50); // post-eviction callbacks run on the thread pool
        Assert.Equal(2, l1.Count);

        l1.Evict("b", 0, TimeSpan.Zero);

        await DistributedLockTests.Eventually(() => l1.Count == 1, "b left the index");
        l1.Clear();
        Assert.False(l1.TryGet("a", out _)); // the replaced key was still indexed, so Clear found it
    }

    // ------------------------------------------------------------------ keys, tokens, expiration, codecs

    [Theory]
    [InlineData("A")]
    [InlineData("Z")]
    [InlineData("a")]
    [InlineData("z")]
    [InlineData("0")]
    [InlineData("9")]
    [InlineData("-_=/")]
    public void Boundary_characters_are_allowed_in_keys(string key) => CacheKeys.Validate(key);

    [Theory]
    [InlineData("")]
    [InlineData("@")]
    [InlineData("[")]
    [InlineData("`")]
    [InlineData("{")]
    [InlineData(":")]
    public void Empty_keys_and_characters_next_to_the_allowed_ranges_are_rejected(string key) =>
        Assert.Throws<ArgumentException>(() => CacheKeys.Validate(key));

    [Theory]
    [InlineData("A")]
    [InlineData("Z")]
    [InlineData("a-9")]
    public void Boundary_characters_are_allowed_in_the_store_prefix(string prefix) => CacheKeys.ValidateStorePrefix(prefix);

    [Fact]
    public void User_key_of_strips_the_schema_segment_only()
    {
        Assert.Equal("orders.1", CacheKeys.UserKeyOf("orders.1._s1"));
        Assert.Equal("plain", CacheKeys.UserKeyOf("plain"));
    }

    [Theory]
    [InlineData(1, 1, true)]
    [InlineData(86_400_000, 86_400_000, true)] // exactly one day
    [InlineData(0, 1, false)]
    [InlineData(1, 0, false)]
    [InlineData(86_400_001, 1, false)]
    [InlineData(1, 86_400_001, false)]
    public void Lock_token_durations_must_be_in_zero_to_one_day(long leaseMs, long factoryMs, bool valid)
    {
        var parsed = LockToken.Parse(Encoding.UTF8.GetBytes($"node:abc:1000:{leaseMs}:{factoryMs}"));
        Assert.Equal(valid, parsed is not null);
    }

    [Fact]
    public void Explicit_lease_equal_to_the_maximum_is_allowed()
    {
        var e = new Expiration(new NatsCacheOptions(), new FixedRandom(0.5));
        Assert.Equal(TimeSpan.FromSeconds(60), e.LeaseTtl(new CacheEntryOptions { LeaseTtl = TimeSpan.FromSeconds(60) }));
    }

    [Fact]
    public void Default_lease_is_raised_to_the_minimum()
    {
        var e = new Expiration(new NatsCacheOptions(), new FixedRandom(0.5));
        Assert.Equal(TimeSpan.FromSeconds(6), e.LeaseTtl(new CacheEntryOptions { FactoryTimeout = TimeSpan.FromSeconds(1) })); // 1 + 2 s < 6 s
    }

    [Fact]
    public void Zero_l2_ttl_is_rejected_and_nats_ttl_may_equal_the_bucket_max_age()
    {
        var o = new NatsCacheOptions { CacheMaxAge = TimeSpan.FromSeconds(622) };
        var e = new Expiration(o, new FixedRandom(0.5));
        var now = _time.GetUtcNow();
        Assert.Throws<ArgumentOutOfRangeException>(() => e.PlanWrite(new CacheEntryOptions { L2Ttl = TimeSpan.Zero }, now));
        Assert.Equal(TimeSpan.FromSeconds(622), e.PlanWrite(CacheEntryOptions.Default, now)!.Value.NatsTtl); // 600 + 22 s
    }

    [Fact]
    public void L1_plan_validates_the_jitter()
    {
        var e = new Expiration(new NatsCacheOptions(), new FixedRandom(0.5));
        var now = _time.GetUtcNow();
        Assert.Throws<ArgumentOutOfRangeException>(() => e.PlanL1(new CacheEntryOptions { JitterRatio = 1 }, now, now.AddMinutes(10)));
    }

    [Fact]
    public void Absolute_expiration_exactly_at_the_safety_margin_is_not_cached()
    {
        var e = new Expiration(new NatsCacheOptions(), new FixedRandom(0.5));
        var now = _time.GetUtcNow();
        Assert.Null(e.PlanWrite(new CacheEntryOptions { AbsoluteExpiration = now + TimeSpan.FromSeconds(60) }, now));
    }

    [Theory]
    [InlineData("wrong last sequence: 42", 42UL)]
    [InlineData("nats: wrong last sequence: 7", 7UL)]
    [InlineData("something else", null)]
    public void Wrong_last_sequence_is_read_from_the_error_text(string error, ulong? expected)
    {
        var r = new WriteResult(WriteStatus.Rejected, 0, WriteResult.WrongLastSequence, error, "");
        Assert.Equal(expected, r.LastSequenceFromError());
    }

    [Fact]
    public void Incompressible_payloads_above_the_threshold_stay_raw()
    {
        var raw = new byte[2048];
        new Random(42).NextBytes(raw);
        var (payload, encoding) = PayloadCodec.Encode(raw, 1024);
        Assert.Null(encoding);
        Assert.Same(raw, payload);
    }

    [Fact]
    public void Option_defaults_and_schema_version_bounds()
    {
        var o = new NatsCacheOptions();
        Assert.Equal("nats://localhost:4222", o.Url);
        Assert.Equal("", o.Prefix); // required: an unset prefix fails validation
        Assert.Equal(-1, o.CacheMaxBytes);
        o.ForType<int>(1); // 1 is the first valid version
        Assert.Throws<ArgumentOutOfRangeException>(() => o.ForType<long>(0));
    }

    // ------------------------------------------------------------------ NATS parsing and store checks

    [Theory]
    [InlineData("2026-09-30T20:50:06Z", 0L)]
    [InlineData("2026-09-30T20:50:06.12Z", 1_200_000L)]
    [InlineData("2026-09-30T20:50:06.1234567Z", 1_234_567L)]
    [InlineData("2026-09-30T20:50:06.12345678Z", 1_234_567L)]
    public void Nats_timestamps_parse_with_any_fraction_length(string value, long ticks)
    {
        Assert.Equal(new DateTimeOffset(2026, 9, 30, 20, 50, 6, TimeSpan.Zero).AddTicks(ticks), NatsL2Store.ParseTimestamp(value));
    }

    [Fact]
    public void Nats_timestamps_with_an_offset_are_returned_in_utc()
    {
        var t = NatsL2Store.ParseTimestamp("2026-09-30T22:50:06.385421512+02:00");
        Assert.Equal(TimeSpan.Zero, t.Offset);
        Assert.Equal(new DateTimeOffset(2026, 9, 30, 20, 50, 6, TimeSpan.Zero).AddTicks(3854215), t);
    }

    [Fact]
    public void Header_lines_without_a_name_are_skipped_and_markers_are_recognised()
    {
        var block = Convert.ToBase64String(Encoding.UTF8.GetBytes("NATS/1.0\r\n: orphan\r\nnocolon\r\nNats-Marker-Reason: MaxAge\r\n\r\n"));
        var h = NatsL2Store.ParseHeaderBlock(block);
        Assert.Equal(["Nats-Marker-Reason"], h.Keys);
        Assert.Equal(L2Op.Marker, NatsL2Store.OpOf(h));
    }

    [Fact]
    public void Store_check_names_every_problem()
    {
        var bad = new StreamConfig("KV_orders_cache", ["$KV.orders_cache.>"])
        {
            Storage = StreamConfigStorage.Memory, MaxMsgsPerSubject = 5, AllowMsgTTL = false,
        };
        var problems = string.Join("; ", Provisioner.CheckKv(bad));
        Assert.Contains("storage is Memory", problems);
        Assert.Contains("history is 5", problems);
        Assert.Contains("per-message TTL", problems);
        Assert.Contains("delete markers", problems);
    }
}
