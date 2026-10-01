using System.Text;
using Microsoft.Extensions.Time.Testing;
using NATS.Client.JetStream.Models;
using NatsDistributedCache.Internal;

namespace NatsDistributedCache.UnitTests;

public class PayloadCodecTests
{
    [Fact]
    public void Payloads_at_or_below_the_threshold_are_not_compressed()
    {
        var raw = Encoding.UTF8.GetBytes(new string('x', 4096));
        var (payload, enc) = PayloadCodec.Encode(raw, 4096);
        Assert.Null(enc);
        Assert.Same(raw, payload);
    }

    [Fact]
    public void Payloads_above_the_threshold_are_brotli_compressed_and_round_trip()
    {
        var raw = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat("{\"id\":42,\"name\":\"order\"},", 500)));
        var (payload, enc) = PayloadCodec.Encode(raw, 4096);
        Assert.Equal(PayloadCodec.Brotli, enc);
        Assert.True(payload.Length < raw.Length / 5);
        Assert.Equal(raw, PayloadCodec.Decode(payload, enc));
    }

    [Fact]
    public void Threshold_zero_turns_compression_off()
    {
        var raw = new byte[100_000];
        Assert.Null(PayloadCodec.Encode(raw, 0).Encoding);
    }

    [Fact]
    public void Unknown_encoding_is_rejected() =>
        Assert.Throws<InvalidDataException>(() => PayloadCodec.Decode(new byte[] { 1 }, "zstd"));
}

public class L1StoreTests
{
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 9, 30, 12, 0, 0, TimeSpan.Zero));

    private L1Item Item(object? value, ulong rev) => new(value, rev, _time.GetUtcNow().AddMinutes(1), 10);

    [Fact]
    public void An_older_revision_never_replaces_a_newer_one()
    {
        using var l1 = new L1Store(1_000_000, _time);
        Assert.True(l1.Set("k", Item("v2", 2), TimeSpan.FromMinutes(1)));
        Assert.False(l1.Set("k", Item("v1", 1), TimeSpan.FromMinutes(1)));
        Assert.True(l1.TryGet("k", out var item));
        Assert.Equal("v2", item.Value);
    }

    [Fact]
    public void Evict_raises_a_floor_that_blocks_older_refills_but_not_newer()
    {
        using var l1 = new L1Store(1_000_000, _time);
        l1.Set("k", Item("v5", 5), TimeSpan.FromMinutes(1));
        l1.Evict("k", floorRevision: 7, TimeSpan.FromSeconds(33));
        Assert.False(l1.TryGet("k", out _));
        Assert.False(l1.Set("k", Item("v5", 5), TimeSpan.FromMinutes(1))); // lagging replica returns the deleted value
        Assert.True(l1.Set("k", Item("v8", 8), TimeSpan.FromMinutes(1)));   // a new Set after the delete
    }

    [Fact]
    public void Entries_expire_by_the_injected_clock()
    {
        using var l1 = new L1Store(1_000_000, _time);
        l1.Set("k", Item("v", 1), TimeSpan.FromMinutes(1));
        _time.Advance(TimeSpan.FromSeconds(61));
        Assert.False(l1.TryGet("k", out _));
    }

    [Fact]
    public void Prefix_eviction_removes_keys_under_the_prefix_and_holds_a_floor()
    {
        using var l1 = new L1Store(1_000_000, _time);
        l1.Set("orders.1._s1", Item("a", 1), TimeSpan.FromMinutes(1));
        l1.Set("orders.2._s1", Item("b", 2), TimeSpan.FromMinutes(1));
        l1.Set("users.1._s1", Item("c", 3), TimeSpan.FromMinutes(1));
        l1.EvictPrefix("orders.", floorRevision: 10, TimeSpan.FromSeconds(33));
        Assert.False(l1.TryGet("orders.1._s1", out _));
        Assert.False(l1.TryGet("orders.2._s1", out _));
        Assert.True(l1.TryGet("users.1._s1", out _));
        Assert.False(l1.Set("orders.1._s1", Item("a", 1), TimeSpan.FromMinutes(1)));
        Assert.True(l1.Set("orders.1._s1", Item("a2", 11), TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public void Floors_expire()
    {
        using var l1 = new L1Store(1_000_000, _time);
        l1.EvictPrefix("orders.", floorRevision: 10, TimeSpan.FromSeconds(33));
        _time.Advance(TimeSpan.FromSeconds(34));
        Assert.Equal(0UL, l1.Floor("orders.1._s1"));
    }
}

public class SingleFlightTests
{
    [Fact]
    public async Task Concurrent_callers_share_one_load()
    {
        var flight = new SingleFlight();
        var calls = 0;
        var gate = new TaskCompletionSource();
        var tasks = Enumerable.Range(0, 100).Select(_ => flight.RunAsync("k", async () =>
        {
            Interlocked.Increment(ref calls);
            await gate.Task;
            return 42;
        }, CancellationToken.None)).ToList();
        await Task.Delay(50);
        gate.SetResult();
        Assert.All(await Task.WhenAll(tasks), v => Assert.Equal(42, v));
        Assert.Equal(1, calls);
        Assert.Equal(0, flight.InFlight);
    }

    [Fact]
    public async Task A_failed_load_reaches_all_waiters_and_the_next_call_retries()
    {
        var flight = new SingleFlight();
        await Assert.ThrowsAsync<InvalidOperationException>(() => flight.RunAsync<int>("k", () => throw new InvalidOperationException(), CancellationToken.None));
        Assert.Equal(7, await flight.RunAsync("k", () => Task.FromResult(7), CancellationToken.None));
    }

    [Fact]
    public async Task A_cancelled_caller_does_not_cancel_the_shared_load()
    {
        var flight = new SingleFlight();
        var gate = new TaskCompletionSource();
        using var cts = new CancellationTokenSource();
        var first = flight.RunAsync("k", async () => { await gate.Task; return 1; }, cts.Token);
        var second = flight.RunAsync("k", async () => { await gate.Task; return 2; }, CancellationToken.None);
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        gate.SetResult();
        Assert.Equal(1, await second);
    }
}

public class ProvisionerTests
{
    private static readonly NatsCacheOptions Options = new() { Prefix = "orders" };

    [Fact]
    public void Store_names_follow_the_prefix()
    {
        var p = new Provisioner(Options);
        Assert.Equal("orders_cache", p.CacheConfig().Bucket);
        Assert.Equal("orders_locks", p.LocksConfig().Bucket);
        Assert.Equal("orders_notifications", p.NotificationsConfig().Name);
        Assert.Equal(["orders.notify.>"], p.NotificationsConfig().Subjects!);
        Assert.Equal("orders_objects", p.ObjectsConfig().Bucket);
    }

    [Fact]
    public void Every_store_is_file_backed_replicated_and_kv_buckets_have_history_1_and_markers()
    {
        var p = new Provisioner(Options);
        foreach (var kv in new[] { p.CacheConfig(), p.LocksConfig() })
        {
            Assert.Equal(1, kv.History);
            Assert.Equal(NATS.Client.KeyValueStore.NatsKVStorageType.File, kv.Storage);
            Assert.Equal(3, kv.NumberOfReplicas);
            Assert.Equal(TimeSpan.FromSeconds(30), kv.LimitMarkerTTL);
        }

        Assert.Equal(TimeSpan.FromHours(24), p.CacheConfig().MaxAge);
        Assert.Equal(TimeSpan.FromSeconds(120), p.LocksConfig().MaxAge);
        Assert.Equal(StreamConfigStorage.File, p.NotificationsConfig().Storage);
        Assert.Equal(StreamConfigDiscard.Old, p.NotificationsConfig().Discard);
        Assert.Equal(TimeSpan.FromMinutes(5), p.NotificationsConfig().MaxAge);
        Assert.Equal(NATS.Client.ObjectStore.NatsObjStorageType.File, p.ObjectsConfig().Storage);
    }

    [Fact]
    public void Existing_kv_stream_must_be_file_history_1_with_ttl_and_markers()
    {
        var good = new StreamConfig("KV_orders_cache", ["$KV.orders_cache.>"])
        {
            Storage = StreamConfigStorage.File, MaxMsgsPerSubject = 1, AllowMsgTTL = true, SubjectDeleteMarkerTTL = TimeSpan.FromSeconds(30),
        };
        Assert.Empty(Provisioner.CheckKv(good));

        var bad = new StreamConfig("KV_orders_cache", ["$KV.orders_cache.>"])
        {
            Storage = StreamConfigStorage.Memory, MaxMsgsPerSubject = 5, AllowMsgTTL = false,
        };
        Assert.Equal(4, Provisioner.CheckKv(bad).Count);
    }

    [Theory]
    [InlineData("")]
    [InlineData("bad.prefix")]
    public void Invalid_prefix_fails_validation(string prefix) =>
        Assert.Throws<ArgumentException>(() => Provisioner.ValidateOptions(new NatsCacheOptions { Prefix = prefix }));

    [Fact]
    public void Marker_ttl_below_one_second_fails_validation() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => Provisioner.ValidateOptions(new NatsCacheOptions { Prefix = "p", LimitMarkerTtl = TimeSpan.FromMilliseconds(500) }));

    [Fact]
    public void Nats_timestamps_with_nanoseconds_parse()
    {
        var t = NatsL2Store.ParseTimestamp("2026-09-30T20:50:06.385421512Z");
        Assert.Equal(new DateTimeOffset(2026, 9, 30, 20, 50, 6, TimeSpan.Zero).AddTicks(3854215), t);
    }

    [Fact]
    public void Header_blocks_parse_with_status_line_and_first_value_wins()
    {
        var block = Convert.ToBase64String(Encoding.UTF8.GetBytes("NATS/1.0\r\nKV-Operation: DEL\r\nX: 1\r\nX: 2\r\n\r\n"));
        var h = NatsL2Store.ParseHeaderBlock(block);
        Assert.Equal("DEL", h["KV-Operation"]);
        Assert.Equal("1", h["X"]);
        Assert.Equal(L2Op.Delete, NatsL2Store.OpOf(h));
    }
}
