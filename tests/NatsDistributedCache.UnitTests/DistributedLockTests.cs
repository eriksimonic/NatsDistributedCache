using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using NatsDistributedCache.Internal;

namespace NatsDistributedCache.UnitTests;

public sealed class DistributedLockTests
{
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(12);
    private static readonly TimeSpan FactoryTimeout = TimeSpan.FromSeconds(10);

    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero));
    private readonly FakeL2Store _locks;

    public DistributedLockTests() => _locks = new FakeL2Store(_time);

    private DistributedLock Lock(string node) => new(_locks, node, _time, NullLogger.Instance);

    internal static async Task Eventually(Func<bool> condition, string what)
    {
        for (var i = 0; i < 500 && !condition(); i++) await Task.Delay(10);
        Assert.True(condition(), what);
    }

    [Fact]
    public void Token_round_trips_and_parses_from_the_right()
    {
        var t = LockToken.New("pod:7", DateTimeOffset.FromUnixTimeMilliseconds(1_000), Lease, FactoryTimeout);
        Assert.Matches("^pod:7:[0-9a-f]{32}:1000:12000:10000$", t.ToString());
        Assert.Equal(t, LockToken.Parse(Encoding.UTF8.GetBytes(t.ToString())));
        Assert.Null(LockToken.Parse(Encoding.UTF8.GetBytes("owner:abc")));
    }

    [Fact]
    public async Task First_acquire_expects_an_empty_subject_and_a_second_one_sees_the_holder()
    {
        var a = await Lock("a").TryAcquireAsync("k", Lease, FactoryTimeout, default);
        Assert.NotNull(a.Lease);
        var put = Assert.Single(_locks.Puts);
        Assert.Equal(0UL, put.Expected);
        Assert.Equal(Lease, put.NatsTtl);

        var b = await Lock("b").TryAcquireAsync("k", Lease, FactoryTimeout, default);
        Assert.Null(b.Lease);
        Assert.Equal(a.Lease!.Token, LockToken.Parse(b.Holder!.Payload));
        Assert.Equal(2, _locks.Puts.Count); // lost after one publish and a leader read, no blind retries
    }

    [Fact]
    public async Task Released_lock_is_free_and_the_next_acquire_expects_the_tombstone()
    {
        var a = (await Lock("a").TryAcquireAsync("k", Lease, FactoryTimeout, default)).Lease!;
        await a.DisposeAsync();
        var tombstone = _locks.Peek("k")!;
        Assert.Equal(L2Op.Delete, tombstone.Op);

        var b = await Lock("b").TryAcquireAsync("k", Lease, FactoryTimeout, default);
        Assert.NotNull(b.Lease);
        Assert.Equal(tombstone.Revision, _locks.Puts[^1].Expected);
    }

    [Fact]
    public async Task Expired_lease_turns_into_a_marker_that_counts_as_free()
    {
        await Lock("crashed").TryAcquireAsync("k", Lease, FactoryTimeout, default); // never released
        _time.Advance(Lease);
        var marker = _locks.Peek("k")!;
        Assert.Equal(L2Op.Marker, marker.Op);

        Assert.NotNull((await Lock("b").TryAcquireAsync("k", Lease, FactoryTimeout, default)).Lease);
        Assert.Equal(marker.Revision, _locks.Puts[^1].Expected);
    }

    [Fact]
    public async Task Unknown_outcome_is_settled_as_owned_when_our_token_is_at_the_head()
    {
        _locks.NextPutLosesReply = true;
        var a = await Lock("a").TryAcquireAsync("k", Lease, FactoryTimeout, default);
        Assert.NotNull(a.Lease);
        Assert.Single(_locks.Puts);
        Assert.Equal(_locks.Peek("k")!.Revision, a.Lease!.Revision);
    }

    [Fact]
    public async Task Stale_owner_release_is_a_no_op()
    {
        var stale = (await Lock("a").TryAcquireAsync("k", Lease, FactoryTimeout, default)).Lease!;
        _time.Advance(Lease + TimeSpan.FromSeconds(1));
        var fresh = (await Lock("b").TryAcquireAsync("k", Lease, FactoryTimeout, default)).Lease!;

        await stale.DisposeAsync();

        var head = _locks.Peek("k")!;
        Assert.Equal(L2Op.Put, head.Op);
        Assert.Equal(fresh.Revision, head.Revision);
    }

    [Fact]
    public async Task Renewal_every_third_of_the_lease_keeps_the_lock_and_release_expects_the_latest_revision()
    {
        var lease = (await Lock("a").TryAcquireAsync("k", Lease, FactoryTimeout, default)).Lease!.StartRenewal();
        var acquired = lease.Revision;
        for (var i = 0; i < 5; i++) // renewals stop at FactoryTimeout + LeaseTtl = 22 s
        {
            await Task.Delay(20); // let the renewal loop arm its timer
            _time.Advance(TimeSpan.FromSeconds(4));
            var expected = i + 1;
            await Eventually(() => lease.Renewals == expected, $"renewal {expected}");
        }

        // 20 s on a 12 s lease: still held, because every renewal reset the server TTL.
        Assert.Equal(L2Op.Put, _locks.Peek("k")!.Op);
        Assert.True(lease.Revision > acquired);
        Assert.All(_locks.Puts.Skip(1), p => Assert.Equal(Lease, p.NatsTtl));

        var latest = lease.Revision;
        await lease.DisposeAsync();
        var tombstone = _locks.Peek("k")!;
        Assert.Equal(L2Op.Delete, tombstone.Op);
        Assert.Equal(latest + 1, tombstone.Revision);
    }

    [Fact]
    public async Task Renewal_that_finds_another_owner_marks_the_lease_lost_and_skips_the_release()
    {
        var lease = (await Lock("a").TryAcquireAsync("k", Lease, FactoryTimeout, default)).Lease!.StartRenewal();
        await _locks.DeleteAsync("k", expected: null, default); // e.g. an operator cleared the lock
        var other = (await Lock("b").TryAcquireAsync("k", Lease, FactoryTimeout, default)).Lease!;

        await Task.Delay(20);
        _time.Advance(TimeSpan.FromSeconds(4));
        await Eventually(() => lease.Lost, "lease lost");
        await lease.DisposeAsync();

        Assert.Equal(other.Revision, _locks.Peek("k")!.Revision);
    }

    [Fact]
    public async Task Rejected_acquire_other_than_wrong_sequence_is_reported_as_unavailable()
    {
        _locks.RejectWritesWith = 10059; // stream not found
        await Assert.ThrowsAsync<L2UnavailableException>(() => Lock("a").TryAcquireAsync("k", Lease, FactoryTimeout, default));
    }

    [Theory]
    [InlineData("a:b:1:0:1000")]
    [InlineData("a:b:1:1000:-5")]
    [InlineData("a:b:1:99999999999999:1000")]
    public void Token_with_out_of_range_durations_is_rejected(string text) =>
        Assert.Null(LockToken.Parse(Encoding.UTF8.GetBytes(text)));

    private async Task AdvanceRenewal(TimeSpan by)
    {
        await Task.Delay(20); // let the renewal loop arm its timer
        _time.Advance(by);
    }

    [Fact]
    public async Task Unknown_renewal_that_did_not_land_is_retried_without_counting_as_confirmed()
    {
        var lease = (await Lock("a").TryAcquireAsync("k", Lease, FactoryTimeout, default)).Lease!.StartRenewal();
        var confirmed = lease.LastConfirmedAt;
        _locks.NextPutUnknownNotApplied = true;

        await AdvanceRenewal(TimeSpan.FromSeconds(4));
        await Eventually(() => _locks.PutsSnapshot().Count == 2, "first renewal sent");
        Assert.Equal(0, lease.Renewals);
        Assert.Equal(confirmed, lease.LastConfirmedAt); // the old TTL is still the one protecting the lock

        await AdvanceRenewal(TimeSpan.FromMilliseconds(250)); // retried soon, not a full interval later
        await Eventually(() => lease.Renewals == 1, "renewal retried");
        Assert.True(lease.LastConfirmedAt > confirmed);
        Assert.False(lease.Lost);
        await lease.DisposeAsync();
        Assert.Equal(L2Op.Delete, _locks.Peek("k")!.Op);
    }

    [Fact]
    public async Task Renewal_that_landed_late_is_adopted_by_the_release()
    {
        var lease = (await Lock("a").TryAcquireAsync("k", Lease, FactoryTimeout, default)).Lease!;
        // An earlier renewal of ours lands after we stopped tracking it: our token at a newer revision.
        await _locks.PutAsync("k", lease.TokenBytes, new Dictionary<string, string>(), Lease, lease.Revision, default);

        await lease.DisposeAsync();

        Assert.Equal(L2Op.Delete, _locks.Peek("k")!.Op); // released anyway, so waiters need not wait a lease
    }

    [Fact]
    public async Task Release_with_a_lost_reply_is_settled_by_a_leader_read()
    {
        var lease = (await Lock("a").TryAcquireAsync("k", Lease, FactoryTimeout, default)).Lease!;
        _locks.NextDeleteLosesReply = true;
        Assert.True(await Lock("a").ReleaseAsync(lease, lease.Revision));
        Assert.Equal(L2Op.Delete, _locks.Peek("k")!.Op);
    }

    [Fact]
    public async Task Outage_during_renewal_keeps_the_lease_while_its_ttl_runs_then_marks_it_lost()
    {
        var lease = (await Lock("a").TryAcquireAsync("k", Lease, FactoryTimeout, default)).Lease!.StartRenewal();
        _locks.Unavailable = true;
        await AdvanceRenewal(TimeSpan.FromSeconds(4));
        await Task.Delay(50);
        Assert.False(lease.Lost); // 4 s of a 12 s lease: still protected

        for (var i = 0; i < 40 && !lease.Lost; i++) await AdvanceRenewal(TimeSpan.FromMilliseconds(250));
        Assert.True(lease.Lost);
        _locks.Unavailable = false;
        await lease.DisposeAsync();
        Assert.Equal(L2Op.Marker, _locks.Peek("k")!.Op); // not released: it expired on the server
    }

    [Fact]
    public async Task Renewal_rejected_for_another_reason_is_not_a_lost_lease()
    {
        var lease = (await Lock("a").TryAcquireAsync("k", Lease, FactoryTimeout, default)).Lease!.StartRenewal();
        _locks.RejectWritesWith = 10077; // bucket full
        await AdvanceRenewal(TimeSpan.FromSeconds(4));
        await Eventually(() => _locks.PutsSnapshot().Count >= 2, "renewal attempted");
        _locks.RejectWritesWith = 0;
        await AdvanceRenewal(TimeSpan.FromMilliseconds(250));
        await Eventually(() => lease.Renewals == 1, "renewed after the rejection");

        Assert.False(lease.Lost);
        await lease.DisposeAsync();
        Assert.Equal(L2Op.Delete, _locks.Peek("k")!.Op);
    }

    [Fact]
    public async Task Renewal_stops_at_factory_timeout_plus_lease_so_a_hung_owner_hands_over_by_expiry()
    {
        var lease = (await Lock("a").TryAcquireAsync("k", Lease, FactoryTimeout, default)).Lease!.StartRenewal();
        for (var i = 0; i < 12; i++) await AdvanceRenewal(TimeSpan.FromSeconds(4)); // 48 s
        await Task.Delay(50);

        // Renewals at 4..20 s (5 of them); the cap is 10 + 12 = 22 s, so the last TTL ran out at 32 s.
        Assert.Equal(5, lease.Renewals);
        Assert.Equal(L2Op.Marker, _locks.Peek("k")!.Op);
        Assert.NotNull((await Lock("b").TryAcquireAsync("k", Lease, FactoryTimeout, default)).Lease);
        await lease.DisposeAsync(); // stale release: a no-op
        Assert.Equal(L2Op.Put, _locks.Peek("k")!.Op);
    }
}
