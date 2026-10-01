using Microsoft.Extensions.Time.Testing;

namespace NatsDistributedCache.UnitTests;

/// <summary>
/// A FakeTimeProvider that counts timers once they are armed (given a finite due time), so a test can wait until a
/// background loop has armed its next delay before advancing the clock; advancing first would skip that tick.
/// Task.Delay creates its timer unarmed and arms it with Change, so creation alone is not enough.
/// </summary>
internal sealed class CountingTimeProvider(DateTimeOffset start) : FakeTimeProvider(start)
{
    private int _armed;

    public int Timers => Volatile.Read(ref _armed);

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = new CountingTimer(base.CreateTimer(callback, state, dueTime, period), this);
        if (dueTime != Timeout.InfiniteTimeSpan) Interlocked.Increment(ref _armed);
        return timer;
    }

    private sealed class CountingTimer(ITimer inner, CountingTimeProvider owner) : ITimer
    {
        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            var changed = inner.Change(dueTime, period);
            if (dueTime != Timeout.InfiniteTimeSpan) Interlocked.Increment(ref owner._armed);
            return changed;
        }

        public void Dispose() => inner.Dispose();

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }
}
