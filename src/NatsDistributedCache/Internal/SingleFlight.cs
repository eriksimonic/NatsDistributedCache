using System.Collections.Concurrent;

namespace NatsDistributedCache.Internal;

/// <summary>
/// Local single-flight (design section 5): all callers on one node for the same key share one load, so at
/// most one contender per node reaches L2 and the distributed lock. A caller's cancellation stops only its
/// own wait, never the shared load.
/// </summary>
internal sealed class SingleFlight
{
    private readonly ConcurrentDictionary<string, Task<object?>> _inflight = new(StringComparer.Ordinal);

    public int InFlight => _inflight.Count;
    public async Task<T> RunAsync<T>(string key, Func<Task<T>> load, CancellationToken ct)
    {
        while (true)
        {
            if (_inflight.TryGetValue(key, out var existing))
                return (T)(await WaitAsync(existing, ct).ConfigureAwait(false))!;

            var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!_inflight.TryAdd(key, tcs.Task)) continue;

            _ = tcs.Task.ContinueWith(static t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted); // observed
            _ = Task.Run(async () =>
            {
                object? result = null;
                Exception? error = null;
                try
                {
                    result = await load().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    error = ex;
                }

                // Leave the table before completing: a caller woken by this result that calls again must start a
                // fresh load, not join this finished one (and get its failure instead of a retry).
                _inflight.TryRemove(key, out _);
                if (error is null) tcs.TrySetResult(result);
                else tcs.TrySetException(error);
            }, CancellationToken.None);
            return (T)(await WaitAsync(tcs.Task, ct).ConfigureAwait(false))!;
        }
    }

    private static async Task<object?> WaitAsync(Task<object?> task, CancellationToken ct)
    {
        if (!ct.CanBeCanceled || task.IsCompleted) return await task.ConfigureAwait(false);
        var cancelled = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        using (ct.Register(static s => ((TaskCompletionSource<object?>)s!).TrySetCanceled(), cancelled))
        {
            var done = await Task.WhenAny(task, cancelled.Task).ConfigureAwait(false);
            return await done.ConfigureAwait(false);
        }
    }
}
