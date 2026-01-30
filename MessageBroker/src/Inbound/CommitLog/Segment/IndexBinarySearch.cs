namespace MessageBroker.Inbound.CommitLog.Segment;

public static class IndexBinarySearch
{

    public static TEntry? Search<TKey, TEntry>(
        int entryCount,
        Func<int, TEntry> readEntryAt,
        Func<TEntry, TKey> getKey,
        TKey targetKey)
        where TKey : IComparable<TKey>
    {
        if (entryCount == 0)
            return default!;

        int left = 0;
        int right = entryCount - 1;
        var best = default(TEntry);

        while (left <= right)
        {
            int mid = left + (right - left) / 2;
            var entry = readEntryAt(mid);
            var key = getKey(entry);

            int cmp = key.CompareTo(targetKey);

            if (cmp == 0)
            {
                return entry;
            }

            if (cmp < 0)
            {
                best = entry;
                left = mid + 1;
            }
            else
            {
                right = mid - 1;
            }
        }

        return best;
    }
}
