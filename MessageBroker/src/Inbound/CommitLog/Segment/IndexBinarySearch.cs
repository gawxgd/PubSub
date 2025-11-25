namespace MessageBroker.Inbound.CommitLog.Segment;

public static class IndexBinarySearch
{
    /// <summary>
    /// Searches an index file using binary search and returns the best entry
    /// whose key <= targetKey.
    /// </summary>
    /// <typeparam name="TKey">The key type (ulong for both indexes)</typeparam>
    /// <typeparam name="TEntry">The entry type (OffsetIndexEntry, TimeIndexEntry, etc)</typeparam>
    /// <param name="entryCount">Entries in the index file</param>
    /// <param name="readEntryAt">Callback to read entry at index i</param>
    /// <param name="getKey">Callback to extract key from entry</param>
    /// <param name="targetKey">Key to search for</param>
    /// <returns>Best matching entry (key <= targetKey); or default entry if none exist</returns>
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
                return entry; // exact match
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