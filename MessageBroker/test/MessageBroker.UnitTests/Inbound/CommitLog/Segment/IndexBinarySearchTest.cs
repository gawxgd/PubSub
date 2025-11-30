using MessageBroker.Inbound.CommitLog.Segment;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.CommitLog.Segment;

public class IndexBinarySearchTests
{
    private readonly TestEntry[] _entries = new[]
    {
        new TestEntry(10, 100),
        new TestEntry(20, 200),
        new TestEntry(30, 300),
        new TestEntry(40, 400),
        new TestEntry(50, 500),
    };

    private record TestEntry(ulong Key, long Position);

    [Fact]
    public void Search_ExactMatch_ReturnsEntry()
    {
        var target = 30UL;

        var result = IndexBinarySearch.Search(
            entryCount: _entries.Length,
            readEntryAt: i => _entries[i],
            getKey: e => e.Key,
            targetKey: target);

        Assert.NotNull(result);
        Assert.Equal(30UL, result.Key);
        Assert.Equal(300L, result.Position);
    }

    [Fact]
    public void Search_NoExactMatch_ReturnsClosestSmaller()
    {
        var target = 35UL; // between 30 and 40

        var result = IndexBinarySearch.Search(
            entryCount: _entries.Length,
            readEntryAt: i => _entries[i],
            getKey: e => e.Key,
            targetKey: target);

        Assert.NotNull(result);
        Assert.Equal(30UL, result.Key); // closest smaller
        Assert.Equal(300L, result.Position);
    }

    [Fact]
    public void Search_SmallerThanAll_ReturnsDefault()
    {
        var target = 5UL; // smaller than the first key

        var result = IndexBinarySearch.Search(
            entryCount: _entries.Length,
            readEntryAt: i => _entries[i],
            getKey: e => e.Key,
            targetKey: target);

        Assert.Null(result); // default(TEntry) is null for reference/record
    }

    [Fact]
    public void Search_LargerThanAll_ReturnsLast()
    {
        var target = 60UL; // larger than last entry

        var result = IndexBinarySearch.Search(
            entryCount: _entries.Length,
            readEntryAt: i => _entries[i],
            getKey: e => e.Key,
            targetKey: target);

        Assert.NotNull(result);
        Assert.Equal(50UL, result.Key); // last entry
    }

    [Fact]
    public void Search_EmptyIndex_ReturnsDefault()
    {
        var result = IndexBinarySearch.Search<ulong, TestEntry?>(
            entryCount: 0,
            readEntryAt: i => null, // must return TEntry? to match the type
            getKey: e => e!.Key,    // use null-forgiving since e can be null
            targetKey: 20UL);

        Assert.Null(result);
    }
}