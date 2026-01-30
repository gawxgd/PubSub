using FluentAssertions;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Port.CommitLog.Segment;
using MessageBroker.Domain.Port.CommitLog.TopicSegmentManager;
using MessageBroker.Inbound.CommitLog;
using MessageBroker.Infrastructure.Configuration.Options.CommitLog;
using Microsoft.Extensions.Options;
using NSubstitute;
using Xunit;

namespace MessageBroker.UnitTests.Inbound.CommitLog;

public class CommitLogFactoryTests : IDisposable
{
    private readonly ILogSegmentFactory _segmentFactory;
    private readonly ITopicSegmentRegistryFactory _topicSegmentRegistryFactory;
    private readonly string _testDirectory;

    public CommitLogFactoryTests()
    {
        var logger = Substitute.For<ILogger>();
        AutoLoggerFactory.Initialize(logger);

        _testDirectory = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDirectory);

        _segmentFactory = Substitute.For<ILogSegmentFactory>();
        _topicSegmentRegistryFactory = Substitute.For<ITopicSegmentRegistryFactory>();

        _segmentFactory.CreateLogSegment(Arg.Any<string>(), Arg.Any<ulong>())
            .Returns(call => new MessageBroker.Domain.Entities.CommitLog.LogSegment(
                Path.Combine(_testDirectory, "test.log"),
                Path.Combine(_testDirectory, "test.index"),
                Path.Combine(_testDirectory, "test.timeindex"),
                0,
                0
            ));

        _segmentFactory.CreateWriter(Arg.Any<MessageBroker.Domain.Entities.CommitLog.LogSegment>())
            .Returns(Substitute.For<ILogSegmentWriter>());
    }

    [Fact]
    public void Get_Should_Create_Appender_For_Configured_Topic()
    {
        // Arrange
        var factory = CreateFactory("test-topic");

        // Act
        var appender = factory.GetAppender("test-topic");

        // Assert
        appender.Should().NotBeNull();
        _topicSegmentRegistryFactory.Received(1).GetOrCreate("test-topic", Arg.Any<string>(), Arg.Any<ulong>());
    }

    [Fact]
    public void Get_Should_Throw_When_Topic_Not_Configured()
    {
        // Arrange
        var factory = CreateFactory("test-topic");

        // Act
        var act = () => factory.GetAppender("unknown-topic");

        // Assert
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Topic 'unknown-topic' is not configured*");
    }

    [Fact]
    public void GetReader_Should_Throw_When_Topic_Not_Configured()
    {
        var factory = CreateFactory("test-topic");

        var act = () => factory.GetReader("unknown-topic");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*Topic 'unknown-topic' is not configured*");
    }

    [Fact]
    public void Get_Should_Return_Same_Instance_For_Same_Topic()
    {
        // Arrange
        var factory = CreateFactory("test-topic");

        // Act
        var appender1 = factory.GetAppender("test-topic");
        var appender2 = factory.GetAppender("test-topic");

        // Assert
        appender1.Should().BeSameAs(appender2);
        _topicSegmentRegistryFactory.Received(1).GetOrCreate("test-topic", Arg.Any<string>(), Arg.Any<ulong>());
    }

    [Fact]
    public void Get_Should_Create_Appender_With_Custom_Directory()
    {
        // Arrange
        var customDirectory = Path.Combine(_testDirectory, "custom-logs");
        var factory = CreateFactoryWithCustomDirectory("test-topic", customDirectory);

        // Act
        var appender = factory.GetAppender("test-topic");

        // Assert
        appender.Should().NotBeNull();
        _topicSegmentRegistryFactory.Received().GetOrCreate("test-topic", customDirectory, Arg.Any<ulong>());
    }

    [Fact]
    public void Get_Should_Use_Default_Directory_When_Not_Specified()
    {
        // Arrange
        var factory = CreateFactory("test-topic");

        // Act
        var appender = factory.GetAppender("test-topic");

        // Assert
        appender.Should().NotBeNull();
        _topicSegmentRegistryFactory.Received().GetOrCreate(
            "test-topic",
            Arg.Is<string>(d => d.Contains(_testDirectory) && d.Contains("test-topic")),
            Arg.Any<ulong>());
    }

    [Fact]
    public void Get_Should_Create_Appender_With_Base_Offset()
    {
        // Arrange
        const ulong expectedOffset = 12345;
        var factory = CreateFactory("test-topic", baseOffset: expectedOffset);

        // Act
        var appender = factory.GetAppender("test-topic");

        // Assert
        appender.Should().NotBeNull();
        _topicSegmentRegistryFactory.Received().GetOrCreate("test-topic", Arg.Any<string>(), expectedOffset);
    }

    [Fact]
    public void Get_Should_Create_Multiple_Appenders_For_Different_Topics()
    {
        // Arrange
        var factory = CreateFactoryWithMultipleTopics();

        // Act
        var appender1 = factory.GetAppender("topic1");
        var appender2 = factory.GetAppender("topic2");

        // Assert
        appender1.Should().NotBeNull();
        appender2.Should().NotBeNull();
        appender1.Should().NotBeSameAs(appender2);
    }

    [Fact]
    public void Dispose_Should_Not_Throw()
    {
        // Arrange
        var factory = CreateFactory("test-topic");
        var appender = factory.GetAppender("test-topic");

        // Act
        var act = () => factory.DisposeAsync();

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void GetReader_Should_Create_Reader_For_Configured_Topic()
    {
        var factory = CreateFactory("test-topic");

        var reader = factory.GetReader("test-topic");

        reader.Should().NotBeNull();
        _topicSegmentRegistryFactory.Received(1).GetOrCreate(Arg.Is("test-topic"), Arg.Any<string>(), Arg.Any<ulong>());
    }

    [Fact]
    public void GetReader_Should_Return_Same_Instance_For_Same_Topic()
    {
        var factory = CreateFactory("test-topic");

        var r1 = factory.GetReader("test-topic");
        var r2 = factory.GetReader("test-topic");

        r1.Should().BeSameAs(r2);
    }

    private CommitLogFactory CreateFactory(
        string topicName,
        ulong baseOffset = 0,
        uint flushIntervalMs = 100)
    {
        var commitLogOptions = Substitute.For<IOptions<CommitLogOptions>>();
        commitLogOptions.Value.Returns(new CommitLogOptions
        {
            Directory = _testDirectory
        });

        var topicOptions = Substitute.For<IOptions<List<CommitLogTopicOptions>>>();
        topicOptions.Value.Returns(new List<CommitLogTopicOptions>
        {
            new CommitLogTopicOptions
            {
                Name = topicName,
                BaseOffset = baseOffset,
                FlushIntervalMs = flushIntervalMs
            }
        });

        var manager = Substitute.For<ITopicSegmentRegistry>();
        manager.GetActiveSegment().Returns(new MessageBroker.Domain.Entities.CommitLog.LogSegment(
            Path.Combine(_testDirectory, "00000000000000000000.log"),
            Path.Combine(_testDirectory, "00000000000000000000.index"),
            Path.Combine(_testDirectory, "00000000000000000000.timeindex"),
            baseOffset,
            baseOffset));
        _topicSegmentRegistryFactory.GetOrCreate(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ulong>())
            .Returns(manager);
        var segmentReader = Substitute.For<ILogSegmentReader>();
        _segmentFactory.CreateReader(Arg.Any<MessageBroker.Domain.Entities.CommitLog.LogSegment>())
            .Returns(segmentReader);

        return new CommitLogFactory(_segmentFactory, _topicSegmentRegistryFactory, commitLogOptions, topicOptions);
    }

    private CommitLogFactory CreateFactoryWithCustomDirectory(
        string topicName,
        string directory,
        ulong baseOffset = 0,
        uint flushIntervalMs = 100)
    {
        var commitLogOptions = Substitute.For<IOptions<CommitLogOptions>>();
        commitLogOptions.Value.Returns(new CommitLogOptions
        {
            Directory = _testDirectory
        });

        var topicOptions = Substitute.For<IOptions<List<CommitLogTopicOptions>>>();
        topicOptions.Value.Returns(new List<CommitLogTopicOptions>
        {
            new CommitLogTopicOptions
            {
                Name = topicName,
                BaseOffset = baseOffset,
                FlushIntervalMs = flushIntervalMs,
                Directory = directory
            }
        });

        var manager = Substitute.For<ITopicSegmentRegistry>();
        manager.GetActiveSegment().Returns(new MessageBroker.Domain.Entities.CommitLog.LogSegment(
            Path.Combine(directory, "00000000000000000000.log"),
            Path.Combine(directory, "00000000000000000000.index"),
            Path.Combine(directory, "00000000000000000000.timeindex"),
            baseOffset,
            baseOffset));
        _topicSegmentRegistryFactory.GetOrCreate(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ulong>())
            .Returns(manager);
        var segmentReader = Substitute.For<ILogSegmentReader>();
        _segmentFactory.CreateReader(Arg.Any<MessageBroker.Domain.Entities.CommitLog.LogSegment>())
            .Returns(segmentReader);

        return new CommitLogFactory(_segmentFactory, _topicSegmentRegistryFactory, commitLogOptions, topicOptions);
    }

    private CommitLogFactory CreateFactoryWithMultipleTopics()
    {
        var commitLogOptions = Substitute.For<IOptions<CommitLogOptions>>();
        commitLogOptions.Value.Returns(new CommitLogOptions
        {
            Directory = _testDirectory
        });

        var topicOptions = Substitute.For<IOptions<List<CommitLogTopicOptions>>>();
        topicOptions.Value.Returns(new List<CommitLogTopicOptions>
        {
            new CommitLogTopicOptions { Name = "topic1", BaseOffset = 0, FlushIntervalMs = 100 },
            new CommitLogTopicOptions { Name = "topic2", BaseOffset = 0, FlushIntervalMs = 100 }
        });

        var manager = Substitute.For<ITopicSegmentRegistry>();
        manager.GetActiveSegment().Returns(new MessageBroker.Domain.Entities.CommitLog.LogSegment(
            Path.Combine(_testDirectory, "00000000000000000000.log"),
            Path.Combine(_testDirectory, "00000000000000000000.index"),
            Path.Combine(_testDirectory, "00000000000000000000.timeindex"),
            0,
            0));
        _topicSegmentRegistryFactory.GetOrCreate(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ulong>())
            .Returns(manager);
        var segmentReader = Substitute.For<ILogSegmentReader>();
        _segmentFactory.CreateReader(Arg.Any<MessageBroker.Domain.Entities.CommitLog.LogSegment>())
            .Returns(segmentReader);

        return new CommitLogFactory(_segmentFactory, _topicSegmentRegistryFactory, commitLogOptions, topicOptions);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDirectory))
        {
            try
            {
                Directory.Delete(_testDirectory, true);
            }
            catch
            {
            }
        }
    }
}
