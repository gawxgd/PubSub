using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using MessageBroker.Domain.Entities;
using MessageBroker.Domain.Port.CommitLog.RecordBatch;
using Publisher.Configuration.Options;
using Publisher.Domain.Logic;
using Publisher.Domain.Port;
using Publisher.Outbound.Exceptions;

namespace Publisher.Outbound.Adapter;

public sealed class TcpPublisher<T>(
    PublisherOptions options,
    SerializeMessageUseCase<T> serializeMessageUseCase,
    ILogRecordBatchWriter batchWriter)
    : IPublisher<T>, IAsyncDisposable
{
    private readonly IAutoLogger _logger = AutoLoggerFactory.CreateLogger<TcpPublisher<T>>(LogSource.Publisher);
    private readonly TimeSpan _baseRetryDelay = TimeSpan.FromSeconds(1);
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    private readonly Channel<byte[]> _channel = Channel.CreateBounded<byte[]>(
        new BoundedChannelOptions((int)options.MaxPublisherQueueSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });

    private readonly Channel<byte[]> _deadLetterChannel = Channel.CreateBounded<byte[]>(
        new BoundedChannelOptions((int)options.MaxPublisherQueueSize)
        {
            FullMode = BoundedChannelFullMode.DropNewest
        });

    private IPublisherConnection? _currentPublisherConnection;
    
    public ChannelReader<PublishResponse>? ErrorResponses =>
        (_currentPublisherConnection as TcpPublisherConnection)
        ?.ResponseHandler.ErrorResponses;

    public async ValueTask DisposeAsync()
    {
        try
        {
            await _cancellationTokenSource.CancelAsync();

            _channel.Writer.TryComplete();
            _deadLetterChannel.Writer.TryComplete();
            
            await SafeDisconnectPublisher();

            _cancellationTokenSource.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogError("Exception while disposing", ex);
        }
    }

    public async Task CreateConnection()
    {
        if (_currentPublisherConnection != null)
        {
            throw new InvalidOperationException("Publisher is already connected");
        }

        var retryCount = 0;

        while (retryCount < options.MaxRetryAttempts && !_cancellationTokenSource.Token.IsCancellationRequested)
        {
            retryCount++;

            try
            {
                var batchUseCase = new BatchMessagesUseCase(batchWriter);
                _currentPublisherConnection = new TcpPublisherConnection(
                    options,
                    _channel.Reader,
                    _deadLetterChannel,
                    batchUseCase);
                await _currentPublisherConnection.ConnectAsync();
                return;
            }
            catch (PublisherConnectionException ex)
            {
                var delay = TimeSpan.FromSeconds(_baseRetryDelay.TotalSeconds *
                                                 Math.Min(retryCount, options.MaxRetryAttempts));

                _logger.LogWarning($"Caught retriable exception {ex.Message}, retrying connection after {delay} delay",
                    ex);

                await SafeDisconnectPublisher();

                await Task.Delay(delay, _cancellationTokenSource.Token);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInfo("Operation cancelled");
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Unrecoverable connection: {ex.Message}", ex);

                await SafeDisconnectPublisher();
                throw;
            }
        }

        throw new PublisherConnectionException($"Failed to connect to broker after {retryCount} retries");
    }

    public async Task PublishAsync(T message)
    {
        var serializedMessage = await serializeMessageUseCase.Serialize(message);

        await _channel.Writer.WriteAsync(serializedMessage, _cancellationTokenSource.Token);
    }

    public async Task<bool> WaitForAcknowledgmentsAsync(int count, TimeSpan timeout)
    {
        if (_currentPublisherConnection is TcpPublisherConnection tcpConnection)
        {
            return await tcpConnection.ResponseHandler.WaitForAcknowledgmentsAsync(count, timeout,
                _cancellationTokenSource.Token);
        }

        _logger.LogWarning("WaitForAcknowledgmentsAsync called but connection is not a TcpPublisherConnection");
        return false;
    }


    public long AcknowledgedCount
    {
        get
        {
            if (_currentPublisherConnection is TcpPublisherConnection tcpConnection)
            {
                return tcpConnection.ResponseHandler.AcknowledgedCount;
            }

            return 0;
        }
    }

    private async Task SafeDisconnectPublisher()
    {
        if (_currentPublisherConnection != null)
        {
            await _currentPublisherConnection.DisconnectAsync();
        }
    }
}