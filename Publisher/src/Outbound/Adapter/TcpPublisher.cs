using System.Threading.Channels;
using LoggerLib.Domain.Enums;
using LoggerLib.Domain.Port;
using LoggerLib.Outbound.Adapter;
using Publisher.Configuration.Options;
using Publisher.Domain.Logic;
using Publisher.Domain.Port;
using Publisher.Outbound.Exceptions;

namespace Publisher.Outbound.Adapter;

public sealed class TcpPublisher<T>(PublisherOptions options, SerializeMessageUseCase<T> serializeMessageUseCase)
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

    public async ValueTask DisposeAsync()
    {
        try
        {
            await _cancellationTokenSource.CancelAsync();

            _channel.Writer.TryComplete();

            await _channel.Reader.Completion;

            await SafeDisconnectPublisher();

            _deadLetterChannel.Writer.TryComplete();

            await _deadLetterChannel.Reader.Completion;

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
                _currentPublisherConnection =
                    new TcpPublisherConnection(options,
                        _channel.Reader, _deadLetterChannel);
                await _currentPublisherConnection.ConnectAsync();
                break;
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
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Unrecoverable connection: {ex.Message}", ex);

                await SafeDisconnectPublisher();
                throw;
            }
        }
    }

    public async Task PublishAsync(T message)
    {
        var serializedMessage = await serializeMessageUseCase.Serialize(message);

        await _channel.Writer.WriteAsync(serializedMessage, _cancellationTokenSource.Token);
    }

    private async Task SafeDisconnectPublisher()
    {
        if (_currentPublisherConnection != null)
        {
            await _currentPublisherConnection.DisconnectAsync();
        }
    }
}