using System.Threading.Channels;
using Publisher.Domain.Port;

namespace Publisher.Outbound.Adapter;

public sealed class TcpPublisher(string host, int port, uint maxSendAttempts, uint maxQueueSize, uint maxRetryAttempts) : IPublisher, IAsyncDisposable
{
    private readonly Channel<byte[]> _channel = Channel.CreateBounded<byte[]>(
        new BoundedChannelOptions((int)maxQueueSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });
    
    private readonly Channel<byte[]> _deadLetterChannel = Channel.CreateBounded<byte[]>(
        new BoundedChannelOptions((int)maxQueueSize)
        {
            FullMode = BoundedChannelFullMode.DropNewest
        });
    
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    
    private IPublisherConnection? _currentPublisherConnection;
    
    private static readonly TimeSpan BaseRetryDelay = TimeSpan.FromSeconds(1);

    public async Task CreateConnection()
    {
        if (_currentPublisherConnection != null)
        {
            throw new InvalidOperationException("Publisher is already connected");
        }
        
        var retryCount = 0;
        
        while (retryCount < maxRetryAttempts && !_cancellationTokenSource.Token.IsCancellationRequested)
        {
            retryCount++;
            
            try
            {
                _currentPublisherConnection = new TcpPublisherConnection(host, port, maxSendAttempts, _channel.Reader, _deadLetterChannel);
                await _currentPublisherConnection.ConnectAsync();
                break;
            }
            catch (PublisherConnectionException ex)
            {
                var delay = TimeSpan.FromSeconds(BaseRetryDelay.TotalSeconds *
                                                 Math.Min(retryCount, maxRetryAttempts));
                
                Console.WriteLine($"Caught retriable exception {ex.Message}, retrying connection after {delay} delay");

                await SafeDisconnectPublisher();
                
                await Task.Delay(delay, _cancellationTokenSource.Token);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Operation cancelled");
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unrecoverable connection: {ex.Message}");
                
                await SafeDisconnectPublisher();
                throw;
            }
        }
    }

    public async Task PublishAsync(byte[] message)
    {
        await _channel.Writer.WriteAsync(message, _cancellationTokenSource.Token);
    }

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
            Console.WriteLine("Exception while disposing " + ex);
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