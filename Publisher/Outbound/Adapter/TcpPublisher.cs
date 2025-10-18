using System.IO.Pipelines;
using System.Net.Sockets;
using System.Threading.Channels;
using Publisher.Domain.Port;
using Publisher.Outbound.Exceptions;

namespace Publisher.Outbound.Adapter;

public sealed class TcpPublisher(string host, int port, uint maxQueueSize) : IPublisher, IAsyncDisposable
{
    private const int MaxRetryAttemptsBeforeBackoff = 5;

    private const int MaxSendAttempts = 5;

    // ToDo add a dead letter queue
    private static readonly TimeSpan BaseRetryDelay = TimeSpan.FromSeconds(1);

    private readonly Channel<byte[]> _channel = Channel.CreateBounded<byte[]>(
        new BoundedChannelOptions((int)maxQueueSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });

    private readonly CancellationTokenSource _cts = new();

    private readonly Channel<byte[]> _deadLetterChannel = Channel.CreateBounded<byte[]>(
        new BoundedChannelOptions((int)maxQueueSize)
        {
            FullMode = BoundedChannelFullMode.DropNewest
        });

    private TcpClient? _client;
    private PipeWriter? _pipeWriter;
    private Task? _processChannelTask;

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();

        _channel.Writer.TryComplete();

        if (_processChannelTask != null)
        {
            try
            {
                await _processChannelTask;
            }
            catch
            {
                Console.WriteLine("Error while awaiting sender task");
            }
        }

        if (_pipeWriter != null)
        {
            try
            {
                await _pipeWriter.FlushAsync();
                await _pipeWriter.CompleteAsync();
            }
            catch
            {
                Console.WriteLine("Error when disposing publishers pipe writer");
            }
        }

        _client?.Dispose();
        _cts.Dispose();
    }

    public async Task ConnectAsync(CancellationToken cancellationToken)
    {
        await HandleConnectionToBroker(cancellationToken);
        _processChannelTask = Task.Run(() => ProcessChannelAsync(_cts.Token), _cts.Token);
    }

    public async Task PublishAsync(byte[] message, CancellationToken cancellationToken = default)
    {
        if (_client is null)
        {
            throw new PublisherException("Client is not created");
        }

        if (!_client.Connected)
        {
            throw new PublisherException("Publisher is not connected");
        }

        await _channel.Writer.WriteAsync(message, cancellationToken);
    }

    private async Task HandleConnectionToBroker(CancellationToken cancellationToken)
    {
        var retryCount = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            _client?.Dispose();
            _client = new TcpClient();
            retryCount++;

            try
            {
                await _client.ConnectAsync(host, port, cancellationToken);
                _pipeWriter = PipeWriter.Create(_client.GetStream());
                Console.WriteLine($"Connected to broker on {_client.Client.RemoteEndPoint}");
                break;
            }
            catch (SocketException ex) when (CanRetrySocketException(ex))
            {
                var delay = TimeSpan.FromSeconds(BaseRetryDelay.TotalSeconds *
                                                 Math.Min(retryCount, MaxRetryAttemptsBeforeBackoff));

                try
                {
                    await Task.Delay(delay, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unrecoverable connection error: {ex.Message}");
                throw new PublisherException($"Unrecoverable connection error {ex.Message}", ex);
            }
        }
    }

    private async Task ProcessChannelAsync(CancellationToken cancellationToken)
    {
        await foreach (var msg in _channel.Reader.ReadAllAsync(cancellationToken))
        {
            var attempts = 0;
            var sent = false;

            while (!sent && attempts < MaxSendAttempts && !cancellationToken.IsCancellationRequested)
            {
                attempts++;

                try
                {
                    if (_pipeWriter == null)
                    {
                        await HandleReconnectAsync(cancellationToken);
                        continue;
                    }

                    var buffer = _pipeWriter.GetMemory(msg.Length);
                    msg.CopyTo(buffer);
                    _pipeWriter.Advance(msg.Length);

                    var result = await _pipeWriter.FlushAsync(cancellationToken);
                    if (result.IsCompleted || result.IsCanceled)
                    {
                        await HandleReconnectAsync(cancellationToken);
                        continue;
                    }

                    sent = true;
                }
                catch (IOException ex)
                {
                    Console.WriteLine($"IO exception: {ex.Message} retrying connection");
                    await HandleReconnectAsync(cancellationToken);
                }
                catch (SocketException ex) when (CanRetrySocketException(ex))
                {
                    Console.WriteLine($"Retriable socket exception: {ex.Message} retrying connection");
                    await HandleReconnectAsync(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unexpected error while sending message: {ex.Message}");
                    break;
                }
            }

            if (!sent)
            {
                SendToDeadLetterNoBlocking(msg);
            }
        }
    }

    private async Task HandleReconnectAsync(CancellationToken cancellationToken)
    {
        if (_pipeWriter != null)
        {
            await _pipeWriter.CompleteAsync();

            _pipeWriter = null;
        }

        await HandleConnectionToBroker(cancellationToken);
    }

    private void SendToDeadLetterNoBlocking(byte[] msg)
    {
        if (!_deadLetterChannel.Writer.TryWrite(msg))
        {
            Console.WriteLine("Dead-letter queue full, newest message dropped.");
            return;
        }

        Console.WriteLine($"Send {msg} to dead letter queue");
    }

    private static bool CanRetrySocketException(SocketException ex)
    {
        return ex.SocketErrorCode switch
        {
            SocketError.TimedOut or
                SocketError.NetworkDown or
                SocketError.NetworkUnreachable or
                SocketError.HostUnreachable or
                SocketError.Interrupted or
                SocketError.ConnectionAborted or
                SocketError.ConnectionReset or
                SocketError.ProcessLimit or
                SocketError.SystemNotReady or
                SocketError.TryAgain => true,
            _ => false
        };
    }
}