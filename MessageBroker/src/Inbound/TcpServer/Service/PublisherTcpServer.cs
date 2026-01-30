using MessageBroker.Domain.Enums;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port;
using MessageBroker.Infrastructure.Configuration.Options;
using Microsoft.Extensions.Options;

namespace MessageBroker.Inbound.TcpServer.Service;

public class PublisherTcpServer(CreateSocketUseCase createSocketUseCase, IConnectionManager connectionManager, IOptionsMonitor<TcpServerOptions> optionsMonitor)
    : TcpServer(ConnectionType.Publisher, optionsMonitor.CurrentValue.PublisherPort, createSocketUseCase, connectionManager);
