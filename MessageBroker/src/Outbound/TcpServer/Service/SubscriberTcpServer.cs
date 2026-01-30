using MessageBroker.Domain.Enums;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port;
using MessageBroker.Infrastructure.Configuration.Options;
using Microsoft.Extensions.Options;

namespace MessageBroker.Outbound.TcpServer.Service;

public class SubscriberTcpServer(CreateSocketUseCase createSocketUseCase, IConnectionManager connectionManager, IOptionsMonitor<TcpServerOptions> optionsMonitor)
    : Inbound.TcpServer.Service.TcpServer(ConnectionType.Subscriber, optionsMonitor.CurrentValue.SubscriberPort, createSocketUseCase, connectionManager);
