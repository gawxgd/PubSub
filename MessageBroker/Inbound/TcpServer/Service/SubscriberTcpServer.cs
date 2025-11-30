using MessageBroker.Domain.Enums;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port;

namespace MessageBroker.Inbound.TcpServer.Service;

public class SubscriberTcpServer(CreateSocketUseCase createSocketUseCase, IConnectionManager connectionManager)
    : TcpServer(ConnectionType.Subscriber, createSocketUseCase, connectionManager);