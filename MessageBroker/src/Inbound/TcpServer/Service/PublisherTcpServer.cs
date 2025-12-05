using MessageBroker.Domain.Enums;
using MessageBroker.Domain.Logic.TcpServer.UseCase;
using MessageBroker.Domain.Port;

namespace MessageBroker.Inbound.TcpServer.Service;

public class PublisherTcpServer(CreateSocketUseCase createSocketUseCase, IConnectionManager connectionManager)
    : TcpServer(ConnectionType.Publisher, createSocketUseCase, connectionManager);