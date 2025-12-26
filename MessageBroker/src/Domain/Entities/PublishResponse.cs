using MessageBroker.Domain.Enums;

namespace MessageBroker.Domain.Entities;

public sealed record PublishResponse(ulong BaseOffset, ErrorCode ErrorCode);

