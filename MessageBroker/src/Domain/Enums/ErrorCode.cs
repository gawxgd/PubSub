namespace MessageBroker.Domain.Enums;

public enum ErrorCode : short
{
    None = 0,
    InvalidMessageFormat = 1,
    TopicNotFound = 2,
    InvalidTopic = 3,
    InvalidOffset = 4,
    InternalError = 5,
    Unauthorized = 6,
    ServiceUnavailable = 7
}

