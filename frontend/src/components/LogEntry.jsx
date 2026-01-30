import { memo } from 'react'
import { getLogTypeColor, getLogTypeIcon } from './logUtils'

/**
 * Individual log entry component
 * Memoized to prevent unnecessary re-renders when logs list updates
 */
const LogEntry = memo(function LogEntry({ log }) {
    const color = getLogTypeColor(log.type)

    return (
        <div
            className="log-entry"
            style={{ borderLeftColor: color }}
            data-log-id={log.id}
        >
            <div className="log-header">
                <span className="log-type" style={{ color }}>
                    {getLogTypeIcon(log.type)} {log.type.toUpperCase()}
                </span>
                <span className="log-source">{log.source}</span>
                <span className="log-timestamp">
                    {log.timestamp.toLocaleTimeString()}
                </span>
            </div>
            <div className="log-message">{log.message}</div>
        </div>
    )
})

export default LogEntry
