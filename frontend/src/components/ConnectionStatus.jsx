import { memo } from 'react'

/**
 * Connection status indicator component
 */
const ConnectionStatus = memo(function ConnectionStatus({ status }) {
    const getStatusIcon = () => {
        switch (status) {
            case 'Connected':
                return '🟢'
            case 'Reconnecting...':
                return '🟡'
            default:
                return '🔴'
        }
    }

    return (
        <div className="connection-status">
            <span className={`status-indicator ${status.toLowerCase().replace(/\s+/g, '-')}`}>
                {getStatusIcon()}
            </span>
            <span>{status}</span>
        </div>
    )
})

export default ConnectionStatus
