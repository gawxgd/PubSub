// Utility functions for log type styling

export const getLogTypeColor = (type) => {
    switch (type.toLowerCase()) {
        case 'error':
            return '#ef4444'
        case 'warning':
            return '#f59e0b'
        case 'info':
            return '#3b82f6'
        case 'debug':
            return '#6b7280'
        case 'trace':
            return '#9ca3af'
        default:
            return '#6b7280'
    }
}

export const getLogTypeIcon = (type) => {
    switch (type.toLowerCase()) {
        case 'error':
            return '❌'
        case 'warning':
            return '⚠️'
        case 'info':
            return 'ℹ️'
        case 'debug':
            return '🔍'
        case 'trace':
            return '🔎'
        default:
            return '📝'
    }
}
