import { memo } from 'react'

/**
 * Log filtering controls component
 */
const LogFilters = memo(function LogFilters({
    filterLevel,
    setFilterLevel,
    filterSource,
    setFilterSource,
    uniqueSources,
    maxLogs,
    setMaxLogs,
    onClear
}) {
    return (
        <>
            <select
                value={filterLevel}
                onChange={(e) => setFilterLevel(e.target.value)}
                className="filter-select"
            >
                <option value="all">All Levels</option>
                <option value="error">Error</option>
                <option value="warning">Warning</option>
                <option value="info">Info</option>
                <option value="debug">Debug</option>
                <option value="trace">Trace</option>
            </select>
            <select
                value={filterSource}
                onChange={(e) => setFilterSource(e.target.value)}
                className="filter-select"
            >
                <option value="all">All Sources</option>
                {uniqueSources.map(source => (
                    <option key={source} value={source}>{source}</option>
                ))}
            </select>
            <label className="max-logs-label">
                Max logs:
                <input
                    type="number"
                    min="10"
                    max="500000"
                    value={maxLogs}
                    onChange={(e) => setMaxLogs(parseInt(e.target.value) || 10000)}
                    className="max-logs-input"
                    title="Max logs to keep"
                />
            </label>
            <button onClick={onClear} className="clear-button">
                Clear
            </button>
        </>
    )
})

export default LogFilters
