import { useState, useEffect, useRef, useCallback, useId } from 'react'
import { useSignalRLogs } from '../hooks/useSignalRLogs'
import ConnectionStatus from './ConnectionStatus'
import LogFilters from './LogFilters'
import LogEntry from './LogEntry'
import './LogsViewer.css'

function LogsViewer() {
  // Unique ID for this component instance (for debugging)
  const instanceId = useId()

  const [maxLogs, setMaxLogs] = useState(100)
  const [filterLevel, setFilterLevel] = useState('all')
  const [filterSource, setFilterSource] = useState('all')
  const [isScrolledToBottom, setIsScrolledToBottom] = useState(true)
  const [showScrollButton, setShowScrollButton] = useState(false)
  // Key to force list remount on clear
  const [listKey, setListKey] = useState(0)

  const logsEndRef = useRef(null)
  const logsContainerRef = useRef(null)
  // Ref to always have current maxLogs value in the SignalR handler
  const maxLogsRef = useRef(maxLogs)

  // Keep maxLogsRef in sync with maxLogs state
  useEffect(() => {
    maxLogsRef.current = maxLogs
  }, [maxLogs])

  // Use custom hook for SignalR connection
  const { logs, connectionStatus, clearLogs: clearLogsFromHook } = useSignalRLogs(maxLogsRef)

  // Check if user is scrolled to bottom
  const checkIfScrolledToBottom = useCallback(() => {
    if (!logsContainerRef.current) return false
    const container = logsContainerRef.current
    const threshold = 100 // pixels from bottom
    const isAtBottom = container.scrollHeight - container.scrollTop - container.clientHeight < threshold
    setIsScrolledToBottom(isAtBottom)
    return isAtBottom
  }, [])

  // Handle scroll events
  useEffect(() => {
    const container = logsContainerRef.current
    if (!container) return

    const handleScroll = () => {
      checkIfScrolledToBottom()
    }

    container.addEventListener('scroll', handleScroll)
    return () => container.removeEventListener('scroll', handleScroll)
  }, [checkIfScrolledToBottom])

  // Auto-scroll to bottom when new logs arrive (only if user was already at bottom)
  useEffect(() => {
    if (isScrolledToBottom && logsEndRef.current) {
      logsEndRef.current.scrollIntoView({ behavior: 'smooth' })
    }
    // Check scroll position after logs update
    const timeoutId = setTimeout(() => checkIfScrolledToBottom(), 100)
    return () => clearTimeout(timeoutId)
  }, [logs, isScrolledToBottom, checkIfScrolledToBottom])

  const scrollToBottom = () => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' })
    setIsScrolledToBottom(true)
    setShowScrollButton(false)
  }

  const handleClearLogs = () => {
    console.log(`[${instanceId}] CLEAR called - setting logs to empty array`)
    clearLogsFromHook()
    setListKey(k => k + 1) // Force list to remount
  }

  // Handle filter changes - also increment listKey to force list rebuild
  const handleFilterLevelChange = (value) => {
    console.log(`[${instanceId}] Filter level changed: ${filterLevel} -> ${value}`)
    setFilterLevel(value)
    setListKey(k => k + 1) // Force list to remount to clear stale DOM
  }

  const handleFilterSourceChange = (value) => {
    console.log(`[${instanceId}] Filter source changed: ${filterSource} -> ${value}`)
    setFilterSource(value)
    setListKey(k => k + 1) // Force list to remount to clear stale DOM
  }

  // Filter logs based on current filter settings
  const filteredLogs = logs.filter(log => {
    if (filterLevel !== 'all' && log.type.toLowerCase() !== filterLevel.toLowerCase()) {
      return false
    }
    if (filterSource !== 'all' && log.source !== filterSource) {
      return false
    }
    return true
  })

  // Debug: Log render info
  console.log(`[${instanceId}] RENDER: logs=${logs.length}, filtered=${filteredLogs.length}, filter=${filterLevel}, listKey=${listKey}`)

  const uniqueSources = [...new Set(logs.map(log => log.source))].sort()

  // Update scroll button visibility when filtered logs change
  useEffect(() => {
    if (logsContainerRef.current) {
      const container = logsContainerRef.current
      const threshold = 100
      const isAtBottom = container.scrollHeight - container.scrollTop - container.clientHeight < threshold
      setShowScrollButton(!isAtBottom && filteredLogs.length > 0)
    }
  }, [filteredLogs.length])

  return (
    <div className="logs-viewer" data-instance-id={instanceId}>
      <div className="logs-header">
        <h2>📋 Logs</h2>
        <div className="logs-controls">
          <ConnectionStatus status={connectionStatus} />
          <LogFilters
            filterLevel={filterLevel}
            setFilterLevel={handleFilterLevelChange}
            filterSource={filterSource}
            setFilterSource={handleFilterSourceChange}
            uniqueSources={uniqueSources}
            maxLogs={maxLogs}
            setMaxLogs={setMaxLogs}
            onClear={handleClearLogs}
          />
        </div>
      </div>
      <div className="logs-container" ref={logsContainerRef}>
        {showScrollButton && (
          <button className="scroll-to-bottom-button" onClick={scrollToBottom} title="Scroll to bottom">
            ⬇️ Scroll to bottom
          </button>
        )}
        {filteredLogs.length === 0 ? (
          <div className="logs-empty">
            <p>No logs to display</p>
            {connectionStatus !== 'Connected' && (
              <p className="logs-empty-hint">
                {connectionStatus === 'Reconnecting...'
                  ? 'Reconnecting to logger hub...'
                  : connectionStatus === 'Connection Error'
                    ? 'Cannot connect to logger hub. Make sure MessageBroker is running on port 5001.'
                    : 'Waiting for connection to logger hub. Make sure MessageBroker is running.'}
              </p>
            )}
          </div>
        ) : (
          <div className="logs-list" key={listKey} data-list-key={listKey}>
            {filteredLogs.map(log => (
              <LogEntry key={log.id} log={log} />
            ))}
            <div ref={logsEndRef} />
          </div>
        )}
      </div>
      <div className="logs-footer">
        <span>Showing {filteredLogs.length} of {logs.length} logs</span>
      </div>
    </div>
  )
}

export default LogsViewer
