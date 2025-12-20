import { useState, useEffect, useRef, useCallback } from 'react'
import * as signalR from '@microsoft/signalr'
import './LogsViewer.css'

function LogsViewer() {
  const [logs, setLogs] = useState([])
  const [connectionStatus, setConnectionStatus] = useState('Disconnected')
  const [maxLogs, setMaxLogs] = useState(100)
  const [filterLevel, setFilterLevel] = useState('all')
  const [filterSource, setFilterSource] = useState('all')
  const [isScrolledToBottom, setIsScrolledToBottom] = useState(true)
  const [showScrollButton, setShowScrollButton] = useState(false)
  const connectionRef = useRef(null)
  const logsEndRef = useRef(null)
  const logsContainerRef = useRef(null)

  useEffect(() => {
    // Create SignalR connection
    const connection = new signalR.HubConnectionBuilder()
      .withUrl('/loggerhub')
      .withAutomaticReconnect()
      .build()

    connectionRef.current = connection

    // Handle connection events
    connection.onclose(() => {
      setConnectionStatus('Disconnected')
    })

    connection.onreconnecting(() => {
      setConnectionStatus('Reconnecting...')
    })

    connection.onreconnected(() => {
      setConnectionStatus('Connected')
    })

    // Listen for log messages
    connection.on('ReceiveLog', (logType, source, message) => {
      const logEntry = {
        id: Date.now() + Math.random(),
        timestamp: new Date(),
        type: logType,
        source: source,
        message: message
      }

      setLogs(prevLogs => {
        const newLogs = [...prevLogs, logEntry]
        // Keep only the last maxLogs entries
        return newLogs.slice(-maxLogs)
      })
    })

    // Start connection with retry logic
    const startConnection = async () => {
      try {
        await connection.start()
        setConnectionStatus('Connected')
      } catch (err) {
        // Don't log to console - SignalR will automatically retry
        setConnectionStatus('Disconnected')
        // Retry after 5 seconds
        setTimeout(() => {
          if (connectionRef.current?.state === signalR.HubConnectionState.Disconnected) {
            startConnection()
          }
        }, 5000)
      }
    }

    startConnection()

    return () => {
      connection.stop()
    }
  }, [maxLogs])

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

  const clearLogs = () => {
    setLogs([])
  }

  const getLogTypeColor = (type) => {
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

  const getLogTypeIcon = (type) => {
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

  const filteredLogs = logs.filter(log => {
    if (filterLevel !== 'all' && log.type.toLowerCase() !== filterLevel.toLowerCase()) {
      return false
    }
    if (filterSource !== 'all' && log.source !== filterSource) {
      return false
    }
    return true
  })

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
    <div className="logs-viewer">
      <div className="logs-header">
        <h2>📋 Logs</h2>
        <div className="logs-controls">
          <div className="connection-status">
            <span className={`status-indicator ${connectionStatus.toLowerCase().replace(/\s+/g, '-')}`}>
              {connectionStatus === 'Connected' ? '🟢' : connectionStatus === 'Reconnecting...' ? '🟡' : '🔴'}
            </span>
            <span>{connectionStatus}</span>
          </div>
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
          <input
            type="number"
            min="10"
            max="1000"
            value={maxLogs}
            onChange={(e) => setMaxLogs(parseInt(e.target.value) || 100)}
            className="max-logs-input"
            title="Max logs to keep"
          />
          <button onClick={clearLogs} className="clear-button">
            Clear
          </button>
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
          <div className="logs-list">
            {filteredLogs.map(log => (
              <div
                key={log.id}
                className="log-entry"
                style={{ borderLeftColor: getLogTypeColor(log.type) }}
              >
                <div className="log-header">
                  <span className="log-type" style={{ color: getLogTypeColor(log.type) }}>
                    {getLogTypeIcon(log.type)} {log.type.toUpperCase()}
                  </span>
                  <span className="log-source">{log.source}</span>
                  <span className="log-timestamp">
                    {log.timestamp.toLocaleTimeString()}
                  </span>
                </div>
                <div className="log-message">{log.message}</div>
              </div>
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

