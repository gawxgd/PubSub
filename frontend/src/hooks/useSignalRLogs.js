import { useState, useEffect, useRef, useCallback } from 'react'
import * as signalR from '@microsoft/signalr'

/**
 * Custom hook for managing SignalR connection and receiving logs
 * Handles connection lifecycle, reconnection, and cleanup
 */
export function useSignalRLogs(maxLogsRef) {
    const [logs, setLogs] = useState([])
    const [connectionStatus, setConnectionStatus] = useState('Disconnected')
    const connectionRef = useRef(null)

    useEffect(() => {
        // Cleanup flag to prevent ghost handlers from updating state after unmount
        let isActive = true

        // Create SignalR connection
        const connection = new signalR.HubConnectionBuilder()
            .withUrl('/loggerhub')
            .withAutomaticReconnect()
            .build()

        connectionRef.current = connection

        // Handle connection events
        connection.onclose(() => {
            if (isActive) setConnectionStatus('Disconnected')
        })

        connection.onreconnecting(() => {
            if (isActive) setConnectionStatus('Reconnecting...')
        })

        connection.onreconnected(() => {
            if (isActive) setConnectionStatus('Connected')
        })

        // Counter for guaranteed unique IDs
        let logCounter = 0

        // Listen for log messages
        connection.on('ReceiveLog', (logType, source, message) => {
            // Skip if component was unmounted (prevents ghost handlers from StrictMode)
            if (!isActive) return

            // Generate truly unique ID using crypto.randomUUID if available, 
            // otherwise fallback to timestamp + counter + random
            const uniqueId = typeof crypto !== 'undefined' && crypto.randomUUID
                ? crypto.randomUUID()
                : `${Date.now()}-${++logCounter}-${Math.random().toString(36).substr(2, 9)}`

            const logEntry = {
                id: uniqueId,
                timestamp: new Date(),
                type: logType,
                source: source,
                message: message
            }

            setLogs(prevLogs => {
                const newLogs = [...prevLogs, logEntry]
                // Use ref to always get current maxLogs value
                return newLogs.slice(-maxLogsRef.current)
            })
        })

        // Start connection with retry logic
        const startConnection = async () => {
            try {
                await connection.start()
                if (isActive) setConnectionStatus('Connected')
            } catch (err) {
                // Don't log to console - SignalR will automatically retry
                if (isActive) setConnectionStatus('Disconnected')
                // Retry after 5 seconds
                setTimeout(() => {
                    if (isActive && connectionRef.current?.state === signalR.HubConnectionState.Disconnected) {
                        startConnection()
                    }
                }, 5000)
            }
        }

        startConnection()

        return () => {
            isActive = false
            connection.stop()
        }
    }, [maxLogsRef])

    const clearLogs = useCallback(() => {
        setLogs([])
    }, [])

    return { logs, connectionStatus, clearLogs, setLogs }
}
