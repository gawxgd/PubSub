import { useState, useEffect } from 'react'
import './StatisticsDashboard.css'
import StatCard from './StatCard'
import { fetchStatistics } from '../services/api'

function StatisticsDashboard() {
  const [stats, setStats] = useState({
    messagesPublished: 0,
    messagesConsumed: 0,
    activeConnections: 0,
    topics: [],
    lastUpdate: null
  })
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [usingMockData, setUsingMockData] = useState(false)

  useEffect(() => {
    const loadStatistics = async () => {
      try {
        setLoading(true)
        setError(null)
        const data = await fetchStatistics()
        setStats(data)
        // Check if we're using mock data by checking if API call failed
        // This is a simple check - in production, API should return a flag
        setUsingMockData(data.isMockData || false)
      } catch (err) {
        // This should rarely happen now since fetchStatistics always returns data
        console.error('Unexpected error loading statistics:', err)
        setError('Unexpected error occurred')
      } finally {
        setLoading(false)
      }
    }

    loadStatistics()
    
    // Refresh every 5 seconds
    const interval = setInterval(loadStatistics, 5000)
    
    return () => clearInterval(interval)
  }, [])

  if (loading && !stats.lastUpdate) {
    return (
      <div className="dashboard-loading">
        <div className="spinner"></div>
        <p>Loading statistics...</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="dashboard-error">
        <p>Error: {error}</p>
        <button onClick={() => window.location.reload()}>Retry</button>
      </div>
    )
  }

  return (
    <div className="statistics-dashboard">
      <div className="stats-grid">
        <StatCard
          title="Messages Published"
          value={stats.messagesPublished}
          icon="📤"
          color="#667eea"
        />
        <StatCard
          title="Messages Delivered"
          value={stats.messagesConsumed}
          icon="📥"
          color="#764ba2"
        />
        <StatCard
          title="Active Connections"
          value={stats.activeConnections}
          icon="🔌"
          color="#f093fb"
        />
        <StatCard
          title="Topics"
          value={stats.topics?.length || 0}
          icon="📋"
          color="#4facfe"
        />
      </div>

      {stats.topics && stats.topics.length > 0 && (
        <div className="topics-section">
          <h2>Topics</h2>
          <div className="topics-list">
            {stats.topics.map((topic, index) => (
              <div key={index} className="topic-card">
                <h3>{topic.name}</h3>
                <div className="topic-stats">
                  <span>Messages: {topic.messageCount || 0}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="last-update">
        {stats.lastUpdate && (
          <span>Last updated: {new Date(stats.lastUpdate).toLocaleTimeString()}</span>
        )}
        {usingMockData && (
          <span className="mock-data-indicator">⚠️ Using mock data (API not available)</span>
        )}
      </div>
    </div>
  )
}

export default StatisticsDashboard

