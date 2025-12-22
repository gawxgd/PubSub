import { useState, useEffect } from 'react'
import './App.css'
import StatisticsDashboard from './components/StatisticsDashboard'
import LogsViewer from './components/LogsViewer'

function App() {
  return (
    <div className="app">
      <header className="app-header">
        <h1>PubSub Statistics Dashboard</h1>
      </header>
      <main className="app-main">
        <StatisticsDashboard />
        <LogsViewer />
      </main>
    </div>
  )
}

export default App

