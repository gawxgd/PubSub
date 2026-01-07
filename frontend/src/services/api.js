import axios from 'axios'

// In production (Docker), nginx proxies /api to messagebroker:5001
// In development, use localhost:5001
const API_BASE_URL = import.meta.env.VITE_API_URL || (import.meta.env.PROD ? '/api' : 'http://localhost:5001/api')

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json',
  },
})

export const fetchStatistics = async () => {
  try {
    const response = await api.get('/statistics')
    return response.data
  } catch (error) {
    // If API is not available, return mock data for development
    // Handle various network errors - always return mock data instead of throwing
    const isNetworkError = 
      error.code === 'ECONNREFUSED' || 
      error.code === 'ERR_NETWORK' ||
      error.code === 'ETIMEDOUT' ||
      error.code === 'ERR_INTERNET_DISCONNECTED' ||
      error.message?.includes('Network Error') ||
      error.message?.includes('timeout') ||
      error.response?.status === 404 ||
      error.response?.status === 500 ||
      !error.response;
    
    if (isNetworkError) {
      console.warn('API not available, using mock data:', error.message || error.code || 'Network error')
      return getMockStatistics()
    }
    
    // For any other errors, still return mock data but log the error
    console.warn('Error fetching statistics, using mock data:', error)
    return getMockStatistics()
  }
}

// Mock data for development
const getMockStatistics = () => {
  return {
    messagesPublished: Math.floor(Math.random() * 10000) + 5000,
    messagesConsumed: Math.floor(Math.random() * 10000) + 5000,
    activeConnections: Math.floor(Math.random() * 10) + 5,
    topics: [
      { name: 'default', messageCount: Math.floor(Math.random() * 5000), lastOffset: Math.floor(Math.random() * 10000) },
      { name: 'orders', messageCount: Math.floor(Math.random() * 3000), lastOffset: Math.floor(Math.random() * 8000) },
      { name: 'events', messageCount: Math.floor(Math.random() * 2000), lastOffset: Math.floor(Math.random() * 6000) },
    ],
    lastUpdate: new Date().toISOString(),
    isMockData: true, // Flag to indicate this is mock data
  }
}

export default api

