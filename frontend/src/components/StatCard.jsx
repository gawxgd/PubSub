import './StatCard.css'

function StatCard({ title, value, icon, color }) {
  return (
    <div className="stat-card" style={{ '--card-color': color }}>
      <div className="stat-card-icon">{icon}</div>
      <div className="stat-card-content">
        <h3 className="stat-card-title">{title}</h3>
        <p className="stat-card-value">{value.toLocaleString()}</p>
      </div>
    </div>
  )
}

export default StatCard

