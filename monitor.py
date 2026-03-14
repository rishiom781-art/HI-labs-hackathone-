import time
import threading
from datetime import datetime
from data_loader import load_data, find_stuck_ros, find_critical_markets

class RealtimeMonitor:
    """Monitors pipeline in real-time and sends alerts"""
    
    def __init__(self, check_interval=60):
        self.check_interval = check_interval
        self.alerts = []
        self.thresholds = {
            'stuck': 1,  # Alert if >1 stuck
            'market_critical': 50,  # Alert if market <50%
            'failure_rate': 5  # Alert if failure rate >5%
        }
        
        # Start monitoring thread
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop)
        self.thread.daemon = True
        self.thread.start()
        
        print(f"📡 Realtime Monitor active (checking every {check_interval}s)")
    
    def _monitor_loop(self):
        """Continuous monitoring loop"""
        while self.running:
            try:
                self._check_pipeline()
                time.sleep(self.check_interval)
            except Exception as e:
                print(f"Monitor error: {e}")
    
    def _check_pipeline(self):
        """Check pipeline health"""
        roster, market = load_data()
        
        # Check stuck operations
        stuck = find_stuck_ros(roster)
        if len(stuck) > self.thresholds['stuck']:
            self._trigger_alert('PIPELINE', f"{len(stuck)} operations stuck")
        
        # Check critical markets
        critical = find_critical_markets(market)
        if not critical.empty:
            for _, row in critical.iterrows():
                if row['SCS_PERCENT'] < self.thresholds['market_critical']:
                    self._trigger_alert('MARKET', 
                        f"{row['MARKET']} at {row['SCS_PERCENT']}% success")
        
        # Check failure rate
        fail_rate = len(roster[roster['IS_FAILED'] == 1]) / len(roster) * 100
        if fail_rate > self.thresholds['failure_rate']:
            self._trigger_alert('SYSTEM', f"Failure rate at {fail_rate:.1f}%")
    
    def _trigger_alert(self, alert_type, message):
        """Trigger an alert"""
        alert = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'type': alert_type,
            'message': message,
            'severity': 'HIGH' if 'critical' in message.lower() else 'MEDIUM'
        }
        self.alerts.append(alert)
        print(f"\n🚨 ALERT [{alert['timestamp']}] {alert['type']}: {alert['message']}")
    
    def get_alerts(self, since=None):
        """Get alerts since timestamp"""
        if since:
            return [a for a in self.alerts if a['timestamp'] > since]
        return self.alerts
    
    def stop(self):
        """Stop monitoring"""
        self.running = False
        print("🛑 Monitor stopped")

# Test if run directly
if __name__ == "__main__":
    print("Testing RealtimeMonitor...")
    m = RealtimeMonitor(5)
    try:
        time.sleep(15)
    finally:
        m.stop()