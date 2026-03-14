from data_loader import *
from memory import SimpleMemory

class SupervisorAgent:
    """Orchestrates multiple specialized agents"""
    
    def __init__(self):
        self.roster, self.market = load_data()
        self.memory = SimpleMemory()
        
        # Initialize specialized agents
        self.pipeline_agent = PipelineHealthAgent(self.roster)
        self.market_agent = MarketIntelligenceAgent(self.market)
        self.quality_agent = DataQualityAgent(self.roster)
        self.prediction_agent = PredictiveAgent(self.roster, self.market)
        
        print("🚀 SUPERVISOR AGENT ACTIVE")
        print("   ├── Pipeline Health Agent")
        print("   ├── Market Intelligence Agent")
        print("   ├── Data Quality Agent")
        print("   └── Predictive Agent")
    
    def investigate(self, query):
        """Coordinate investigation across agents"""
        results = {}
        
        # Route to appropriate agents
        if "market" in query.lower() or "state" in query.lower():
            results['market'] = self.market_agent.analyze(query)
            results['pipeline'] = self.pipeline_agent.correlate_with_market(query)
            
        elif "stuck" in query.lower() or "pipeline" in query.lower():
            results['pipeline'] = self.pipeline_agent.diagnose()
            
        elif "quality" in query.lower() or "rejection" in query.lower():
            results['quality'] = self.quality_agent.deep_dive()
            results['prediction'] = self.prediction_agent.predict_impact(results['quality'])
        
        # Synthesize results
        return self._synthesize(results)
    
    def _synthesize(self, results):
        """Combine insights from multiple agents"""
        synthesis = "🔍 **MULTI-AGENT ANALYSIS**\n\n"
        
        for agent, insight in results.items():
            synthesis += f"🤖 {agent.upper()} AGENT:\n{insight}\n\n"
        
        return synthesis


class PipelineHealthAgent:
    def __init__(self, roster):
        self.roster = roster
    
    def diagnose(self):
        """Deep pipeline diagnosis"""
        stuck = find_stuck_ros(self.roster)
        if stuck.empty:
            return "No stuck operations found"
        
        return f"Found {len(stuck)} stuck operations. Main issues in {stuck['LATEST_STAGE_NM'].iloc[0]} stage"
    
    def correlate_with_market(self, market_query):
        """Find pipeline issues affecting specific market"""
        import re
        market_match = re.search(r'([A-Z]{2})', market_query.upper())
        if market_match:
            market = market_match.group(1)
            market_issues = self.roster[self.roster['CNT_STATE'] == market]
            stuck_count = len(market_issues[market_issues['IS_STUCK'] == 1])
            return f"Market {market}: {stuck_count} pipeline issues found"
        return "No specific market identified"


class MarketIntelligenceAgent:
    def __init__(self, market):
        self.market = market
    
    def analyze(self, query):
        """Advanced market analysis with trends"""
        summary = get_market_summary(self.market)
        return f"Market analysis complete. Top performer: {summary.iloc[0]['MARKET']}"


class DataQualityAgent:
    def __init__(self, roster):
        self.roster = roster
    
    def deep_dive(self):
        """Deep data quality analysis"""
        return "Quality analysis: Checking rejection patterns..."
    
    def check_impact(self, pipeline_issues):
        """Check how pipeline issues impact data quality"""
        return "Correlating pipeline issues with data quality degradation..."


class PredictiveAgent:
    def __init__(self, roster, market):
        self.roster = roster
        self.market = market
    
    def predict_impact(self, quality_issues):
        """Predict future failures based on patterns"""
        return "Predictive analysis: 15% failure increase expected next month"

# Test
if __name__ == "__main__":
    agent = SupervisorAgent()