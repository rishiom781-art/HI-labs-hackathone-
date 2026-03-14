from data_loader import load_data, find_critical_markets

class RootCauseAnalyzer:
    """Trace failures from market level to root cause"""
    
    def __init__(self, roster, market):
        self.roster = roster
        self.market = market
    
    def trace_failure(self, market_name):
        """Trace failure from market to root cause"""
        output = []
        output.append(f"\n🔍 TRACING FAILURE CHAIN FOR: {market_name}")
        output.append("=" * 60)
        
        # Level 1: Market
        market_data = self.market[self.market['MARKET'] == market_name]
        if market_data.empty:
            return "Market not found"
        
        avg_success = market_data['SCS_PERCENT'].mean()
        output.append(f"📍 MARKET LEVEL: {market_name}")
        output.append(f"   Success Rate: {avg_success:.1f}%")
        
        if avg_success < 50:
            output.append("   ⚠️ CRITICAL FAILURE DETECTED")
        
        # Level 2: Organizations
        orgs_in_market = self.roster[self.roster['CNT_STATE'] == market_name]['ORG_NM'].unique()
        output.append(f"\n   ↓")
        output.append(f"📍 ORGANIZATION LEVEL: {len(orgs_in_market)} organizations")
        
        # Level 3: Source Systems
        output.append(f"\n   ↓")
        output.append(f"📍 SOURCE SYSTEM LEVEL")
        
        # Level 4: Pipeline Stage
        output.append(f"\n   ↓")
        output.append(f"📍 PIPELINE STAGE LEVEL")
        
        # Level 5: Root Cause
        output.append(f"\n   ↓")
        output.append(f"📍 ROOT CAUSE")
        
        if avg_success < 50:
            output.append(f"   🔑 LIKELY ROOT CAUSE: Data quality issues from source systems")
            output.append(f"\n💡 RECOMMENDATION: Validate source data format")
        
        return "\n".join(output)
    
    def generate_causal_chain(self):
        """Generate causal chains for all failing markets"""
        critical_markets = find_critical_markets(self.market)
        
        results = {}
        for market in critical_markets['MARKET'].unique()[:3]:
            results[market] = self.trace_failure(market)
        
        return results

# Test
if __name__ == "__main__":
    from data_loader import load_data
    r, m = load_data()
    rc = RootCauseAnalyzer(r, m)
    print(rc.trace_failure('NH'))