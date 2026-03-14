from data_loader import *
from memory import SimpleMemory
import time

class RosterIQAgent:
    def __init__(self):
        print("\n" + "="*60)
        print("🚀 INITIALIZING ROSTERIQ AI AGENT")
        print("="*60)
        
        # Load data
        print("\n📂 Loading datasets...")
        self.roster, self.market = load_data()
        print(f"✅ Loaded {len(self.roster):,} roster operations")
        print(f"✅ Loaded {len(self.market):,} market records")
        
        # Initialize memory
        print("\n🧠 Initializing memory system...")
        self.memory = SimpleMemory()
        print("✅ Memory system ready")
        
        # Store initial insights in memory
        self._store_initial_insights()
        
        print("\n" + "="*60)
        print("🤖 ROSTERIQ AGENT READY - ASK ME ANYTHING!")
        print("="*60)
    
    def _store_initial_insights(self):
        """Store initial analysis in memory"""
        # Store stuck operations
        stuck = find_stuck_ros(self.roster)
        if not stuck.empty:
            self.memory.save_conversation(
                "initial_analysis_stuck",
                f"Found {len(stuck)} stuck operations: {', '.join(stuck['RO_ID'].tolist())}"
            )
        
        # Store critical markets
        critical = find_critical_markets(self.market)
        if not critical.empty:
            markets = critical['MARKET'].unique()
            self.memory.save_conversation(
                "initial_analysis_critical_markets",
                f"Found {len(markets)} critical markets: {', '.join(markets[:5])}"
            )
    
    def ask(self, question):
        """Process a question and return answer"""
        print(f"\n❓ You: {question}")
        print("-" * 60)
        
        # Check memory for similar questions
        remembered = self.memory.remember(question)
        if remembered:
            print(f"📝 I remember discussing this:")
            print(f"   Previously: {remembered[:200]}...")
        
        # Get answer based on question type
        answer = self._get_answer(question)
        
        # Save to memory
        self.memory.save_conversation(question, answer)
        
        return answer
    
    def _get_answer(self, question):
        """Route question to appropriate handler"""
        q = question.lower()
        
        # STUCK OPERATIONS
        if any(word in q for word in ['stuck', 'blocked', 'hung', 'pending']):
            stuck = find_stuck_ros(self.roster)
            if stuck.empty:
                return "✅ No stuck operations found in the pipeline!"
            
            response = f"🔍 Found {len(stuck)} stuck operations:\n\n"
            for _, row in stuck.iterrows():
                status = "❌ FAILED" if row['IS_FAILED'] == 1 else "⚠️ STUCK"
                response += f"• {row['RO_ID']} - {row['ORG_NM']} ({row['CNT_STATE']})\n"
                response += f"  Stage: {row['LATEST_STAGE_NM']} | Status: {status}\n\n"
            
            # Add bottleneck analysis
            bottlenecks = analyze_stage_bottlenecks(self.roster)
            if not bottlenecks.empty:
                response += f"\n🔧 Main bottleneck: {bottlenecks.index[0]} stage"
            
            return response
        
        # MARKET QUESTIONS
        elif any(word in q for word in ['market', 'state', 'region', 'performance']):
            # Check for specific market
            markets = self.market['MARKET'].unique()
            mentioned = None
            for m in markets:
                if m.lower() in q:
                    mentioned = m
                    break
            
            if mentioned:
                # Specific market analysis
                market_data = self.market[self.market['MARKET'] == mentioned]
                avg_rate = market_data['SCS_PERCENT'].mean()
                
                response = f"📊 **{mentioned} MARKET ANALYSIS**\n\n"
                response += f"Average Success Rate: {avg_rate:.1f}%\n"
                response += f"Total Records: {len(market_data)}\n\n"
                
                if avg_rate < 85:
                    response += f"⚠️ This market is BELOW target (85%)\n"
                    
                    # Check if critical
                    if avg_rate < 50:
                        response += f"🚨 CRITICAL - Needs immediate attention!\n"
                    
                    # Find worst months
                    worst = market_data.nsmallest(3, 'SCS_PERCENT')
                    response += f"\nWorst months:\n"
                    for _, row in worst.iterrows():
                        response += f"  • {row['MONTH']}: {row['SCS_PERCENT']}%\n"
                
                return response
            else:
                # General market summary
                summary = get_market_summary(self.market)
                response = "📈 **MARKET PERFORMANCE SUMMARY**\n\n"
                response += "TOP 5 MARKETS:\n"
                for _, row in summary.head(5).iterrows():
                    response += f"  • {row['MARKET']}: {row['AVG_SUCCESS']:.1f}%\n"
                
                response += "\nBOTTOM 5 MARKETS:\n"
                for _, row in summary.tail(5).iterrows():
                    response += f"  • {row['MARKET']}: {row['AVG_SUCCESS']:.1f}%\n"
                
                # Add critical count
                critical = find_critical_markets(self.market)
                if not critical.empty:
                    response += f"\n⚠️ {len(critical['MARKET'].unique())} markets are CRITICAL (<50%)"
                
                return response
        
        # ORGANIZATION QUESTIONS
        elif any(word in q for word in ['organization', 'org', 'provider', 'hospital']):
            org_issues = analyze_organization_health(self.roster)
            
            if org_issues.empty:
                return "✅ No organizations with major issues found."
            
            response = "🏢 **ORGANIZATIONS WITH MOST ISSUES**\n\n"
            for org, row in org_issues.head(10).iterrows():
                response += f"• {org[:40]}...\n"
                response += f"  Issues: {row['TOTAL_ISSUES']} (Stuck: {row['STUCK_COUNT']}, Failed: {row['FAILED_COUNT']})\n"
            
            return response
        
        # PIPELINE STAGE QUESTIONS
        elif any(word in q for word in ['stage', 'pipeline', 'bottleneck', 'phase']):
            bottlenecks = analyze_stage_bottlenecks(self.roster)
            
            if bottlenecks.empty:
                return "✅ No pipeline bottlenecks detected."
            
            response = "🔧 **PIPELINE STAGE ANALYSIS**\n\n"
            for stage, row in bottlenecks.iterrows():
                response += f"• {stage}:\n"
                response += f"  Stuck: {row['STUCK_COUNT']}, Failed: {row['FAILED_COUNT']}, Failure Rate: {row['FAILURE_RATE']}%\n"
            
            # Add semantic explanation
            response += "\n📚 **Stage Descriptions:**\n"
            stage_descriptions = {
                'PRE_PROCESSING': 'Initial validation and preparation',
                'DART_REVIEW': 'Review of generated DART files',
                'DART_GENERATION': 'Converting provider data to DART format',
                'SPS_LOAD': 'Final loading into production'
            }
            for stage in bottlenecks.index[:3]:
                if stage in stage_descriptions:
                    response += f"  • {stage}: {stage_descriptions[stage]}\n"
            
            return response
        
        # RETRY ANALYSIS
        elif any(word in q for word in ['retry', 'retries', 'reprocess']):
            retry = analyze_retry_effectiveness(self.market)
            
            if not retry:
                return "Retry data not available in the dataset."
            
            response = "🔄 **RETRY EFFECTIVENESS ANALYSIS**\n\n"
            response += f"First Pass Success: {retry['first_pass_success']:,}\n"
            response += f"Retry Success: {retry['retry_success']:,}\n"
            response += f"Total Failures: {retry['total_failures']:,}\n"
            response += f"Improvement: {retry['improvement_percent']}%\n"
            response += f"Effectiveness: {retry['retry_effectiveness']}\n"
            
            if retry['improvement_percent'] > 50:
                response += "\n✅ Retries are VERY effective - good recovery mechanism!"
            elif retry['improvement_percent'] > 20:
                response += "\n👍 Retries are moderately effective"
            else:
                response += "\n⚠️ Retries show low improvement - investigate root causes"
            
            return response
        
        # DEFINITION QUESTIONS
        elif any(word in q for word in ['what is', 'define', 'meaning', 'explain']):
            # Check for terms in memory
            for term in self.memory.knowledge.keys():
                if term.lower() in q.lower():
                    return f"📚 **{term}**: {self.memory.knowledge[term]}"
            
            # Common terms
            definitions = {
                'dart': "DART (Data Aggregation and Reporting Tool) - Format for provider data",
                'rejection': "Records rejected due to compliance or validation failures",
                'stuck': "Operation that cannot progress to next pipeline stage",
                'failed': "Operation that has completely failed",
                'pre_processing': "Initial stage where files are validated and prepared",
                'sps': "Final system where provider data is loaded"
            }
            
            for term, definition in definitions.items():
                if term in q.lower():
                    return f"📚 **{term.upper()}**: {definition}"
            
            return "I don't have information about that term. Try asking about: stuck, failed, rejection, DART, PRE_PROCESSING, SPS"
        
        # CRITICAL ISSUES
        elif any(word in q for word in ['critical', 'emergency', 'urgent', 'worst']):
            critical = find_critical_markets(self.market)
            
            if critical.empty:
                return "✅ No critical markets found!"
            
            response = "🚨 **CRITICAL ISSUES REQUIRING IMMEDIATE ATTENTION**\n\n"
            
            # Markets
            response += "📉 **Critical Markets:**\n"
            for _, row in critical.iterrows():
                emoji = "🆘" if row['PRIORITY'] == 'EMERGENCY' else "⚠️"
                response += f"  {emoji} {row['MARKET']}: {row['SCS_PERCENT']}% ({row['PRIORITY']}) - {row['MONTH']}\n"
            
            # Stuck operations
            stuck = find_stuck_ros(self.roster)
            if not stuck.empty:
                response += f"\n🔧 **Stuck Operations:** {len(stuck)} need investigation\n"
            
            return response
        
        # REPORT / SUMMARY
        elif any(word in q for word in ['report', 'summary', 'overview', 'health']):
            return generate_full_report(self.roster, self.market)
        
        # HELP
        elif 'help' in q:
            return self._get_help()
        
        # DEFAULT
        else:
            return self._get_help()
    
    def _get_help(self):
        """Return help message"""
        return """🤖 **I can help you with:**

📊 **Market Questions:**
  • "How is CA market performing?"
  • "Show me critical markets"
  • "Which markets are failing?"

🔧 **Pipeline Questions:**
  • "Show stuck operations"
  • "What are the bottlenecks?"
  • "Which stages have issues?"

🏢 **Organization Questions:**
  • "Which organizations have most failures?"
  • "Show problem providers"

🔄 **Analysis Questions:**
  • "How effective are retries?"
  • "Generate health report"
  • "What are critical issues?"

📚 **Definition Questions:**
  • "What is DART_GENERATION?"
  • "Define rejection rate"

Try asking something specific!"""

# Interactive mode
if __name__ == "__main__":
    agent = RosterIQAgent()
    
    print("\n" + "="*60)
    print("💬 INTERACTIVE MODE - Type your questions (or 'exit' to quit)")
    print("="*60)
    
    while True:
        try:
            user_input = input("\n💬 You: ").strip()
            
            if user_input.lower() in ['exit', 'quit', 'bye']:
                print("\n👋 Goodbye! Thanks for using RosterIQ!")
                break
            
            if not user_input:
                continue
            
            response = agent.ask(user_input)
            print(f"\n🤖 Agent:\n{response}")
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")