import json
import os
from datetime import datetime

class SimpleMemory:
    def __init__(self):
        """Initialize memory system"""
        # Create memory folder
        if not os.path.exists("memory_store"):
            os.makedirs("memory_store")
            print("📁 Created memory_store folder")
        
        # Episodic memory (conversations)
        self.episodic_file = "memory_store/conversations.json"
        self.load_conversations()
        
        # Procedural memory (procedures)
        self.procedures_file = "memory_store/procedures.json"
        self.load_procedures()
        
        # Semantic memory (knowledge)
        self.knowledge_file = "memory_store/knowledge.json"
        self.load_knowledge()
    
    def load_conversations(self):
        """Load past conversations"""
        if os.path.exists(self.episodic_file):
            with open(self.episodic_file, 'r') as f:
                self.conversations = json.load(f)
        else:
            self.conversations = []
            self.save_conversations()
        print(f"📚 Loaded {len(self.conversations)} past conversations")
    
    def save_conversations(self):
        """Save conversations to file"""
        with open(self.episodic_file, 'w') as f:
            json.dump(self.conversations, f, indent=2)
    
    def load_procedures(self):
        """Load diagnostic procedures"""
        if os.path.exists(self.procedures_file):
            with open(self.procedures_file, 'r') as f:
                self.procedures = json.load(f)
        else:
            # Default procedures
            self.procedures = {
                "triage_stuck_ros": {
                    "name": "Triage Stuck ROs",
                    "description": "Find all stuck operations",
                    "logic": "Filter IS_STUCK=1 and return details"
                },
                "market_health_report": {
                    "name": "Market Health Report",
                    "description": "Find markets below 85%",
                    "logic": "Filter SCS_PERCENT < 85 and analyze"
                },
                "record_quality_audit": {
                    "name": "Record Quality Audit",
                    "description": "Find organizations with high rejections",
                    "logic": "Calculate rejection rates and find worst performers"
                },
                "retry_effectiveness": {
                    "name": "Retry Effectiveness",
                    "description": "Analyze if retries improve success",
                    "logic": "Compare FIRST_ITER vs NEXT_ITER success"
                }
            }
            self.save_procedures()
        print(f"📋 Loaded {len(self.procedures)} diagnostic procedures")
    
    def save_procedures(self):
        """Save procedures to file"""
        with open(self.procedures_file, 'w') as f:
            json.dump(self.procedures, f, indent=2)
    
    def load_knowledge(self):
        """Load domain knowledge"""
        if os.path.exists(self.knowledge_file):
            with open(self.knowledge_file, 'r') as f:
                self.knowledge = json.load(f)
        else:
            # Default knowledge from problem statement
            self.knowledge = {
                # Pipeline Stages
                "PRE_PROCESSING": "Initial stage where roster files are validated and prepared",
                "MAPPING_APPROVAL": "Stage where data mapping is reviewed and approved",
                "ISF_GENERATION": "Intermediate file generation stage",
                "DART_GENERATION": "Critical stage where provider data is converted to DART format",
                "DART_REVIEW": "Review stage for DART files",
                "DART_UI_VALIDATION": "User interface validation of DART files",
                "SPS_LOAD": "Final stage where data is loaded into production system",
                
                # Record Types
                "FAIL_REC_CNT": "Records that failed during processing due to technical errors",
                "REJ_REC_CNT": "Records rejected due to compliance or validation failures - indicates data quality issues",
                "SKIP_REC_CNT": "Records skipped due to business rules or duplicates",
                "SCS_REC_CNT": "Records successfully processed",
                
                # Health Flags
                "GREEN": "Stage performing normally",
                "YELLOW": "Stage showing signs of degradation",
                "RED": "Stage critical - immediate attention needed",
                
                # Source Systems
                "AvailityPDM": "Provider data management system",
                "Demographic": "Demographic data source",
                "ProviderGroup": "Provider group management system",
                
                # File Status
                "FILE_STATUS_CD 9": "Stopped - Pipeline halted",
                "FILE_STATUS_CD 45": "DART Review - Awaiting review",
                "FILE_STATUS_CD 49": "DART Generation - File being generated",
                "FILE_STATUS_CD 99": "Resolved - Successfully completed",
                
                # Business Concepts
                "IS_STUCK": "Operation cannot progress to next stage - needs investigation",
                "IS_FAILED": "Operation has completely failed",
                "RUN_NO": "Processing iteration number (1=first attempt, >1=retry)",
                "SCS_PERCENT": "Overall transaction success rate for a market",
                "retry_effectiveness": "Higher retry success indicates temporary issues, lower indicates systemic problems"
            }
            self.save_knowledge()
        print(f"📚 Loaded {len(self.knowledge)} knowledge items")
    
    def save_knowledge(self):
        """Save knowledge to file"""
        with open(self.knowledge_file, 'w') as f:
            json.dump(self.knowledge, f, indent=2)
    
    def save_conversation(self, question, answer):
        """Save a conversation to episodic memory"""
        self.conversations.append({
            "question": question,
            "answer": answer[:500] + "..." if len(answer) > 500 else answer,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        # Keep only last 100 conversations
        if len(self.conversations) > 100:
            self.conversations = self.conversations[-100:]
        
        self.save_conversations()
    
    def remember(self, question):
        """Check if we've seen similar question before"""
        question_lower = question.lower()
        
        # Check last 10 conversations
        for conv in reversed(self.conversations[-10:]):
            if question_lower in conv["question"].lower() or conv["question"].lower() in question_lower:
                return conv["answer"]
        
        return None
    
    def get_procedure(self, name):
        """Get a procedure by name"""
        return self.procedures.get(name)
    
    def update_procedure(self, name, new_description):
        """Update a procedure"""
        if name in self.procedures:
            self.procedures[name]["description"] = new_description
            self.procedures[name]["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.save_procedures()
            return True
        return False
    
    def explain(self, term):
        """Get explanation for a term"""
        term_upper = term.upper()
        
        # Direct match
        if term_upper in self.knowledge:
            return self.knowledge[term_upper]
        
        # Partial match
        for key, value in self.knowledge.items():
            if term_upper in key.upper() or key.upper() in term_upper:
                return f"{key}: {value}"
        
        return f"I don't have information about '{term}'"
    
    def get_market_history(self, market):
        """Get conversation history for a specific market"""
        market_history = []
        for conv in self.conversations:
            if market.upper() in conv["question"].upper():
                market_history.append(conv)
        return market_history
    
    def get_stats(self):
        """Get memory statistics"""
        return {
            "conversations": len(self.conversations),
            "procedures": len(self.procedures),
            "knowledge_items": len(self.knowledge),
            "memory_types": ["Episodic", "Procedural", "Semantic"]
        }

# Test the memory system
if __name__ == "__main__":
    print("\n" + "="*60)
    print("🧠 TESTING MEMORY SYSTEM")
    print("="*60)
    
    mem = SimpleMemory()
    
    # Test saving
    print("\n📝 Testing save...")
    mem.save_conversation("test question", "test answer")
    print("✅ Saved successfully")
    
    # Test knowledge
    print("\n📚 Testing knowledge...")
    print(f"Explain DART_GENERATION: {mem.explain('DART_GENERATION')}")
    print(f"Explain REJ_REC_CNT: {mem.explain('REJ_REC_CNT')}")
    
    # Show stats
    print("\n📊 Memory Stats:")
    stats = mem.get_stats()
    for key, value in stats.items():
        print(f"  • {key}: {value}")
    
    print("\n✅ Memory system ready!")