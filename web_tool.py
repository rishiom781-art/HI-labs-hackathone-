import requests
import json
import os

class WebSearchTool:
    def __init__(self, api_key=None):
        """Initialize web search tool"""
        self.api_key = api_key or os.getenv("TAVILY_API_KEY", "")
        
        # Mock data for when API not available
        self.mock_data = {
            "cms": {
                "title": "CMS Provider Directory Rules 2026",
                "content": "CMS updated provider directory requirements. Monthly verification now required. Non-compliance can result in 40% rejection rates."
            },
            "medicaid": {
                "title": "Medicaid FFS Provider Data Requirements",
                "content": "Medicaid requires 95% accuracy in provider data. Common rejections: NPI mismatches, expired licenses, invalid tax IDs."
            },
            "dart": {
                "title": "DART Generation Troubleshooting",
                "content": "Most DART failures come from incomplete provider records. Check source system data quality before generation."
            },
            "validation": {
                "title": "Provider Validation Error Guide",
                "content": "Top validation failures: 1) Missing tax ID (34%), 2) Invalid NPI (28%), 3) Expired license (22%), 4) Address mismatch (16%)"
            },
            "rejection": {
                "title": "Understanding Provider Rejections",
                "content": "Rejections indicate compliance issues, not processing errors. Usually requires source data correction, not just retry."
            },
            "availity": {
                "title": "AvailityPDM System Guide",
                "content": "AvailityPDM is a provider data management system. Common issues: data format mismatches, missing required fields."
            },
            "npi": {
                "title": "NPI Validation Requirements",
                "content": "NPIs must be active in NPPES database. Expired or invalid NPIs cause automatic rejection."
            }
        }
        
        if self.api_key:
            print("✅ Web search configured with API key")
        else:
            print("⚠️  Using mock web search (no API key)")
    
    def search(self, query, max_results=3):
        """Search the web for information"""
        print(f"\n🌐 Searching web for: '{query}'")
        
        # If API key exists, use real search
        if self.api_key:
            return self._real_search(query, max_results)
        
        # Otherwise use mock search
        return self._mock_search(query, max_results)
    
    def _real_search(self, query, max_results):
        """Real search using Tavily API"""
        try:
            url = "https://api.tavily.com/search"
            payload = {
                "api_key": self.api_key,
                "query": query,
                "search_depth": "basic",
                "max_results": max_results
            }
            
            response = requests.post(url, json=payload)
            
            if response.status_code == 200:
                data = response.json()
                return data.get("results", [])
            else:
                print(f"⚠️ Search failed: {response.status_code}")
                return self._mock_search(query, max_results)
                
        except Exception as e:
            print(f"⚠️ Search error: {e}")
            return self._mock_search(query, max_results)
    
    def _mock_search(self, query, max_results):
        """Mock search for demonstration"""
        results = []
        query_lower = query.lower()
        
        # Find relevant mock results
        for key, value in self.mock_data.items():
            if key in query_lower:
                results.append({
                    "title": value["title"],
                    "content": value["content"],
                    "url": f"https://example.com/{key}"
                })
        
        # Add general result if none found
        if not results:
            results.append({
                "title": f"Information about {query}",
                "content": f"Based on healthcare data standards, {query} relates to provider roster management. Check CMS guidelines for specific requirements.",
                "url": "https://cms.gov"
            })
        
        # Add second result if needed
        if len(results) < max_results and "cms" not in query_lower:
            results.append({
                "title": "CMS Provider Data Guidelines",
                "content": "CMS requires accurate provider directories. Monthly verification and compliance checks are mandatory.",
                "url": "https://cms.gov/provider-directory"
            })
        
        return results[:max_results]
    
    def search_regulations(self, market=None):
        """Search for regulatory changes"""
        query = "CMS provider regulation updates 2026"
        if market:
            query = f"{market} Medicaid provider requirements 2026"
        return self.search(query)
    
    def explain_failure(self, failure_type):
        """Get explanation for a failure type"""
        queries = {
            "rejection": "provider data rejection causes",
            "validation": "provider validation failure reasons",
            "dart": "DART generation failure troubleshooting",
            "stuck": "pipeline stuck operation resolution"
        }
        
        query = queries.get(failure_type.lower(), f"{failure_type} in healthcare data processing")
        return self.search(query)

# Test the web search
if __name__ == "__main__":
    print("\n" + "="*60)
    print("🌐 TESTING WEB SEARCH TOOL")
    print("="*60)
    
    web = WebSearchTool()
    
    # Test searches
    test_queries = [
        "CMS provider rules",
        "validation failure reasons",
        "DART generation issues"
    ]
    
    for query in test_queries:
        print(f"\n🔍 Testing: {query}")
        results = web.search(query)
        for i, result in enumerate(results, 1):
            print(f"\n  Result {i}:")
            print(f"  📰 {result['title']}")
            print(f"  📝 {result['content'][:150]}...")
    
    print("\n✅ Web search tool ready!")