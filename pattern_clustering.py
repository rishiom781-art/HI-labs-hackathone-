import numpy as np
try:
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    print("⚠️ scikit-learn not installed. Run: pip install scikit-learn")

class FailurePatternCluster:
    """Cluster similar failure patterns"""
    
    def __init__(self, roster):
        self.roster = roster
        self.patterns = None
    
    def extract_features(self):
        """Extract features for clustering"""
        features = []
        orgs = []
        
        for org in self.roster['ORG_NM'].unique()[:30]:  # Limit for performance
            org_data = self.roster[self.roster['ORG_NM'] == org]
            
            total_ops = len(org_data)
            if total_ops == 0:
                continue
                
            stuck_rate = len(org_data[org_data['IS_STUCK'] == 1]) / total_ops
            fail_rate = len(org_data[org_data['IS_FAILED'] == 1]) / total_ops
            
            features.append([stuck_rate, fail_rate])
            orgs.append(org[:30])
        
        return np.array(features), orgs
    
    def cluster_patterns(self, n_clusters=3):
        """Cluster organizations by failure patterns"""
        if not SKLEARN_AVAILABLE:
            return "Install scikit-learn first: pip install scikit-learn"
        
        X, orgs = self.extract_features()
        
        if len(X) < n_clusters:
            return {
                "Cluster 1 - Mixed": {
                    'organizations': orgs[:5],
                    'size': len(orgs)
                }
            }
        
        # Normalize features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Cluster
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        clusters = kmeans.fit_predict(X_scaled)
        
        # Analyze clusters
        results = {}
        for i in range(n_clusters):
            cluster_orgs = [orgs[j] for j in range(len(orgs)) if clusters[j] == i]
            
            # Simple pattern naming
            pattern = f"Pattern {i+1}"
            results[pattern] = {
                'organizations': cluster_orgs[:5],
                'size': len(cluster_orgs)
            }
        
        return results
    
    def recommend_actions(self, cluster_results):
        """Recommend actions based on cluster patterns"""
        recommendations = []
        
        for cluster_name, data in cluster_results.items():
            if data['size'] > 5:
                recommendations.append(f"🔄 {cluster_name}: {data['size']} organizations - Focus on pipeline optimization")
            else:
                recommendations.append(f"🔧 {cluster_name}: Small cluster - Individual investigation needed")
        
        return recommendations

# Test
if __name__ == "__main__":
    from data_loader import load_data
    r, m = load_data()
    fc = FailurePatternCluster(r)
    patterns = fc.cluster_patterns()
    print(patterns)