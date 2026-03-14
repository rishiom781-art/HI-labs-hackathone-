import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Load data FIRST
from data_loader import load_data, find_stuck_ros, find_critical_markets, get_market_summary, analyze_stage_bottlenecks, analyze_organization_health, generate_full_report
roster, market = load_data()

# Import advanced components
try:
    from multi_agent import SupervisorAgent
    from root_cause import RootCauseAnalyzer
    from monitor import RealtimeMonitor
    from pattern_clustering import FailurePatternCluster
    
    # Initialize with loaded data
    supervisor = SupervisorAgent()
    root_cause = RootCauseAnalyzer(roster, market)
    monitor = RealtimeMonitor(check_interval=300)
    cluster = FailurePatternCluster(roster)
    advanced_available = True
except Exception as e:
    advanced_available = False
    st.warning(f"Some advanced features not available: {e}")

# Page config
st.set_page_config(
    page_title="RosterIQ - Healthcare Pipeline Intelligence",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 10px;
    }
    .critical {
        color: red;
        font-weight: bold;
    }
    .warning {
        color: orange;
        font-weight: bold;
    }
    .good {
        color: green;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar
st.sidebar.image("https://img.icons8.com/fluency/96/robot.png", width=80)
st.sidebar.title("🚀 RosterIQ")
st.sidebar.markdown("---")

# Navigation
if advanced_available:
    page = st.sidebar.radio(
        "Navigation",
        ["📊 Dashboard", "🔍 Pipeline Analysis", "📈 Market Intelligence", 
         "🤖 Multi-Agent", "🔗 Root Cause", "🧬 Pattern Clustering", "🚨 Alerts", "📋 Full Report"]
    )
else:
    page = st.sidebar.radio(
        "Navigation",
        ["📊 Dashboard", "🔍 Pipeline Analysis", "📈 Market Intelligence", "📋 Full Report"]
    )

st.sidebar.markdown("---")
st.sidebar.info(
    """
    **Pipeline Stats:**
    - Total Ops: {:,}
    - Stuck: {}
    - Failed: {:,}
    """.format(
        len(roster),
        len(roster[roster["IS_STUCK"] == 1]),
        len(roster[roster["IS_FAILED"] == 1])
    )
)

# ==================== DASHBOARD PAGE ====================
if page == "📊 Dashboard":
    st.title("📊 Pipeline Health Dashboard")
    
    # Key Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_ops = len(roster)
        st.metric("Total Operations", f"{total_ops:,}")
    
    with col2:
        stuck_ops = len(roster[roster["IS_STUCK"] == 1])
        delta = f"{stuck_ops/total_ops*100:.1f}%" if total_ops > 0 else "0%"
        st.metric("Stuck Operations", stuck_ops, delta=delta)
    
    with col3:
        failed_ops = len(roster[roster["IS_FAILED"] == 1])
        delta = f"{failed_ops/total_ops*100:.1f}%" if total_ops > 0 else "0%"
        st.metric("Failed Operations", f"{failed_ops:,}", delta=delta)
    
    with col4:
        markets = market['MARKET'].nunique()
        critical = len(find_critical_markets(market)['MARKET'].unique()) if not find_critical_markets(market).empty else 0
        st.metric("Critical Markets", critical, delta=f"out of {markets}")
    
    st.markdown("---")
    
    # Two columns for charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Operations by Pipeline Stage")
        stage_counts = roster["LATEST_STAGE_NM"].value_counts().reset_index()
        stage_counts.columns = ["Stage", "Count"]
        
        fig = px.pie(stage_counts, values="Count", names="Stage", 
                     title="Distribution Across Pipeline Stages",
                     color_discrete_sequence=px.colors.sequential.RdBu)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📈 Market Success Rates")
        market_avg = market.groupby("MARKET")["SCS_PERCENT"].mean().reset_index()
        market_avg = market_avg.sort_values("SCS_PERCENT", ascending=False).head(15)
        
        fig = px.bar(market_avg, x="MARKET", y="SCS_PERCENT",
                     title="Top 15 Markets by Success Rate",
                     color="SCS_PERCENT",
                     color_continuous_scale="RdYlGn",
                     range_color=[0, 100])
        fig.add_hline(y=85, line_dash="dash", line_color="red", 
                     annotation_text="Target (85%)")
        st.plotly_chart(fig, use_container_width=True)
    
    # Stuck Operations Table
    st.markdown("---")
    st.subheader("🔍 Current Stuck Operations")
    
    stuck_df = find_stuck_ros(roster)
    if not stuck_df.empty:
        # Color code based on failed status
        def color_status(val):
            return 'background-color: #ffcccc' if val == 1 else 'background-color: #fff3cd'
        
        styled_df = stuck_df.style.applymap(color_status, subset=['IS_FAILED'])
        st.dataframe(styled_df, use_container_width=True)
    else:
        st.success("✅ No stuck operations found!")

# ==================== PIPELINE ANALYSIS PAGE ====================
elif page == "🔍 Pipeline Analysis":
    st.title("🔍 Detailed Pipeline Analysis")
    
    # Stage bottlenecks
    st.subheader("🔧 Pipeline Stage Bottlenecks")
    bottlenecks = analyze_stage_bottlenecks(roster)
    
    if not bottlenecks.empty:
        # Create a nice visualization
        fig = go.Figure(data=[
            go.Bar(name='Stuck Count', x=bottlenecks.index, y=bottlenecks['STUCK_COUNT'],
                   marker_color='orange'),
            go.Bar(name='Failed Count', x=bottlenecks.index, y=bottlenecks['FAILED_COUNT'],
                   marker_color='red')
        ])
        fig.update_layout(barmode='group', title="Stuck vs Failed by Stage")
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(bottlenecks, use_container_width=True)
    else:
        st.info("No bottlenecks detected")
    
    # Organization health
    st.markdown("---")
    st.subheader("🏥 Organization Health")
    
    org_health = analyze_organization_health(roster)
    if not org_health.empty:
        # Top 10 problematic organizations
        fig = px.bar(org_health.head(10), 
                     x=org_health.head(10).index, 
                     y='TOTAL_ISSUES',
                     title="Top 10 Organizations with Most Issues",
                     color='TOTAL_ISSUES',
                     color_continuous_scale='Reds')
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(org_health.head(20), use_container_width=True)
    else:
        st.info("No organization issues found")

# ==================== MARKET INTELLIGENCE PAGE ====================
elif page == "📈 Market Intelligence":
    st.title("📈 Market Intelligence Dashboard")
    
    # Market summary
    market_summary = get_market_summary(market)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏆 Top Performing Markets")
        st.dataframe(market_summary.head(10), use_container_width=True)
    
    with col2:
        st.subheader("⚠️ Bottom Performing Markets")
        st.dataframe(market_summary.tail(10), use_container_width=True)
    
    # Critical markets
    st.markdown("---")
    st.subheader("🚨 Critical Markets (Below 50% Success)")
    
    critical = find_critical_markets(market)
    if not critical.empty:
        fig = px.scatter(critical, x="MONTH", y="SCS_PERCENT", 
                        color="MARKET", size="OVERALL_SCS_CNT",
                        title="Critical Markets Over Time",
                        hover_data=['PRIORITY'])
        st.plotly_chart(fig, use_container_width=True)
        
        # Show as table with priority coloring
        def color_priority(val):
            if val == 'EMERGENCY':
                return 'background-color: #ff0000; color: white'
            elif val == 'CRITICAL':
                return 'background-color: #ff6b6b'
            else:
                return 'background-color: #ffd93d'
        
        styled_critical = critical.style.applymap(color_priority, subset=['PRIORITY'])
        st.dataframe(styled_critical, use_container_width=True)
    else:
        st.success("No critical markets found!")
    
    # Market selector for detailed view
    st.markdown("---")
    st.subheader("📊 Individual Market Analysis")
    
    selected_market = st.selectbox("Select Market", market['MARKET'].unique())
    
    market_data = market[market['MARKET'] == selected_market]
    avg_rate = market_data['SCS_PERCENT'].mean()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Average Success Rate", f"{avg_rate:.1f}%")
    with col2:
        st.metric("Total Records", len(market_data))
    with col3:
        trend = "📈 Improving" if len(market_data) > 1 and market_data.iloc[-1]['SCS_PERCENT'] > market_data.iloc[0]['SCS_PERCENT'] else "📉 Declining"
        st.metric("Trend", trend)
    
    # Trend chart
    fig = px.line(market_data, x="MONTH", y="SCS_PERCENT",
                  title=f"{selected_market} Success Rate Trend",
                  markers=True)
    fig.add_hline(y=85, line_dash="dash", line_color="green", annotation_text="Target")
    fig.add_hline(y=50, line_dash="dash", line_color="red", annotation_text="Critical")
    st.plotly_chart(fig, use_container_width=True)

# ==================== MULTI-AGENT PAGE ====================
elif page == "🤖 Multi-Agent" and advanced_available:
    st.title("🤖 Multi-Agent Intelligence System")
    
    st.markdown("""
    This page uses **4 specialized AI agents** working together:
    - **Pipeline Agent**: Monitors pipeline health
    - **Market Agent**: Analyzes market trends
    - **Quality Agent**: Checks data quality
    - **Predictive Agent**: Forecasts future issues
    """)
    
    query = st.text_input("Ask the multi-agent system:", 
                         placeholder="e.g., Why is market failing? or Show pipeline issues")
    
    if st.button("Investigate") and query:
        with st.spinner("Agents are analyzing..."):
            result = supervisor.investigate(query)
            st.markdown(result)
    
    # Show agent status
    st.markdown("---")
    st.subheader("🤖 Agent Status")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.success("🟢 Pipeline Agent\nActive")
    with col2:
        st.success("🟢 Market Agent\nActive")
    with col3:
        st.success("🟢 Quality Agent\nActive")
    with col4:
        st.success("🟢 Predictive Agent\nActive")

# ==================== ROOT CAUSE PAGE ====================
elif page == "🔗 Root Cause" and advanced_available:
    st.title("🔗 Root Cause Chain Analysis")
    
    st.markdown("""
    Trace failures from **Market Level → Organization → Source System → Pipeline Stage → Root Cause**
    """)
    
    market_to_trace = st.selectbox("Select market to analyze:", market['MARKET'].unique())
    
    if st.button("Trace Failure Chain"):
        with st.spinner("Tracing failure chain..."):
            chain = root_cause.trace_failure(market_to_trace)
            st.code(chain, language="text")
    
    # Visual causal chain
    st.markdown("---")
    st.subheader("📊 Causal Chain Visualization")
    
    fig = go.Figure(data=[go.Scatter(
        x=[1, 2, 3, 4, 5],
        y=[5, 4, 3, 2, 1],
        mode='lines+markers+text',
        text=['Market Level', 'Organization', 'Source System', 'Pipeline Stage', 'Root Cause'],
        textposition="top center",
        marker=dict(size=[20, 20, 20, 20, 25], color=['red', 'orange', 'yellow', 'lightgreen', 'darkred']),
        line=dict(color="gray", width=2)
    )])
    
    fig.update_layout(
        title="Failure Causal Chain",
        xaxis=dict(showticklabels=False, showgrid=False),
        yaxis=dict(showticklabels=False, showgrid=False),
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)

# ==================== PATTERN CLUSTERING PAGE ====================
elif page == "🧬 Pattern Clustering" and advanced_available:
    st.title("🧬 Failure Pattern Clustering")
    
    st.markdown("""
    Using **Machine Learning** to group similar failure patterns across organizations.
    """)
    
    if st.button("Run Pattern Analysis"):
        with st.spinner("Clustering organizations by failure patterns..."):
            patterns = cluster.cluster_patterns(n_clusters=4)
            
            if isinstance(patterns, str):
                st.warning(patterns)
            else:
                # Visualize clusters
                cluster_sizes = [data['size'] for data in patterns.values()]
                cluster_names = list(patterns.keys())
                
                fig = px.pie(values=cluster_sizes, names=cluster_names,
                            title="Organization Distribution by Failure Pattern",
                            color_discrete_sequence=px.colors.qualitative.Set3)
                st.plotly_chart(fig, use_container_width=True)
                
                # Show details
                for cluster_name, data in patterns.items():
                    with st.expander(f"📊 {cluster_name} ({data['size']} organizations)"):
                        st.write("**Sample organizations:**")
                        for org in data['organizations']:
                            st.write(f"  • {org}")
                
                # Recommendations
                st.markdown("---")
                st.subheader("💡 Recommended Actions")
                for rec in cluster.recommend_actions(patterns):
                    st.info(rec)

# ==================== ALERTS PAGE ====================
elif page == "🚨 Alerts" and advanced_available:
    st.title("🚨 Realtime Pipeline Alerts")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.subheader("Active Alerts")
        
        alerts = monitor.get_alerts()
        if alerts:
            for alert in alerts[-10:]:  # Last 10 alerts
                if alert['severity'] == 'HIGH':
                    st.error(f"🚨 **{alert['timestamp']}** - {alert['type']}: {alert['message']}")
                else:
                    st.warning(f"⚠️ **{alert['timestamp']}** - {alert['type']}: {alert['message']}")
        else:
            st.success("✅ No active alerts")
    
    with col2:
        st.subheader("Controls")
        if st.button("🔄 Check Now"):
            monitor._check_pipeline()
            st.rerun()
        
        st.metric("Alert Count", len(monitor.alerts))
    
    # Alert history chart
    if monitor.alerts:
        st.markdown("---")
        st.subheader("Alert History")
        
        alert_df = pd.DataFrame(monitor.alerts)
        alert_counts = alert_df['type'].value_counts()
        
        fig = px.bar(x=alert_counts.index, y=alert_counts.values,
                    title="Alerts by Type",
                    color=alert_counts.index,
                    color_discrete_sequence=px.colors.qualitative.Set1)
        st.plotly_chart(fig, use_container_width=True)

# ==================== FULL REPORT PAGE ====================
elif page == "📋 Full Report":
    st.title("📋 Complete Pipeline Health Report")
    
    if st.button("🔄 Generate Fresh Report"):
        with st.spinner("Generating comprehensive report..."):
            report = generate_full_report(roster, market)
            st.text(report)
            
            # Export option
            csv = report.replace('\n', ',')
            st.download_button(
                label="📥 Download Report",
                data=report,
                file_name="pipeline_report.txt",
                mime="text/plain"
            )

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center'>
        <p>🚀 <b>RosterIQ</b> - Autonomous Healthcare Pipeline Intelligence Agent</p>
        <p style='color: gray; font-size: 0.8em;'>Memory Systems: Episodic ✓ | Procedural ✓ | Semantic ✓</p>
    </div>
    """,
    unsafe_allow_html=True
)