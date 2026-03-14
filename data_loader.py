import pandas as pd

def load_data():
    """
    Load both datasets from the data folder
    """
    roster_df = pd.read_csv("data/roster_processing_details.csv")
    market_df = pd.read_csv("data/aggregated_operational_metrics.csv")
    return roster_df, market_df

def find_stuck_ros(roster_df):
    """
    Find all stuck pipeline operations (IS_STUCK = 1)
    This is the triage_stuck_ros procedure
    """
    stuck = roster_df[roster_df["IS_STUCK"] == 1]
    if stuck.empty:
        return pd.DataFrame()
    
    result = stuck[[
        "RO_ID", 
        "ORG_NM", 
        "CNT_STATE", 
        "SRC_SYS", 
        "LATEST_STAGE_NM", 
        "IS_FAILED"
    ]]
    return result

def analyze_market_health(market_df, roster_df=None):
    """
    Find markets with low success rates (<85%)
    This is the market_health_report procedure
    """
    low_markets = market_df[market_df["SCS_PERCENT"] < 85].copy()
    
    if low_markets.empty:
        return low_markets
    
    # Add severity classification
    low_markets.loc[:, "SEVERITY"] = "LOW"
    low_markets.loc[low_markets["SCS_PERCENT"] < 70, "SEVERITY"] = "MEDIUM"
    low_markets.loc[low_markets["SCS_PERCENT"] < 50, "SEVERITY"] = "HIGH"
    low_markets.loc[low_markets["SCS_PERCENT"] < 30, "SEVERITY"] = "CRITICAL"
    
    return low_markets

def get_market_summary(market_df):
    """
    Generate summary statistics for all markets
    """
    summary = market_df.groupby("MARKET").agg({
        "SCS_PERCENT": ["mean", "min", "max"],
        "OVERALL_SCS_CNT": "sum",
        "OVERALL_FAIL_CNT": "sum"
    }).round(2)
    
    # Flatten column names
    summary.columns = ['_'.join(col).strip() for col in summary.columns.values]
    summary = summary.reset_index()
    summary.columns = ['MARKET', 'AVG_SUCCESS', 'MIN_SUCCESS', 'MAX_SUCCESS', 
                      'TOTAL_SUCCESS', 'TOTAL_FAILURES']
    
    summary['TOTAL_TRANSACTIONS'] = summary['TOTAL_SUCCESS'] + summary['TOTAL_FAILURES']
    summary = summary.sort_values('AVG_SUCCESS', ascending=False)
    
    return summary

def analyze_stage_bottlenecks(roster_df):
    """
    Analyze which pipeline stages have the most stuck operations
    """
    stuck_df = roster_df[roster_df["IS_STUCK"] == 1]
    
    if stuck_df.empty:
        return pd.DataFrame()
    
    stage_analysis = stuck_df.groupby("LATEST_STAGE_NM").agg({
        "RO_ID": "count",
        "IS_FAILED": "sum"
    }).rename(columns={"RO_ID": "STUCK_COUNT", "IS_FAILED": "FAILED_COUNT"})
    
    stage_analysis = stage_analysis.sort_values("STUCK_COUNT", ascending=False)
    stage_analysis['FAILURE_RATE'] = (stage_analysis['FAILED_COUNT'] / stage_analysis['STUCK_COUNT'] * 100).round(1)
    
    return stage_analysis

def analyze_organization_health(roster_df):
    """
    Find organizations with most pipeline issues
    """
    # Get all organizations with issues
    issues_df = roster_df[(roster_df["IS_STUCK"] == 1) | (roster_df["IS_FAILED"] == 1)]
    
    if issues_df.empty:
        return pd.DataFrame()
    
    org_analysis = issues_df.groupby("ORG_NM").agg({
        "RO_ID": "count",
        "IS_STUCK": "sum",
        "IS_FAILED": "sum"
    }).rename(columns={
        "RO_ID": "TOTAL_ISSUES",
        "IS_STUCK": "STUCK_COUNT",
        "IS_FAILED": "FAILED_COUNT"
    })
    
    org_analysis = org_analysis.sort_values("TOTAL_ISSUES", ascending=False).head(20)
    org_analysis['STUCK_PERCENT'] = (org_analysis['STUCK_COUNT'] / org_analysis['TOTAL_ISSUES'] * 100).round(1)
    org_analysis['FAILED_PERCENT'] = (org_analysis['FAILED_COUNT'] / org_analysis['TOTAL_ISSUES'] * 100).round(1)
    
    return org_analysis

def analyze_rejection_patterns(roster_df):
    """
    Analyze rejection patterns across organizations and states
    Note: Uses available columns - customize based on your actual column names
    """
    # Try to find rejection-related columns
    rejection_cols = [col for col in roster_df.columns if 'REJ' in col.upper()]
    
    if not rejection_cols:
        return pd.DataFrame()
    
    # Use the first rejection column found
    rej_col = rejection_cols[0]
    
    # Group by state and organization
    rejection_patterns = roster_df.groupby(["CNT_STATE", "ORG_NM"]).agg({
        rej_col: "sum" if rej_col in roster_df.columns else "count"
    }).reset_index() if rej_col in roster_df.columns else pd.DataFrame()
    
    return rejection_patterns

def find_critical_markets(market_df):
    """
    Identify markets that need immediate attention
    """
    critical = market_df[market_df["SCS_PERCENT"] < 50].copy()
    
    if critical.empty:
        return critical
    
    # Add priority ranking
    critical.loc[:, "PRIORITY"] = "HIGH"
    critical.loc[critical["SCS_PERCENT"] < 30, "PRIORITY"] = "CRITICAL"
    critical.loc[critical["SCS_PERCENT"] < 10, "PRIORITY"] = "EMERGENCY"
    
    return critical.sort_values("SCS_PERCENT")

def analyze_retry_effectiveness(market_df):
    """
    Analyze if retries are improving success rates
    This is the retry_effectiveness_analysis procedure
    """
    if 'FIRST_ITER_SCS_CNT' not in market_df.columns or 'NEXT_ITER_SCS_CNT' not in market_df.columns:
        return None
    
    total_first = market_df['FIRST_ITER_SCS_CNT'].sum()
    total_retry = market_df['NEXT_ITER_SCS_CNT'].sum()
    total_failures = market_df['OVERALL_FAIL_CNT'].sum()
    
    if total_first == 0:
        return None
    
    improvement = ((total_retry) / total_first * 100) if total_first > 0 else 0
    
    return {
        'first_pass_success': total_first,
        'retry_success': total_retry,
        'total_failures': total_failures,
        'improvement_percent': round(improvement, 2),
        'retry_effectiveness': 'HIGH' if improvement > 20 else 'MEDIUM' if improvement > 10 else 'LOW'
    }

def generate_full_report(roster_df, market_df):
    """
    Generate a complete pipeline health report with all insights
    """
    report = []
    report.append("="*60)
    report.append("🚀 ROSTERIQ COMPLETE PIPELINE HEALTH REPORT")
    report.append("="*60)
    
    # 1. Basic Statistics
    total_ops = len(roster_df)
    stuck_ops = len(roster_df[roster_df["IS_STUCK"] == 1])
    failed_ops = len(roster_df[roster_df["IS_FAILED"] == 1])
    
    report.append(f"\n📊 OVERALL STATISTICS:")
    report.append(f"   • Total Operations: {total_ops:,}")
    report.append(f"   • Stuck Operations: {stuck_ops:,} ({(stuck_ops/total_ops*100):.1f}%)")
    report.append(f"   • Failed Operations: {failed_ops:,} ({(failed_ops/total_ops*100):.1f}%)")
    
    # 2. Stuck Operations Details
    stuck = find_stuck_ros(roster_df)
    if not stuck.empty:
        report.append(f"\n🔍 STUCK OPERATIONS ({len(stuck)} found):")
        for _, row in stuck.iterrows():
            report.append(f"   • {row['RO_ID']} - {row['ORG_NM']} ({row['CNT_STATE']})")
            report.append(f"     Stage: {row['LATEST_STAGE_NM']}, Failed: {'Yes' if row['IS_FAILED']==1 else 'No'}")
    
    # 3. Market Analysis
    low_markets = analyze_market_health(market_df)
    if not low_markets.empty:
        report.append(f"\n⚠️ LOW PERFORMING MARKETS ({len(low_markets['MARKET'].unique())} markets):")
        
        # Show worst markets
        worst = low_markets.nsmallest(5, 'SCS_PERCENT')[['MARKET', 'MONTH', 'SCS_PERCENT', 'SEVERITY']]
        for _, row in worst.iterrows():
            report.append(f"   • {row['MARKET']}: {row['SCS_PERCENT']}% ({row['SEVERITY']}) - {row['MONTH']}")
    
    # 4. Critical Markets (below 50%)
    critical = find_critical_markets(market_df)
    if not critical.empty:
        report.append(f"\n🚨 CRITICAL MARKETS (NEED IMMEDIATE ATTENTION):")
        for _, row in critical.iterrows():
            report.append(f"   • {row['MARKET']}: {row['SCS_PERCENT']}% ({row['PRIORITY']})")
    
    # 5. Pipeline Bottlenecks
    bottlenecks = analyze_stage_bottlenecks(roster_df)
    if not bottlenecks.empty:
        report.append(f"\n🔧 PIPELINE BOTTLENECKS:")
        for stage, row in bottlenecks.iterrows():
            report.append(f"   • {stage}: {row['STUCK_COUNT']} stuck, {row['FAILED_COUNT']} failed")
    
    # 6. Problem Organizations
    org_issues = analyze_organization_health(roster_df)
    if not org_issues.empty:
        report.append(f"\n🏢 ORGANIZATIONS WITH MOST ISSUES:")
        for org, row in org_issues.head(5).iterrows():
            report.append(f"   • {org[:30]}...: {row['TOTAL_ISSUES']} issues ({row['STUCK_COUNT']} stuck, {row['FAILED_COUNT']} failed)")
    
    # 7. Retry Effectiveness
    retry = analyze_retry_effectiveness(market_df)
    if retry:
        report.append(f"\n🔄 RETRY EFFECTIVENESS:")
        report.append(f"   • First Pass Success: {retry['first_pass_success']:,}")
        report.append(f"   • Retry Success: {retry['retry_success']:,}")
        report.append(f"   • Improvement: {retry['improvement_percent']}% ({retry['retry_effectiveness']})")
    
    # 8. Summary Recommendations
    report.append(f"\n💡 RECOMMENDATIONS:")
    
    if not stuck.empty:
        report.append(f"   • Investigate {len(stuck)} stuck operations immediately")
    
    if not critical.empty:
        report.append(f"   • URGENT: Fix {len(critical['MARKET'].unique())} critically failing markets")
    
    if not bottlenecks.empty:
        top_stage = bottlenecks.index[0] if not bottlenecks.empty else None
        if top_stage:
            report.append(f"   • Focus on {top_stage} stage - main bottleneck")
    
    report.append("\n" + "="*60)
    report.append("✅ REPORT GENERATED SUCCESSFULLY")
    report.append("="*60)
    
    return "\n".join(report)

def export_insights(roster_df, market_df, filename="pipeline_insights.csv"):
    """
    Export key insights to CSV for reporting
    """
    insights = []
    
    # Market insights
    market_summary = get_market_summary(market_df)
    market_summary.to_csv("market_summary.csv", index=False)
    
    # Stuck operations
    stuck = find_stuck_ros(roster_df)
    if not stuck.empty:
        stuck.to_csv("stuck_operations.csv", index=False)
    
    # Organization issues
    org_issues = analyze_organization_health(roster_df)
    if not org_issues.empty:
        org_issues.to_csv("organization_issues.csv")
    
    print(f"✅ Exported insights to CSV files")
    return True

if __name__ == "__main__":
    # Load the data
    print("\n" + "="*60)
    print("📂 LOADING ROSTERIQ DATASETS")
    print("="*60)
    
    roster, market = load_data()
    
    print(f"✅ Data loaded successfully!")
    print(f"📊 Roster rows: {len(roster):,}")
    print(f"📈 Market rows: {len(market):,}")
    
    # Find stuck operations
    stuck = find_stuck_ros(roster)
    print(f"\n🔍 Stuck operations: {len(stuck)}")
    
    # Find low performing markets
    low = analyze_market_health(market)
    print(f"⚠️ Low performing markets (<85%): {len(low['MARKET'].unique()) if not low.empty else 0}")
    
    # Find critical markets
    critical = find_critical_markets(market)
    if not critical.empty:
        print(f"🚨 Critical markets (<50%): {len(critical['MARKET'].unique())}")
        print("\nWorst markets:")
        for _, row in critical.head(3).iterrows():
            print(f"   • {row['MARKET']}: {row['SCS_PERCENT']}% ({row['PRIORITY']})")
    
    # Generate full report
    report = generate_full_report(roster, market)
    print(report)
    
    # Export insights
    export_insights(roster, market)
    
    print("\n💡 Next steps: Run 'python agent.py' for interactive AI or 'streamlit run app.py' for dashboard")
