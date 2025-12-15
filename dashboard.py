import streamlit as st
import pandas as pd
import pymongo
import datetime
import plotly.express as px

from app.core.config import settings
from app.modules.compression import CompressionModule

# --------------------------------------------------
# 1. PAGE CONFIG + STYLING
# --------------------------------------------------
st.set_page_config(
    layout="wide",
    page_title="Cloud Log Intelligence System",
    page_icon="☁️"
)

st.markdown("""
<style>
.metric-card {
    background-color: #f0f2f6;
    border-left: 5px solid #4F8BF9;
    padding: 20px;
    border-radius: 6px;
}
</style>
""", unsafe_allow_html=True)

st.title("☁️ Cloud Log Intelligence System")
st.markdown("### Enterprise-grade Log Ingestion, Compression & Explainable Anomaly Detection")

# --------------------------------------------------
# 2. RESOURCE INITIALIZATION
# --------------------------------------------------
@st.cache_resource
def get_compressor():
    return CompressionModule()

@st.cache_resource
def get_mongo_client():
    try:
        client = pymongo.MongoClient(
            settings.MONGODB_URI,
            serverSelectionTimeoutMS=10000
        )
        client.server_info()
        return client
    except Exception as e:
        st.error(f"MongoDB Connection Failed: {e}")
        return None

compressor = get_compressor()
client = get_mongo_client()

if not client:
    st.stop()

db = client[settings.DB_NAME]
templates_collection = db["templates"]
compressed_collection = db["compressed_blocks"]
logs_collection = db["logs"]
anomalies_collection = db["anomalies"]
incidents_collection = db["incidents"]  

# --------------------------------------------------
# 3. DATA FUNCTIONS
# --------------------------------------------------
def get_system_stats():
    total_templates = templates_collection.count_documents({})

    pipeline = [{
        "$group": {
            "_id": None,
            "logs": {"$sum": "$log_count"},
            "orig": {"$sum": "$original_size_bytes"},
            "comp": {"$sum": "$compressed_size_bytes"}
        }
    }]

    res = list(compressed_collection.aggregate(pipeline))
    if res:
        r = res[0]
        saved = ((r["orig"] - r["comp"]) / r["orig"] * 100) if r["orig"] else 0
        return {
            "logs": r["logs"],
            "templates": total_templates,
            "orig_mb": r["orig"] / (1024 * 1024),
            "comp_mb": r["comp"] / (1024 * 1024),
            "saved_pct": saved
        }
    return {"logs": 0, "templates": 0, "orig_mb": 0, "comp_mb": 0, "saved_pct": 0}

@st.cache_data(ttl=5)
def get_live_logs(limit=50):
    return list(logs_collection.find().sort("timestamp", -1).limit(limit))

@st.cache_data(ttl=10)
def get_templates():
    return list(templates_collection.find().sort("frequency", -1))

@st.cache_data(ttl=10)
def get_blocks(template_id):
    return list(compressed_collection.find({"template_id": template_id}).sort("start_time", -1))

@st.cache_data(ttl=10)
def get_anomalies():
    return list(anomalies_collection.find().sort("last_detected", -1))

@st.cache_data(ttl=5)
def search_logs(keyword, limit=100):
    query = {
        "$or": [
            {"message": {"$regex": keyword, "$options": "i"}},
            {"service_name": {"$regex": keyword, "$options": "i"}},
            {"severity": {"$regex": keyword, "$options": "i"}}
        ]
    }
    return list(logs_collection.find(query).sort("timestamp", -1).limit(limit))

@st.cache_data(ttl=10)
def get_anomaly_heatmap_data(hours=24):
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=hours)

    anomalies = list(anomalies_collection.find({
        "last_detected": {"$gte": cutoff}
    }))

    if not anomalies:
        return pd.DataFrame()

    df = pd.DataFrame(anomalies)
    df["time_bucket"] = pd.to_datetime(df["last_detected"]).dt.floor("H")

    severity_map = {
        "MEDIUM": 1,
        "HIGH": 2,
        "CRITICAL": 3
    }
    df["severity_level"] = df["severity"].map(severity_map).fillna(0)

    return df

@st.cache_data(ttl=10)
def get_active_incidents():
    return list(incidents_collection.find().sort("last_updated", -1))

# --------------------------------------------------
# 4. EXECUTIVE METRICS
# --------------------------------------------------
stats = get_system_stats()
m1, m2, m3, m4 = st.columns(4)

m1.metric("Total Logs Ingested", f"{stats['logs']:,}")
m2.metric("Unique Templates", stats["templates"])
m3.metric("Compressed Storage", f"{stats['comp_mb']:.2f} MB")
m4.metric("Storage Savings", f"{stats['saved_pct']:.1f}%")

st.divider()

# --------------------------------------------------
# 5. TABBED DASHBOARD
# --------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "🚨 Anomalies & Severity (Module 6)",
    "🔍 Hybrid Query Engine",
    "🛠 System Explorer",
    "📊 Incidents & Analytics"
])


# ==================================================
# TAB 1 — MODULE 6: ANOMALY DETECTION
# ==================================================
with tab1:
    st.subheader("🚨 Active Anomaly Alerts")

    anomalies = get_anomalies()

    if not anomalies:
        st.success("System Healthy — No anomalies detected.")
    else:
        df = pd.DataFrame(anomalies)

        severity_filter = st.multiselect(
            "Filter by Severity",
            ["CRITICAL", "HIGH", "MEDIUM"],
            default=["CRITICAL", "HIGH"]
        )

        # Filter if the dataframe is not empty
        if not df.empty:
            df = df[df["severity"].isin(severity_filter)]

            for _, alert in df.iterrows():
                sev = alert["severity"]
                icon = "🔴" if sev == "CRITICAL" else ("🟠" if sev == "HIGH" else "🟡")

                with st.expander(f"{icon} {sev} — {alert['template_string']}"):
                    st.write(f"**Explanation:** {alert['explanation']}")
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Frequency", alert["frequency"])
                    c2.metric("Recent (1h)", alert.get("recent_frequency", 0))
                    c3.metric("Anomaly Score", f"{alert['anomaly_score']:.4f}")
                    st.caption(f"Last Detected: {alert['last_detected']}")

# ==================================================
# TAB 2 — MODULE 3.5: HYBRID QUERY ENGINE
# ==================================================
with tab2:
    st.subheader("Keyword Search across Logs")

    search_term = st.text_input(
        "Search message / service / severity",
        placeholder="timeout | auth-service | CRITICAL"
    )

    if search_term:
        results = search_logs(search_term)
        if results:
            clean = []
            for r in results:
                clean.append({
                    "Time": r.get("timestamp"),
                    "Service": r.get("service_name"),
                    "Severity": r.get("severity"),
                    "Message": r.get("message")
                })
            st.success(f"Found {len(clean)} logs")
            st.dataframe(pd.DataFrame(clean), use_container_width=True)
        else:
            st.warning("No matching logs found")

# ==================================================
# TAB 3 — TEMPLATE & BLOCK EXPLORER
# ==================================================
with tab3:
    templates = get_templates()
    if not templates:
        st.info("No templates available")
    else:
        t_map = {
            f"{t['template_string']} (Freq: {t['frequency']})": t["_id"]
            for t in templates
        }

        selected = st.selectbox("Select Template", list(t_map.keys()))
        blocks = get_blocks(t_map[selected])

        if not blocks:
            st.warning("No compressed blocks found")
        else:
            b = blocks[0]

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Log Count", b["log_count"])
            c2.metric("Compression Ratio", b["compression_ratio"])
            c3.metric("Original Size", f"{b['original_size_bytes']} B")
            c4.metric("Compressed Size", f"{b['compressed_size_bytes']} B")

            with st.expander("Decompressed Parameters"):
                hex_data = b.get("compressed_params_hex")
                if hex_data:
                    try:
                        data = compressor.decompress_block(hex_data)
                        st.json(data)
                    except Exception as e:
                        st.error(f"Decompression Error: {e}")
                else:
                    st.warning("No compressed data found in this block (Key 'compressed_params_hex' missing).")
                    st.write("Raw block data:", b)

# ==================================================
# TAB 4 — MODULE 3.7: VISUALIZATION & ANALYTICS
# ==================================================
with tab4:
    st.subheader("🔥 Anomaly Severity Heatmap (Last 24 Hours)")

    heatmap_df = get_anomaly_heatmap_data(24)

    if heatmap_df.empty:
        st.info("No recent anomalies to visualize.")
    else:
        pivot = heatmap_df.pivot_table(
            index="template_string",
            columns="time_bucket",
            values="severity_level",
            aggfunc="max",
            fill_value=0
        )

        fig = px.imshow(
            pivot,
            aspect="auto",
            color_continuous_scale=[
                [0, "#E8F5E9"],  # 0: None
                [0.33, "#FFF59D"], # 1: MEDIUM
                [0.66, "#FFB74D"], # 2: HIGH
                [1.0, "#FF7043"]   # 3: CRITICAL
            ],
            labels={
                "x": "Time",
                "y": "Log Template",
                "color": "Severity Level"
            }
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("📌 Correlated Incidents")

    incidents = get_active_incidents()

    if not incidents:
        st.success("No active incidents detected.")
    else:
        for inc in incidents:
            sev_icon = "🔴" if inc["severity"] == "CRITICAL" else "🟠"

            with st.expander(f"{sev_icon} {inc['severity']} — {inc.get('title', 'Incident')}"):
                st.write(f"**Status:** {inc.get('status', 'Unknown')}")
                st.write(f"**Anomaly Count:** {inc.get('anomaly_count', 0)}")
                st.caption(f"Last Updated: {inc.get('last_updated')}")

                st.write("### Associated Anomalies")
                anoms = inc.get("anomalies", [])
                if anoms:
                    st.dataframe(
                        pd.DataFrame(anoms)[
                            ["template_string", "severity", "score", "timestamp"]
                        ],
                        use_container_width=True
                    )

    st.divider()
    st.subheader("📈 Global Log Analytics")
    
    days = st.selectbox("Time Window", [1, 7, 30], index=1)
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=days)

    # Fetch logs for analytics (limit to prevent memory issues)
    df_logs = pd.DataFrame(
        list(logs_collection.find({"timestamp": {"$gte": cutoff}}).limit(5000))
    )

    if df_logs.empty:
        st.info("No logs available for analytics in this window.")
    else:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Logs by Severity**")
            sev = df_logs["severity"].value_counts().reset_index()
            sev.columns = ["severity", "count"]
            fig_pie = px.pie(sev, names="severity", values="count", hole=0.4, color_discrete_sequence=px.colors.sequential.RdBu)
            st.plotly_chart(fig_pie, use_container_width=True)

        with col2:
            st.markdown("**Top Services**")
            svc = df_logs["service_name"].value_counts().head(10).reset_index()
            svc.columns = ["service", "count"]
            fig_bar = px.bar(svc, x="service", y="count", color="count")
            st.plotly_chart(fig_bar, use_container_width=True)

        st.markdown("**Log Volume Over Time**")
        df_logs["timestamp"] = pd.to_datetime(df_logs["timestamp"])
        ts = df_logs.set_index("timestamp").resample("1H").size().reset_index(name="count")
        
        fig_line = px.line(ts, x="timestamp", y="count", markers=True)
        st.plotly_chart(fig_line, use_container_width=True)

# --------------------------------------------------
st.caption(
    "System Status: Online • Modules Implemented: Module 3.5 (Hybrid Query Engine) • "
    "Module 3.7 (Visualization) • Module 6 (Explainable Anomaly Detection)"
)