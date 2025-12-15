import streamlit as st
import pandas as pd
import pymongo
import datetime
import altair as alt
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

# ---------- Module 3.5 ----------
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

# --------------------------------------------------
# 4. EXECUTIVE METRICS (FROM CODE-1)
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
    "🚨 Analytics & Alerts (Module 6)",
    "🔍 Hybrid Query Engine (3.5)",
    "🛠 Template & Block Explorer",
    "📊 Visualization (3.7)"
])

# ==================================================
# TAB 1 — MODULE 6: ANOMALY DETECTION
# ==================================================
with tab1:
    anomalies = get_anomalies()

    if not anomalies:
        st.success("System Healthy — No anomalies detected.")
    else:
        st.error(f"{len(anomalies)} anomalous templates detected")

        for a in anomalies[:5]:
            with st.expander(f"⚠️ {a.get('template_string', 'Unknown')}"):
                st.write(f"**Explanation:** {a.get('explanation')}")
                st.metric("Frequency", a.get("frequency", 0))
                st.metric("Anomaly Score", f"{float(a.get('anomaly_score', 0)):.4f}")
                st.caption(f"Last Detected: {a.get('last_detected')}")

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
                data = compressor.decompress_block(b["compressed_params_hex"])
                st.json(data)

# ==================================================
# TAB 4 — MODULE 3.7: VISUALIZATION
# ==================================================
with tab4:
    st.subheader("Log Analytics & Trends")

    days = st.selectbox("Time Window", [1, 7, 30], index=1)
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=days)

    df = pd.DataFrame(
        list(logs_collection.find({"timestamp": {"$gte": cutoff}}))
    )

    if df.empty:
        st.info("No logs available")
    else:
        col1, col2 = st.columns(2)

        with col1:
            sev = df["severity"].value_counts().reset_index()
            sev.columns = ["severity", "count"]
            fig = px.pie(sev, names="severity", values="count", hole=0.4)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            svc = df["service_name"].value_counts().head(10).reset_index()
            svc.columns = ["service", "count"]
            st.bar_chart(svc, x="service", y="count")

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        ts = df.set_index("timestamp").resample("1H").size().reset_index(name="count")

        st.line_chart(ts, x="timestamp", y="count")

# --------------------------------------------------
st.caption(
    "Modules Implemented: Module 3.5 (Hybrid Query Engine) • "
    "Module 3.7 (Visualization) • Module 6 (Explainable Anomaly Detection)"
)
