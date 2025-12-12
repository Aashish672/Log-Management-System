import streamlit as st
import pandas as pd
import pymongo # Use the *synchronous* driver for Streamlit
from app.core.config import settings
from app.modules.compression import CompressionModule
import datetime
import altair as alt
import math

# --- 1. SETUP & DATABASE CONNECTION ---
st.set_page_config(layout="wide", page_title="Cloud Log Dashboard")
st.title("Cloud Log Management Dashboard ☁️")
st.write("Real-time data from the MongoDB ingestion pipeline.")

# Initialize a single compressor instance
@st.cache_resource
def get_compressor():
    return CompressionModule()

compressor = get_compressor()

# Setup synchronous MongoDB connection for Streamlit
@st.cache_resource
def get_mongo_client():
    try:
        # Increase timeout for cloud connections
        client = pymongo.MongoClient(settings.MONGODB_URI, serverSelectionTimeoutMS=10000)
        # Test connection
        client.server_info()
        st.sidebar.success(f"Connected to DB: {settings.DB_NAME}")
        return client
    except pymongo.errors.ServerSelectionTimeoutError as e:
        if "SSL handshake failed" in str(e):
             st.error("MongoDB Connection Failed: SSL Handshake Error. If you are on a restricted network (like a university), try using a mobile hotspot.")
        else:
            st.error(f"Failed to connect to MongoDB: {e}")
        st.sidebar.error("MongoDB Connection Failed")
        return None
    except Exception as e:
        st.error(f"An unexpected connection error occurred: {e}")
        return None

client = get_mongo_client()

if client:
    db = client[settings.DB_NAME]
    templates_collection = db["templates"]
    compressed_collection = db["compressed_blocks"]
    logs_collection = db["logs"]
    anomalies_collection = db["anomalies"]
else:
    st.error("Dashboard cannot load. Please check MongoDB connection and restart.")
    st.stop()

# --- 2. DATA QUERY FUNCTIONS ---

@st.cache_data(ttl=5) # Fast refresh for live logs
def get_live_logs(limit=50):
    """Fetches the most recent raw logs."""
    try:
        logs_cursor = logs_collection.find().sort(
            "timestamp", pymongo.DESCENDING
        ).limit(limit)
        return list(logs_cursor)
    except Exception:
        return []

@st.cache_data(ttl=10)
def get_all_templates():
    """Fetches all unique templates, sorted by frequency."""
    try:
        templates_cursor = templates_collection.find().sort(
            "frequency", pymongo.DESCENDING
        )
        return list(templates_cursor)
    except Exception:
        return []

@st.cache_data(ttl=10)
def get_blocks_for_template(template_id):
    """Fetches compressed blocks for a specific template_id."""
    try:
        blocks_cursor = compressed_collection.find(
            {"template_id": template_id}
        ).sort("start_time", pymongo.DESCENDING)
        return list(blocks_cursor)
    except Exception:
        return []

@st.cache_data(ttl=10)
def get_anomalies():
    """Fetches all detected anomalies."""
    try:
        anomalies_cursor = anomalies_collection.find().sort("last_detected", pymongo.DESCENDING)
        return list(anomalies_cursor)
    except Exception:
        return []

# --- NEW: SEARCH FUNCTION (Module 3.5) ---
@st.cache_data(ttl=5)
def search_logs(keyword, limit=100):
    """
    Implements Hybrid Querying: Searches raw logs for keywords in message, service, or severity.
    """
    try:
        # We use Regex for flexible matching (Case insensitive)
        # In a massive production system, this would use MongoDB Atlas Search indexes.
        query = {
            "$or": [
                {"message": {"$regex": keyword, "$options": "i"}},
                {"service_name": {"$regex": keyword, "$options": "i"}},
                {"severity": {"$regex": keyword, "$options": "i"}}
            ]
        }
        logs_cursor = logs_collection.find(query).sort("timestamp", pymongo.DESCENDING).limit(limit)
        return list(logs_cursor)
    except Exception as e:
        st.error(f"Search failed: {e}")
        return []

# --- 3. REFRESH BUTTON & MAIN UI ---
if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.success(f"Data refreshed at {datetime.datetime.now().strftime('%H:%M:%S')}")

# --- 4. NEW: ANOMALY ALERTS (Module 6) ---
st.header("🚨 Anomaly Alerts (Module 6)")
anomalies = get_anomalies()

if not anomalies:
    st.info("No anomalies detected yet.")
else:
    st.error(f"Detected {len(anomalies)} anomalous log templates!")
    
    # Display top 3 most recent anomalies prominently
    for alert in anomalies[:3]:
        with st.expander(f"🔴 **OUTLIER DETECTED:** {alert.get('template_string', 'Unknown')}"):
            c1, c2 = st.columns([3, 1])
            with c1:
                st.markdown(f"**Explanation:** {alert.get('explanation', 'No explanation available.')}")
                st.caption(f"Last Detected: {alert.get('last_detected', 'Unknown')}")
            with c2:
                st.metric("Frequency", alert.get('frequency', 0))
                # Guard formatting if anomaly_score isn't numeric
                try:
                    score = float(alert.get('anomaly_score', 0))
                    st.metric("Anomaly Score", f"{score:.4f}")
                except Exception:
                    st.metric("Anomaly Score", str(alert.get('anomaly_score', 'N/A')))

st.divider()

# --- 5. NEW: HYBRID QUERY ENGINE (Module 3.5) ---
st.header("🔎 Hybrid Query Engine (Module 3.5)")
st.write("Perform keyword-based search across log messages, services, and severity levels.")

col_search, col_btn = st.columns([4, 1])
with col_search:
    search_term = st.text_input("Enter keyword (e.g., 'timeout', 'admin', 'payment')", "")

if search_term:
    results = search_logs(search_term)
    if results:
        st.success(f"Found {len(results)} logs matching '{search_term}'")
        
        # Display results cleanly
        clean_results = []
        for log in results:
            # Friendly timestamp string
            ts = log.get('timestamp', '')
            # If stored as datetime object, format it
            if isinstance(ts, (datetime.datetime, )):
                ts = ts.isoformat()
            clean_results.append({
                "Time": ts,
                "Service": log.get('service_name', ''),
                "Severity": log.get('severity', ''),
                "Message": log.get('message', '')
            })
        st.dataframe(pd.DataFrame(clean_results), use_container_width=True)
    else:
        st.warning(f"No logs found matching '{search_term}'")

st.divider()

# --- 6. LIVE LOG TAIL ---
st.header("Live Log Tail")
st.write(f"Showing the last 50 raw logs from the '{logs_collection.name}' collection.")
live_logs = get_live_logs()

if not live_logs:
    st.info("No raw logs found. Send data to the `/ingest` endpoint.")
else:
    # We process the list to be DataFrame-friendly
    processed_logs = []
    for log in live_logs:
        # Make a shallow copy to avoid mutating cache
        log_copy = log.copy()
        log_copy.pop('_id', None) # Remove the bulky _id
        
        # Format for display
        params = log_copy.get('parameters', [])
        param_count = len(params) if isinstance(params, (list, tuple)) else (1 if params else 0)
        log_display = {
            "Time": log_copy.get('timestamp', ''),
            "Service": log_copy.get('service_name', ''),
            "Severity": log_copy.get('severity', ''),
            "Message Template": log_copy.get('template', ''),
            "Param Count": param_count
        }
        processed_logs.append(log_display)
        
    st.dataframe(pd.DataFrame(processed_logs), use_container_width=True, height=300)

st.divider()

# --- 7. TEMPLATE & BLOCK EXPLORER ---
st.header("Template & Block Explorer")
st.write(f"Data from '{templates_collection.name}' and '{compressed_collection.name}'.")

templates = get_all_templates()
if not templates:
    st.info("No templates found. Send data to the `/ingest/batch` endpoint.")
else:
    col1, col2 = st.columns([1, 2])
    
    with col1:
        # --- Template Selector ---
        st.subheader("Discovered Templates")
        df_templates = pd.DataFrame(templates)
        # Rename for clarity
        if '_id' in df_templates.columns:
            df_templates = df_templates.rename(columns={"_id": "template_id"})
        
        # Display key columns
        display_cols = ['template_string', 'frequency']
        if 'template_id' in df_templates.columns:
            display_cols.append('template_id')
            
        st.dataframe(df_templates[display_cols], use_container_width=True, height=400)

    with col2:
        # --- Compressed Block Viewer ---
        st.subheader("Compressed Log Block Inspector")
        
        # Create a mapping for the dropdown
        template_options = {}
        for t in templates:
            label = f"{t.get('template_string', 'Unknown')} (Freq: {t.get('frequency', 0)})"
            template_options[label] = t['_id']
        
        selected_template_str = st.selectbox(
            "Select a Log Template to inspect its compressed blocks:", 
            options=list(template_options.keys())
        )
        
        if selected_template_str:
            selected_template_id = template_options[selected_template_str]
            
            blocks = get_blocks_for_template(selected_template_id)
            if not blocks:
                st.warning("No compressed log blocks found for this template yet.")
            else:
                st.write(f"Found {len(blocks)} compressed block(s):")
                
                # Show most recent block first
                latest_block = blocks[0]
                
                block_id = latest_block.get('_id', 'Unknown')
                st.markdown(f"**Viewing Block ID:** `{block_id}`")
                
                # Display block metadata
                meta_cols = st.columns(4)
                meta_cols[0].metric("Log Count", latest_block.get('log_count', 'N/A'))
                meta_cols[1].metric("Compression", latest_block.get('compression_ratio', 'N/A'))
                meta_cols[2].metric("Orig Size", f"{latest_block.get('original_size_bytes', 0)} B")
                meta_cols[3].metric("Comp Size", f"{latest_block.get('compressed_size_bytes', 0)} B")
                
                with st.expander("View Decompressed Parameters (Columnar Format)", expanded=True):
                    try:
                        decompressed_cols = compressor.decompress_block(
                            latest_block['compressed_params_hex']
                        )
                        st.json(decompressed_cols)
                    except Exception as e:
                        st.error(f"Failed to decompress or parse block: {e}")

st.divider()

# --- 8. VISUALIZATION MODULE (Module 3.7) ---
st.header("📊 Visualization (Module 3.7)")
st.write("Charts and tables summarizing recent logs (severity distribution, logs per service, and log volume over time).")

# Controls for visualization timeframe & granularity
viz_col1, viz_col2, viz_col3 = st.columns([1, 1, 1])
with viz_col1:
    days = st.selectbox("Time window", options=[0.01, 0.1, 1, 7, 30], index=2, format_func=lambda d: f"{d} day(s)" if d<2 else f"{int(d)} day(s)")
with viz_col2:
    gran = st.selectbox("Granularity", options=["minute", "hour", "day"], index=1)
with viz_col3:
    max_points = st.slider("Max points (time-series)", min_value=24, max_value=200, value=72)

@st.cache_data(ttl=15)
def get_visualization_data(days_window=1, granularity='hour', max_points=200):
    """
    Aggregate logs_collection for:
      - severity distribution (counts)
      - logs per service (top N)
      - time-series counts (by granularity)
    Returns dict of DataFrames ready for plotting.
    """
    try:
        now = datetime.datetime.utcnow()
        cutoff = now - datetime.timedelta(days=float(days_window))
        # If timestamps stored as strings, try to match ISO strings; if datetimes, use directly.
        match_stage = {"$match": {"timestamp": {"$gte": cutoff}}}
        
        # 1) severity distribution
        sev_pipeline = [
            match_stage,
            {"$group": {"_id": "$severity", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        sev_res = list(logs_collection.aggregate(sev_pipeline))
        df_sev = pd.DataFrame([{"severity": r["_id"] or "UNKNOWN", "count": r["count"]} for r in sev_res])
        if df_sev.empty:
            df_sev = pd.DataFrame(columns=["severity", "count"])
        
        # 2) logs per service
        svc_pipeline = [
            match_stage,
            {"$group": {"_id": "$service_name", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 20}
        ]
        svc_res = list(logs_collection.aggregate(svc_pipeline))
        df_svc = pd.DataFrame([{"service": r["_id"] or "UNKNOWN", "count": r["count"]} for r in svc_res])
        if df_svc.empty:
            df_svc = pd.DataFrame(columns=["service", "count"])
        
        # 3) time-series aggregation
        # Choose bucket size in seconds based on granularity
        if granularity == "minute":
            bucket_secs = 60
        elif granularity == "hour":
            bucket_secs = 3600
        else:
            bucket_secs = 86400
        
        # Create bucketed timestamp using $toDate / $toLong operations if necessary.
        # We'll compute epoch seconds from stored datetime; if stored as strings, Mongo will try to compare earlier via cutoff.
        ts_pipeline = [
            match_stage,
            {"$project": {"ts": {"$toLong": {"$toDate": "$timestamp"}}}},  # convert timestamp to ms
            {"$project": {"bucket": {"$toLong": {"$divide": ["$ts", 1000 * bucket_secs]}}}},
            {"$group": {"_id": "$bucket", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
            {"$limit": max_points}
        ]
        try:
            ts_res = list(logs_collection.aggregate(ts_pipeline))
            # convert buckets back to datetime
            times = []
            counts = []
            for r in ts_res:
                bucket = int(r["_id"])
                ts = datetime.datetime.utcfromtimestamp(bucket * bucket_secs)
                times.append(ts)
                counts.append(r["count"])
            df_ts = pd.DataFrame({"timestamp": times, "count": counts})
        except Exception:
            # Fallback: fetch raw timestamps and build time-series in Python (safer if DB stores mixed formats)
            raw_cursor = logs_collection.find({"timestamp": {"$gte": cutoff}}, {"timestamp": 1})
            timestamps = []
            for doc in raw_cursor:
                t = doc.get("timestamp")
                if isinstance(t, datetime.datetime):
                    timestamps.append(t)
                else:
                    # try parse
                    try:
                        timestamps.append(pd.to_datetime(t))
                    except Exception:
                        continue
            if not timestamps:
                df_ts = pd.DataFrame(columns=["timestamp", "count"])
            else:
                s = pd.Series(1, index=pd.to_datetime(timestamps))
                if granularity == "minute":
                    grouped = s.resample('1T').sum()
                elif granularity == "hour":
                    grouped = s.resample('1H').sum()
                else:
                    grouped = s.resample('1D').sum()
                grouped = grouped.tail(max_points)
                df_ts = pd.DataFrame({"timestamp": grouped.index, "count": grouped.values})
        
        return {"severity": df_sev, "service": df_svc, "timeseries": df_ts}
    except Exception as e:
        st.error(f"Visualization aggregation failed: {e}")
        return {"severity": pd.DataFrame(columns=["severity", "count"]),
                "service": pd.DataFrame(columns=["service", "count"]),
                "timeseries": pd.DataFrame(columns=["timestamp", "count"])}

# Fetch visualization data
viz_data = get_visualization_data(days_window=days, granularity=gran, max_points=max_points)
df_sev = viz_data["severity"]
df_svc = viz_data["service"]
df_ts = viz_data["timeseries"]

# Layout: left for charts, right for tables
left, right = st.columns([2, 1])

with left:
    # Severity distribution - Pie / Donut (using bar as fallback if no alt)
    st.subheader("Severity Distribution")
    if not df_sev.empty and df_sev['count'].sum() > 0:
        pie = alt.Chart(df_sev).mark_arc(innerRadius=50).encode(
            theta=alt.Theta(field="count", type="quantitative"),
            color=alt.Color(field="severity", type="nominal", sort='-y'),
            tooltip=["severity", "count"]
        ).properties(height=300)
        st.altair_chart(pie, use_container_width=True)
    else:
        st.info("No severity data available for the selected time window.")
    
    st.subheader("Logs per Service (Top)")
    if not df_svc.empty and df_svc['count'].sum() > 0:
        bar = alt.Chart(df_svc).mark_bar().encode(
            x=alt.X("count:Q"),
            y=alt.Y("service:N", sort='-x'),
            tooltip=["service", "count"]
        ).properties(height=300)
        st.altair_chart(bar, use_container_width=True)
    else:
        st.info("No service data available for the selected time window.")

    st.subheader("Log Volume Over Time")
    if not df_ts.empty and len(df_ts) > 0:
        line = alt.Chart(df_ts).mark_line(point=True).encode(
            x=alt.X("timestamp:T", title="Time"),
            y=alt.Y("count:Q", title="Log Count"),
            tooltip=[alt.Tooltip("timestamp:T", title="Time"), alt.Tooltip("count:Q", title="Logs")]
        ).properties(height=300)
        st.altair_chart(line, use_container_width=True)
    else:
        st.info("No time-series data for selected window.")

with right:
    st.subheader("Severity Table")
    if not df_sev.empty:
        st.table(df_sev.sort_values("count", ascending=False).reset_index(drop=True))
    else:
        st.info("No severity counts to display.")
    
    st.subheader("Top Services Table")
    if not df_svc.empty:
        st.table(df_svc.sort_values("count", ascending=False).reset_index(drop=True))
    else:
        st.info("No service counts to display.")

st.divider()

st.caption("Module layout: Module 3.5 (Hybrid Query Engine) • Module 3.7 (Visualization) • Module 6 (Anomaly Alerts).")
