import streamlit as st
import pandas as pd
import pymongo # Use the *synchronous* driver for Streamlit
from app.core.config import settings
from app.modules.compression import CompressionModule
import datetime

# --- 1. SETUP & DATABASE CONNECTION ---
st.set_page_config(layout="wide")
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
        client = pymongo.MongoClient(settings.MONGODB_URI, serverSelectionTimeoutMS=5000)
        # Test connection
        client.server_info()
        st.sidebar.success(f"Connected to DB: {settings.DB_NAME}")
        return client
    except pymongo.errors.ServerSelectionTimeoutError as e:
        st.error(f"Failed to connect to MongoDB: {e}")
        st.sidebar.error("MongoDB Connection Failed")
        return None

client = get_mongo_client()

if client:
    db = client[settings.DB_NAME]
    templates_collection = db["templates"]
    compressed_collection = db["compressed_blocks"]
    logs_collection = db["logs"]
else:
    st.error("Dashboard cannot load. Please check MongoDB connection and restart.")
    st.stop() # Stop the script if DB connection fails

# --- 2. DATA QUERY FUNCTIONS ---

@st.cache_data(ttl=10) # Cache data for 10 seconds
def get_live_logs(limit=50):
    """Fetches the most recent raw logs."""
    logs_cursor = logs_collection.find().sort(
        "timestamp", pymongo.DESCENDING
    ).limit(limit)
    return list(logs_cursor)

@st.cache_data(ttl=10)
def get_all_templates():
    """Fetches all unique templates, sorted by frequency."""
    templates_cursor = templates_collection.find().sort(
        "frequency", pymongo.DESCENDING
    )
    return list(templates_cursor)

@st.cache_data(ttl=10)
def get_blocks_for_template(template_id):
    """Fetches compressed blocks for a specific template_id."""
    blocks_cursor = compressed_collection.find(
        {"template_id": template_id}
    ).sort("start_time", pymongo.DESCENDING)
    return list(blocks_cursor)

# --- 3. REFRESH BUTTON & MAIN UI ---
if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.success(f"Data refreshed at {datetime.datetime.now()}")

# --- 4. LIVE LOG TAIL ---
st.header("Live Log Tail")
st.write(f"Showing the last 50 raw logs from the '{logs_collection.name}' collection.")
live_logs = get_live_logs()
if not live_logs:
    st.info("No raw logs found. Send data to the `/ingest` or `/ingest/batch` endpoint.")
else:
    # We process the list to be DataFrame-friendly
    processed_logs = []
    for log in live_logs:
        log.pop('_id', None) # Remove the bulky _id
        log['template'] = log.pop('template', '') # Shorten for display
        log['parameters'] = f"{len(log.get('parameters', []))} params"
        processed_logs.append(log)
    st.dataframe(processed_logs, use_container_width=True, height=300)

st.divider()

# --- 5. TEMPLATE & BLOCK VIEWER ---
st.header("Template & Block Explorer")
st.write(f"Data from '{templates_collection.name}' and '{compressed_collection.name}'.")

templates = get_all_templates()
if not templates:
    st.info("No templates found. Send data to the `/ingest/batch` endpoint to create compressed blocks.")
else:
    col1, col2 = st.columns([1, 2])
    
    with col1:
        # --- Template Selector ---
        st.subheader("Discovered Templates")
        df_templates = pd.DataFrame(templates)
        # Rename _id for clarity
        df_templates = df_templates.rename(columns={"_id": "template_id"})
        st.dataframe(df_templates, use_container_width=True, height=400)

    with col2:
        # --- Compressed Block Viewer ---
        st.subheader("Compressed Log Block Inspector")
        
        #
        # THIS IS THE LINE YOU NEED TO FIX
        #
        template_options = {
            # Use template_string for display, _id (which is template_id) for the key
            f"{t['template_string']} (Freq: {t.get('frequency', 0)})": t['_id'] 
            for t in templates
        }
        
        selected_template_str = st.selectbox(
            "Select a Log Template to inspect its compressed blocks:", 
            options=template_options.keys()
        )
        
        if selected_template_str:
            selected_template_id = template_options[selected_template_str]
            
            blocks = get_blocks_for_template(selected_template_id)
            if not blocks:
                st.warning("No compressed log blocks found for this template yet.")
            else:
                st.write(f"Found {len(blocks)} compressed block(s):")
                
                for block in blocks:
                    block_id = block.pop('_id')
                    st.markdown(f"**Block ID:** `{block_id}`")
                    
                    # Display block metadata
                    meta_cols = st.columns(4)
                    meta_cols[0].metric("Log Count", block.get('log_count', 'N/A'))
                    meta_cols[1].metric("Compression", block.get('compression_ratio', 'N/A'))
                    meta_cols[2].metric("Original Size", f"{block.get('original_size_bytes', 0)} B")
                    meta_cols[3].metric("Compressed Size", f"{block.get('compressed_size_bytes', 0)} B")
                    
                    with st.expander("View Decompressed Parameters (Columnar Format)"):
                        try:
                            decompressed_cols = compressor.decompress_block(
                                block['compressed_params_hex']
                            )
                            st.json(decompressed_cols)
                        except Exception as e:
                            st.error(f"Failed to decompress or parse block: {e}")