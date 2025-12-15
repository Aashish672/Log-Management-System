import pymongo
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import shap
from app.core.config import settings
import time
import datetime
import os

# --- Configuration ---
# How far back to look for "recent" activity (e.g., 60 minutes)
TIME_WINDOW_MINUTES = 60 
MODEL_CONTAMINATION = 0.05 
# Correlation window: Group anomalies within X minutes of each other
CORRELATION_WINDOW_MINUTES = 15

# --- Database Connection ---
try:
    mongo_uri = settings.MONGODB_URI
    db_name = settings.DB_NAME
    
    client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    db = client[db_name]
    templates_collection = db["templates"]
    logs_collection = db["logs"] 
    anomalies_collection = db["anomalies"]
    incidents_collection = db["incidents"] # New collection for correlated incidents
    client.server_info() 
    print("✅ Anomaly Detector connected to MongoDB.")
except Exception as e:
    print(f"❌ Anomaly Detector failed to connect to MongoDB: {e}")

# --- Model ---
model = IsolationForest(n_estimators=200, contamination=MODEL_CONTAMINATION, random_state=42)

def classify_severity(score, frequency):
    """
    Maps the anomaly score (lower is more anomalous) AND frequency to a severity level.
    High frequency anomalies are often more critical (bursts).
    """
    # Base severity from score
    if score < -0.20:
        base_severity = 3 # Critical
    elif score < -0.10:
        base_severity = 2 # High
    else:
        base_severity = 1 # Medium
    
    # Boost severity if frequency is very high (potential DDoS or cascading failure)
    if frequency > 1000:
        base_severity = max(base_severity, 3)
    elif frequency > 500:
        base_severity = max(base_severity, 2)
        
    severity_map = {1: "MEDIUM", 2: "HIGH", 3: "CRITICAL"}
    return severity_map.get(base_severity, "MEDIUM")

def get_recent_frequencies(template_id):
    """
    Query real log activity for a template in time windows (1h and 24h).
    """
    now = datetime.datetime.utcnow()
    last_1h = now - datetime.timedelta(hours=1)
    last_24h = now - datetime.timedelta(hours=24)

    freq_1h = logs_collection.count_documents({
        "template_id": template_id,
        "timestamp": {"$gte": last_1h}
    })

    freq_24h = logs_collection.count_documents({
        "template_id": template_id,
        "timestamp": {"$gte": last_24h}
    })

    return freq_1h, freq_24h

def engineer_features(df):
    """
    Engineers features using REAL historical data vs. recent activity.
    """
    feature_rows = []
    
    for _, row in df.iterrows():
        template_id = row["_id"] 
        freq_total = row.get("frequency", 0)

        freq_1h, freq_24h = get_recent_frequencies(template_id)

        avg_hourly = max(freq_24h / 24, 1)
        burst_ratio = freq_1h / avg_hourly

        feature_rows.append({
            "frequency_log": np.log1p(freq_total),
            "burst_ratio": burst_ratio,
            "freq_1h": freq_1h, 
            "freq_24h": freq_24h
        })

    features_df = pd.DataFrame(feature_rows)
    feature_cols = ["frequency_log", "burst_ratio"]
    
    df_enriched = pd.concat([df.reset_index(drop=True), features_df], axis=1)
    
    return df_enriched, features_df[feature_cols], feature_cols

def get_data_and_features():
    templates = list(templates_collection.find())
    
    if len(templates) < 5: 
        print(f"Waiting for more data... (Current templates: {len(templates)})")
        return None, None, None

    df = pd.DataFrame(templates)
    df, features, feature_cols = engineer_features(df)
    return df, features, feature_cols

def generate_shap_explanation(model, features, idx):
    try:
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(features)
        row_shap = shap_values[idx] if not isinstance(shap_values, list) else shap_values[0][idx]

        feature_names = features.columns
        top_features = sorted(zip(feature_names, row_shap), key=lambda x: abs(x[1]), reverse=True)[:2]

        explanation = "SHAP Analysis: "
        explanation += ", ".join([f"Feature '{f}' impact ({v:.2f})" for f, v in top_features])
        return explanation
    except Exception as e:
        print(f"SHAP Error: {e}")
        return "Statistical outlier (SHAP calculation failed)"

def correlate_incidents(new_anomalies):
    """
    Groups new anomalies into 'Incidents' based on time proximity.
    """
    if not new_anomalies:
        return

    now = datetime.datetime.utcnow()
    window_start = now - datetime.timedelta(minutes=CORRELATION_WINDOW_MINUTES)
    
    # 1. Find existing active incidents in the window
    active_incidents = list(incidents_collection.find({
        "last_updated": {"$gte": window_start},
        "status": "OPEN"
    }))
    
    # 2. Try to map new anomalies to active incidents
    # Simple logic: If an open incident exists, append to it. If not, create new.
    # Ideally, you'd match by Service Name, but Template ID is our main key here.
    
    current_incident_id = None
    
    if active_incidents:
        # Append to the most recent active incident
        current_incident = active_incidents[0]
        current_incident_id = current_incident["_id"]
        
        incidents_collection.update_one(
            {"_id": current_incident_id},
            {
                "$push": {"anomalies": {"$each": new_anomalies}},
                "$set": {
                    "last_updated": now, 
                    "anomaly_count": current_incident["anomaly_count"] + len(new_anomalies),
                    # Upgrade severity if we see critical anomalies
                    "severity": "CRITICAL" if any(a['severity'] == 'CRITICAL' for a in new_anomalies) else current_incident["severity"]
                }
            }
        )
        print(f"🔗 Correlated {len(new_anomalies)} anomalies to Incident {current_incident_id}")
        
    else:
        # Create new incident
        # Only create incident if we have High/Critical anomalies
        criticality = [a['severity'] for a in new_anomalies]
        highest_severity = "CRITICAL" if "CRITICAL" in criticality else ("HIGH" if "HIGH" in criticality else "MEDIUM")
        
        new_incident = {
            "created_at": now,
            "last_updated": now,
            "status": "OPEN",
            "severity": highest_severity,
            "anomalies": new_anomalies,
            "anomaly_count": len(new_anomalies),
            "title": f"Incident: Burst of {len(new_anomalies)} anomalies detected"
        }
        res = incidents_collection.insert_one(new_incident)
        print(f"🆕 Created new Incident {res.inserted_id} with severity {highest_severity}")

def detect_and_store_anomalies():
    print("🧠 Running intelligent anomaly detection cycle...")
    
    result = get_data_and_features()
    if result is None: return

    df, features, feature_cols = result

    # 1. Train & Predict
    model.fit(features)
    df['anomaly_score'] = model.decision_function(features)
    df['is_anomaly'] = model.predict(features) == -1 
    
    # 2. Classify Severity
    df["severity"] = df.apply(lambda x: classify_severity(x['anomaly_score'], x['freq_1h']), axis=1)
    
    anomalies_df = df[df['is_anomaly']]
    print(f"✅ Cycle complete. Found {len(anomalies_df)} anomalies.")

    # 3. Store Individual Anomalies & Prepare for Correlation
    new_anomaly_records = []
    
    if not anomalies_df.empty:
        for idx, row in anomalies_df.iterrows():
            template_id = row['_id'] 
            shap_ex = generate_shap_explanation(model, features, idx)
            MODEL_METADATA = {
                "model": "IsolationForest",
                "version": "IF_v2.1",
                "features": ["frequency_log", "burst_ratio"],
                "time_window_minutes": TIME_WINDOW_MINUTES,
                "trained_on_samples": len(df),
                "contamination": MODEL_CONTAMINATION
            }
            alert_doc = {
                "_id": template_id,
                "template_string": row.get('template_string', 'Unknown'),
                "frequency": int(row['frequency']),
                "recent_frequency": int(row['freq_1h']), 
                "anomaly_score": float(row['anomaly_score']),
                "severity": row["severity"],
                "explanation": shap_ex,
                
                # --- INCLUDE IT IN THE DOCUMENT ---
                "model_metadata": MODEL_METADATA, 
                # ----------------------------------
                
                "last_detected": datetime.datetime.utcnow()
            }
            
            # Upsert individual alert
            anomalies_collection.update_one(
                {"_id": template_id},
                {"$set": alert_doc},
                upsert=True
            )
            
            # Prepare record for Incident Correlation (flattened)
            new_anomaly_records.append({
                "template_id": template_id,
                "template_string": row.get('template_string', 'Unknown'),
                "severity": row["severity"],
                "score": float(row['anomaly_score']),
                "timestamp": datetime.datetime.utcnow()
            })
            
        # 4. Run Correlation Logic
        # Only correlate High/Critical anomalies to avoid noise
        important_anomalies = [a for a in new_anomaly_records if a['severity'] in ['HIGH', 'CRITICAL']]
        if important_anomalies:
            correlate_incidents(important_anomalies)

def run_engine():
    print("🚀 Intelligent Anomaly Engine Started (Real-Time + Correlation).")
    while True:
        try:
            detect_and_store_anomalies()
        except Exception as e:
            print(f"❌ Error in detection cycle: {e}")
        time.sleep(60)

if __name__ == "__main__":
    run_engine()