import pymongo
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
import shap
from app.core.config import settings
import time
import datetime
import os

# --- Database Connection ---
try:
    mongo_uri = settings.MONGODB_URI
    db_name = settings.DB_NAME
    
    client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    db = client[db_name]
    templates_collection = db["templates"]
    anomalies_collection = db["anomalies"]
    client.server_info() 
    print("✅ Anomaly Detector connected to MongoDB.")
except Exception as e:
    print(f"❌ Anomaly Detector failed to connect to MongoDB: {e}")
    # In a real deployment, we might want to retry loop here

# --- Model Configuration ---
# Isolation Forest is effective for high-dimensional anomaly detection
# Contamination is the expected proportion of outliers in the dataset
MODEL_CONTAMINATION = 0.05 
model = IsolationForest(n_estimators=100, contamination=MODEL_CONTAMINATION, random_state=42)

def get_data_and_features():
    """
    Fetches all template data from MongoDB and engineers advanced features.
    Training on existing data ensures the model learns historical baselines.
    """
    templates = list(templates_collection.find())
    
    if len(templates) < 5: 
        print(f"Waiting for more data... (Current templates: {len(templates)})")
        return None, None, None

    df = pd.DataFrame(templates)
    
    # --- Advanced Feature Engineering ---
    
    # 1. Log Frequency (Primary Feature)
    # We log-transform to handle skewness (power law distribution of logs)
    if 'frequency' not in df.columns:
        df['frequency'] = 0
    df['frequency_log'] = np.log1p(df['frequency']) 
    
    # 2. Burstiness / Recent Activity (Simulated for this demo)
    # In a real production system, we would query the time-series logs to calculate this.
    # For now, we can infer 'volatility' if the frequency is extremely high relative to others.
    # (Here we just use frequency_log as the main feature for SHAP to explain)
    
    # Select features for the model
    feature_cols = ['frequency_log']
    features = df[feature_cols]
    
    return df, features, feature_cols

def generate_shap_explanation(model, features_data, anomalous_row_idx):
    """
    Uses SHAP (SHapley Additive exPlanations) to explain WHY a specific row is an anomaly.
    Returns a text explanation of the top contributing features.
    """
    try:
        # Create a SHAP explainer for the Isolation Forest model
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(features_data)
        
        # Get SHAP values for the specific anomalous row
        # shap_values is a list of arrays (one for each class), or just an array for regression/IF
        # For Isolation Forest in newer sklearn/shap versions, it might just be the array.
        if isinstance(shap_values, list):
             row_shap = shap_values[0][anomalous_row_idx]
        else:
             row_shap = shap_values[anomalous_row_idx]

        feature_names = features_data.columns
        
        # Sort features by their absolute contribution to the anomaly score
        # In Isolation Forest, lower score = more anomalous. 
        # SHAP values indicate direction. Negative SHAP pushes towards anomaly.
        
        # We want to find features that pushed the score LOWER (more anomalous)
        sorted_indices = np.argsort(row_shap) # Ascending order (most negative first)
        
        top_feature_idx = sorted_indices[0]
        top_feature_name = feature_names[top_feature_idx]
        top_shap_val = row_shap[top_feature_idx]
        
        # Generate human-readable text based on the feature
        explanation = f"SHAP Analysis: The feature '{top_feature_name}' contributed most significantly ({top_shap_val:.2f}) to this anomaly."
        
        if top_feature_name == 'frequency_log':
             raw_val = np.expm1(features_data.iloc[anomalous_row_idx][top_feature_name])
             explanation += f" The frequency ({int(raw_val)}) is statistically highly unusual compared to the baseline."
             
        return explanation

    except Exception as e:
        print(f"SHAP explanation failed: {e}")
        return "Statistical outlier detected by Isolation Forest (SHAP generation skipped)."

def detect_and_store_anomalies():
    """
    Main function to run the detection pipeline with SHAP explanations.
    """
    print("🧠 Running intelligent anomaly detection cycle...")
    df, features, feature_cols = get_data_and_features()
    
    if df is None:
        return

    # 1. Train the model on ALL existing data
    # This "retraining" approach allows the model to adapt to new normal patterns over time.
    model.fit(features)
    
    # 2. Predict anomalies
    df['anomaly_score'] = model.decision_function(features)
    df['is_anomaly'] = model.predict(features) == -1 # -1 means anomaly

    anomalies = df[df['is_anomaly']]
    print(f"✅ Cycle complete. Scanned {len(df)} templates. Found {len(anomalies)} anomalies.")

    # 3. Explain and Store Anomalies
    if not anomalies.empty:
        # Pre-compute SHAP explainer once for efficiency if needed, 
        # but creating it per batch is fine for this scale.
        
        for idx, row in anomalies.iterrows():
            template_id = row['_id'] 
            
            # Generate SHAP explanation for this specific anomaly
            # We pass the index of the row relative to the 'features' dataframe
            shap_explanation = generate_shap_explanation(model, features, idx)
            
            alert_doc = {
                "_id": template_id,
                "template_string": row.get('template_string', 'Unknown'),
                "frequency": int(row['frequency']),
                "anomaly_score": float(row['anomaly_score']),
                "explanation": shap_explanation,
                "model_version": "IsolationForest_v1",
                "last_detected": datetime.datetime.utcnow()
            }
            
            anomalies_collection.update_one(
                {"_id": template_id},
                {"$set": alert_doc},
                upsert=True
            )
        print(f"💾 Saved {len(anomalies)} intelligent alerts to MongoDB.")

def run_engine():
    """
    Runs the intelligent anomaly detector loop.
    """
    print("🚀 Intelligent Anomaly Engine Started (with SHAP Explainability).")
    
    try:
        detect_and_store_anomalies()
    except Exception as e:
        print(f"❌ Error in initial detection cycle: {e}")

    while True:
        time.sleep(60)
        try:
            detect_and_store_anomalies()
        except Exception as e:
            print(f"❌ Error in detection cycle: {e}")

if __name__ == "__main__":
    run_engine()