import pymongo
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from app.core.config import settings
import time
import datetime
import os

try:
    mongo_uri=settings.MONGODB_URI
    db_name=settings.DB_NAME

    client=pymongo.MongoClient(mongo_uri,serverSelectionTimeoutMS=5000)
    db=client[db_name]
    templates_collection=db["templates"]
    anomalies_collection=db["anomalies"]
    client.server_info()
    print("Anomaly Detector connected to MongoDB.")
except Exception as e:
    print(f"Anomaly Detector failed to connect to MongoDB: {e}")
    exit()

model=IsolationForest(n_estimators=100,contamination=0.05,random_state=42)

def get_features():
    templates = list(templates_collection.find())
    if len(templates) < 10:
        print("Not enough template data to run model. Need at least 10 templates.")
        return None, None

    df = pd.DataFrame(templates)

    if 'frequency' not in df.columns:
        df['frequency']=0

    df['frequency_log']=np.log1p(df['frequency'])

    features=df[['frequency_log']]
    return df,features

def explain_anomaly(template_row):
    freq=template_row['frequency']
    template_str=template_row.get('template_string','Unknown Template')

    explanation=(
        f"Template detected as a frequency outlier. "
        f"It appeared {freq} times, which is statistically abnormal compared "
        f"to the frequency of other log templates. "
        f"Investigate this template: '{template_str}'"
    )
    return explanation

def detect_and_store_anomalies():
    print("Running anomaly detection cycle...")
    df,features=get_features()

    if df is None:
        return
    
    df['anomaly_score']=model.fit_predict(features)
    df['is_anomaly']=df['anomaly_score']==-1

    anomalies=df[df['is_anomaly']]
    print(f"Cycle complete. Scanned {len(df)} templates. Found {len(anomalies)} anomalies")

    if not anomalies.empty:
        for _,row in anomalies.iterrows():
            template_id=row['_id']

            alert_doc={
                "_id": template_id, # Use template_id to prevent duplicate alerts
                "template_string": row['template_string'],
                "frequency": int(row['frequency']),
                "anomaly_score": float(row['anomaly_score']),
                "explanation": explain_anomaly(row),
                "last_detected": datetime.datetime.utcnow()
            }

            anomalies_collection.update_one(
                {"_id":template_id},
                {"$set":alert_doc},
                upsert=True
            )
        print(f"Saved {len(anomalies)} alerts to the 'anomalies_collection'.")

def run_engine():
    
    print("🚀 Anomaly Detection Engine Started. Running detection every 60 seconds.")

    try:
        detect_and_store_anomalies()
    except Exception as e:
        print(f"Error in intial detection cycle: {e}")

    while True:
        time.sleep(60)
        try:
            detect_and_store_anomalies()
        except Exception as e:
            print(f"❌ Error in detection cycle: {e}")
        #time.sleep(60) 

if __name__ == "__main__":
    run_engine()