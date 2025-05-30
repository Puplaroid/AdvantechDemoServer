from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit
import paho.mqtt.client as mqtt
import psycopg2
import json
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()
postgres_password = os.getenv("PG_PASSWORD")
postgres_host = os.getenv("PG_HOST")
postgres_port = os.getenv("PG_PORT", 5432)  # default to 5432 if not set
postgres_db = os.getenv("PG_DATABASE")
postgres_user = os.getenv("PG_USER")

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins='*')


data_storage = []

def to_epoch_ms(timestamp_str):
    dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    return int(dt.timestamp() * 1000)

# ---------------------------
# PostgreSQL
# ---------------------------
try:
    conn = psycopg2.connect(
        host=postgres_host,             
        port=postgres_port,
        database=postgres_db,
        user=postgres_user,
        password=postgres_password
    )

    print("✅ PostgreSQL connection established.")
    app.config['PG_CONN'] = conn
except Exception as e:
    print("❌ PostgreSQL connection failed:", e)
    app.config['PG_CONN'] = None  # ป้องกันไว้ก่อน

# ---------------------------
# MQTT Message Handling
# ---------------------------

def on_message(client, userdata, msg):
    try:
        print("----------------------------"*3)
        print("📬 Received MQTT message on topic:", msg.topic)
        raw_data = json.loads(msg.payload.decode())
        print("✅ Received MQTT:", raw_data)

        conn = app.config.get('PG_CONN')
        if conn is None:
            print("❌ PostgreSQL connection is missing.")
            return

        # ➕ Handle format like:
        # data/device_id {"d":[{"tag":"wise4210:temp","value":249.00},{"tag":"wise4210:hum","value":607.00}],"ts":"2025-05-30T04:23:00Z"}
        if "d" in raw_data and "ts" in raw_data:
            device_id = msg.topic.split("/")[-1]  # extract device_id from topic
            ts = datetime.strptime(raw_data["ts"], "%Y-%m-%dT%H:%M:%SZ")

            temp = None
            hum = None

            for item in raw_data["d"]:
                tag = item.get("tag")
                value = item.get("value") / 10  # scale down
                if "temp" in tag:
                    temp = value
                elif "hum" in tag:
                    hum = value

            if temp is not None or hum is not None:
                with conn.cursor() as cursor:
                    try:
                        cursor.execute(
                            """
                            INSERT INTO iotdata.wise4210_ecu1251 (device_id, temp, hum, timestamp)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (device_id, temp, hum, ts)
                        )
                        conn.commit()
                        print("📥 Inserted row:", device_id, temp, hum, ts)
                        socketio.emit("mqtt_data", {
                            "device_id": device_id,
                            "temp": temp,
                            "hum": hum,
                            "timestamp": ts.isoformat()
                        })
                    except Exception as sql_err:
                        print("❌ SQL Error (insert):", sql_err)
                        conn.rollback()
            return

    except Exception as e:
        print("❌ Error in on_message:", e)

# ---------------------------
# MQTT Client
# ---------------------------
client = mqtt.Client(protocol=mqtt.MQTTv311)
client.username_pw_set("root", "00000000")
client.on_message = on_message
client.connect("172.21.108.87", 1883, 60)
client.subscribe([ # replace with your MQTT topics as needed
    ("data/device_id", 0)
])
client.loop_start()


# ---------------------------
# Grafana Routes
# ---------------------------
@app.route('/')
def index():
    return "✅ MQTT + PostgreSQL Gateway Running"

@app.route('/search', methods=['POST'])
def search():
    return jsonify(["runtime", "machine", "status"])

@app.route('/query', methods=['POST'])
def query():
    req = request.get_json()
    targets = req.get('targets', [])
    results = []
    conn = app.config.get('PG_CONN')
    if not conn:
        return jsonify([])

    for target in targets:
        target_name = target['target']
        datapoints = []

        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT timestamp, {target_name} FROM iot_data.wise4210_data ORDER BY timestamp DESC LIMIT 100"
            )
            for row in cursor.fetchall():
                value = row[1]
                ts = int(row[0].timestamp() * 1000)  # แปลงเป็น epoch ms
                datapoints.append([value, ts])

        results.append({
            "target": target_name,
            "datapoints": datapoints
        })

    return jsonify(results)


@app.route('/api/tpm', methods=['GET'])
def get_data():
    return jsonify(data_storage)

# ---------------------------
# Socket.IO Events
# ---------------------------
@socketio.on('connect')
def handle_connect():
    print("🌐 Client connected")

@socketio.on('disconnect')
def handle_disconnect():
    print("🔌 Client disconnected")

# ---------------------------
# Main
# ---------------------------
if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=4000)


# Note: Make sure to create the PostgreSQL table `iotdata.wise4210_ecu1251` with appropriate columns:
# CREATE TABLE iotdata.wise4210_ecu1251 (
#     device_id VARCHAR(50),
#     temp NUMERIC,
#     hum NUMERIC,          
#     timestamp TIMESTAMP,
#     PRIMARY KEY (device_id, timestamp)
# );
#