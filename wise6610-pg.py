from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit
import paho.mqtt.client as mqtt
import psycopg2
import json
from datetime import datetime, timezone, timedelta
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

# ตรวจสอบว่าเป็นข้อมูลจาก RtuRegister หรือไม่
def is_valid_data(data):
    return (
        "RtuRegister0-0" in data and
        "RtuRegister0-1" in data and
        "Device" in data and
        "Time" in data["Device"] and
        "Data" in data["RtuRegister0-0"] and
        "Data" in data["RtuRegister0-1"]
    )

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
    app.config['PG_CONN'] = None

# ---------------------------
# MQTT Message Handling
# ---------------------------
latest_signal_info = {
    "rssi": None,
    "devaddr": None,
    "timestamp": None
}
def on_message(client, userdata, msg):
    try:
        raw_data = json.loads(msg.payload.decode())
        print("✅ Received MQTT:", raw_data)
        data_storage.append(raw_data)

        # อัปเดตค่า rssi/devaddr/timestamp ถ้ามีในข้อมูล (เก็บไว้ใช้ตอน future RtuRegister มา)
        if "rssi" in raw_data:
            latest_signal_info["rssi"] = raw_data["rssi"]
        if "devaddr" in raw_data:
            latest_signal_info["devaddr"] = raw_data["devaddr"]
        if "datetime" in raw_data:
            latest_signal_info["timestamp"] = raw_data["datetime"]

        # ตรวจว่าเป็นข้อมูลที่เราต้องการ insert หรือไม่
        if not is_valid_data(raw_data):
            print("⚠️ Skipped non-matching MQTT data.")
            return

        conn = app.config.get('PG_CONN')
        if conn is None:
            print("❌ PostgreSQL connection is missing.")
            return

        insert_data = {
            "temp": float(raw_data["RtuRegister0-0"]["Data"])/10,
            "temp_status": raw_data["RtuRegister0-0"]["Status"],
            "humidity": float(raw_data["RtuRegister0-1"]["Data"])/10,
            "humidity_status": raw_data["RtuRegister0-1"]["Status"],
            "rssi": latest_signal_info["rssi"],         # ใช้ค่าล่าสุดที่จำไว้
            "devaddr": latest_signal_info["devaddr"],   # ใช้ค่าล่าสุดที่จำไว้
            "timestamp": datetime.fromtimestamp(raw_data["Device"]["Time"], timezone(timedelta(hours=7))),
        }

        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO iotdata.wise2200_data (
                    temp, temp_status, humidity, humidity_status,
                    rssi, devaddr, timestamp
                ) VALUES (
                    %(temp)s, %(temp_status)s, %(humidity)s, %(humidity_status)s,
                    %(rssi)s, %(devaddr)s, %(timestamp)s
                )
                """,
                insert_data
            )
            conn.commit()
            print("📥 Inserted data:", insert_data)
            socketio.emit("mqtt_data", insert_data)

    except Exception as e:
        print("❌ Error in on_message:", e)

# ---------------------------
# MQTT Client Setup
# ---------------------------
client = mqtt.Client(protocol=mqtt.MQTTv311)
client.on_message = on_message
client.connect("172.21.108.81", 1883, 60)  # replace with your broker IP
client.subscribe("#")
client.loop_start()

# ---------------------------
# Grafana-compatible API
# ---------------------------
@app.route('/')
def index():
    return "✅ MQTT + PostgreSQL Gateway Running"

@app.route('/search', methods=['POST'])
def search():
    return jsonify(["temp", "humidity", "rssi"])

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
                f"""
                SELECT timestamp, {target_name}
                FROM iotdata.wise2200_data
                WHERE {target_name} IS NOT NULL
                ORDER BY timestamp DESC
                LIMIT 100
                """
            )
            for row in cursor.fetchall():
                value = row[1]
                ts = int(row[0].timestamp() * 1000)
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