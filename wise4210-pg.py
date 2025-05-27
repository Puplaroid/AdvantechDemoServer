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
latest_io = {
    "s": 0, "c": 0, "q": 0, "rssi": 0,
    "di1": 0, "di2": 0, "di3": 0, "di4": 0, "di5": 0, "di6": 0,
    "do1": 0, "do2": 0,
    "timestamp": None
}

def on_message(client, userdata, msg):
    try:
        raw_data = json.loads(msg.payload.decode())
        print("✅ Received MQTT:", raw_data)

        conn = app.config.get('PG_CONN')
        if conn is None:
            print("❌ PostgreSQL connection is missing.")
            return

        # check log connection
        if "status" in raw_data and "macid" in raw_data:
            log_insert = {
                "status": raw_data.get("status"),
                "name": raw_data.get("name"),
                "macid": raw_data.get("macid"),
                "ipaddr": raw_data.get("ipaddr"),
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            }
            with conn.cursor() as cursor:
                try:
                    cursor.execute(
                        """
                        INSERT INTO iotdata.connection_log (status, name, macid, ipaddr, timestamp)
                        VALUES (%(status)s, %(name)s, %(macid)s, %(ipaddr)s, %(timestamp)s)
                        """,
                        log_insert
                    )
                    conn.commit()
                    print("📥 Inserted connection_log:", log_insert)
                    socketio.emit("connection_log", log_insert)
                except Exception as log_err:
                    print("❌ SQL Error (log):", log_err)
                    conn.rollback()
            return

        timestamp = datetime.strptime(raw_data["t"], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")

        # ถ้ามี I/O → อัปเดต latest_io
        if any(k in raw_data for k in ["di1", "di2", "di3", "di4", "di5", "di6", "do1", "do2"]):
            latest_io.update({
                "s": raw_data.get("s", 0),
                "c": raw_data.get("c", 0),
                "q": raw_data.get("q", 0),
                "rssi": raw_data.get("rssi"),
                "di1": int(bool(raw_data.get("di1", 0))),
                "di2": int(bool(raw_data.get("di2", 0))),
                "di3": int(bool(raw_data.get("di3", 0))),
                "di4": int(bool(raw_data.get("di4", 0))),
                "di5": int(bool(raw_data.get("di5", 0))),
                "di6": int(bool(raw_data.get("di6", 0))),
                "do1": int(bool(raw_data.get("do1", 0))),
                "do2": int(bool(raw_data.get("do2", 0))),
                "timestamp": timestamp
            })
            print("📌 Cached I/O:", latest_io)
            return  # ยังไม่ insert จนกว่า temp/hum จะมา

        # ถ้ามี temp/humidity → รวมกับ I/O แล้ว insert
        if "p1v00r0000x00" in raw_data and "p1v00r0000x01" in raw_data:
            insert_data = {
                **latest_io,
                "timestamp": timestamp,  # ใช้ timestamp ปัจจุบันจาก temp/hum
                "temp": float(raw_data.get("p1v00r0000x00", 0)) / 10,
                "humidity": float(raw_data.get("p1v00r0000x01", 0)) / 10
            }

            with conn.cursor() as cursor:
                try:
                    cursor.execute(
                        """
                        INSERT INTO iotdata.wise4210_data (
                            s, c, q, rssi,
                            di1, di2, di3, di4, di5, di6,
                            do1, do2, timestamp, temp, humidity
                        ) VALUES (
                            %(s)s, %(c)s, %(q)s, %(rssi)s,
                            %(di1)s, %(di2)s, %(di3)s, %(di4)s, %(di5)s, %(di6)s,
                            %(do1)s, %(do2)s, %(timestamp)s, %(temp)s, %(humidity)s
                        )
                        """,
                        insert_data
                    )
                    conn.commit()
                    print("📥 Inserted combined I/O + Temp/Humidity:", insert_data)
                    socketio.emit("mqtt_data", insert_data)
                except Exception as sql_err:
                    print("❌ SQL Error:", sql_err)
                    conn.rollback()

    except Exception as e:
        print("❌ Error in on_message:", e)


# ---------------------------
# MQTT Client
# ---------------------------
client = mqtt.Client(protocol=mqtt.MQTTv311)
client.username_pw_set("root", "00000000")
client.on_message = on_message
client.connect("172.21.108.81", 1883, 60)  # replace with your broker IP
client.subscribe("#")
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