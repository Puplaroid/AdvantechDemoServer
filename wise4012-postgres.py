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
        print(f"📬 Received MQTT message on topic: {msg.topic}")
        raw_data = json.loads(msg.payload.decode())
        print(f"✅ Received MQTT from {msg.topic}: {raw_data}")
        data_storage.append(raw_data)

        conn = app.config.get('PG_CONN')
        if conn is None:
            print("❌ PostgreSQL connection is missing.")
            return

        # 🟡 Check if it's a connection log payload
        if "status" in raw_data and "macid" in raw_data:
            log_insert = {
                "status": raw_data.get("status"),
                "name": raw_data.get("name"),
                "macid": raw_data.get("macid"),
                "ipaddr": raw_data.get("ipaddr"),
                "timestamp": datetime.utcnow()
            }

            with conn.cursor() as cursor:
                try:
                    cursor.execute(
                        """
                        INSERT INTO iotdata.wise4012_connection_log (status, name, macid, ipaddr, timestamp)
                        VALUES (%(status)s, %(name)s, %(macid)s, %(ipaddr)s, %(timestamp)s)
                        """,
                        log_insert
                    )
                    conn.commit()
                    print(f"📥 Inserted wise4012_connection_log: {log_insert}")
                    socketio.emit("wise4012_connection_log", log_insert)
                except Exception as log_err:
                    print("❌ SQL Error (wise4012_connection_log):", log_err)
                    conn.rollback()
            return  # ✅ don’t proceed to insert sensor data
        

        # Choose table based on topic
        if msg.topic == "wise4012_8C8046":
            table_name = "iotdata.wise4012_8C8046"
        elif msg.topic == "wise4012_FEEAB5":
            table_name = "iotdata.wise4012_FEEAB5"
        else:
            print(f"⚠️ Unknown topic: {msg.topic}, ignoring...")
            return

        # Prepare data (match your new table structure)
        if "t" in raw_data and raw_data["t"]:
            timestamp = datetime.strptime(raw_data["t"], "%Y-%m-%dT%H:%M:%SZ")
        else:
            timestamp = datetime.utcnow()

        insert_data = {
            "time": timestamp,
            "s": raw_data.get("s", 0),
            "q": raw_data.get("q", 0),
            "c": raw_data.get("c", 0),
            "di1": raw_data.get("di1", False),
            "di2": raw_data.get("di2", False),
            "di3": raw_data.get("di3", False),
            "di4": raw_data.get("di4", False),
            "do1": raw_data.get("do1", False),
            "do2": raw_data.get("do2", False)
        }

        with conn.cursor() as cursor:
            try:
                cursor.execute(
                    f"""
                    INSERT INTO {table_name} (
                        time, s, q, c,
                        di1, di2, di3, di4,
                        do1, do2
                    ) VALUES (
                        %(time)s, %(s)s, %(q)s, %(c)s,
                        %(di1)s, %(di2)s, %(di3)s, %(di4)s,
                        %(do1)s, %(do2)s
                    )
                    """,
                    insert_data
                )
                conn.commit()
                print(f"📥 Inserted into {table_name}: {insert_data}")
                socketio.emit("mqtt_data", raw_data)
            except Exception as sql_err:
                print(f"❌ SQL Error inserting into {table_name}:", sql_err)
                conn.rollback()

    except Exception as e:
        print("❌ Error in on_message:", e)

# ---------------------------
# MQTT Client
# ---------------------------
client = mqtt.Client(protocol=mqtt.MQTTv311)
client.on_message = on_message
client.connect("172.21.108.81", 1883, 60)  # replace with your broker IP
client.subscribe([ # replace with your MQTT topics as needed
    ("wise4012_FEEAB5", 0),
    ("wise4012_8C8046", 0),
    ("Advantech/74FE488C8046/Device_Status", 0),
    ("Advantech/00D0C9FEEAB5/Device_Status", 0),
])
client.loop_start()

# ---------------------------
# Flask Routes
# ---------------------------
@app.route('/')
def index():
    return "✅ MQTT + HTTP API Gateway Running"

@app.route('/api/data', methods=['GET'])
def get_data():
    return jsonify(data_storage)

# WISE-4012 HTTP push endpoint
@app.route('/io_log', methods=['POST'])
def receive_io_log():
    try:
        data = request.get_json()
        print("📥 Received /io_log POST:", data)
        data_storage.append({"source": "io_log", "data": data, "timestamp": datetime.utcnow().isoformat()})
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        print("❌ Error in /io_log:", e)
        return jsonify({"status": "error", "message": str(e)}), 500

# Optional WISE-4012 System Event logging
@app.route('/sys_log', methods=['POST'])
def receive_sys_log():
    try:
        data = request.get_json()
        print("📥 Received /sys_log POST:", data)
        data_storage.append({"source": "sys_log", "data": data, "timestamp": datetime.utcnow().isoformat()})
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        print("❌ Error in /sys_log:", e)
        return jsonify({"status": "error", "message": str(e)}), 500

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
# Main Entry Point
# ---------------------------
if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=4001)


# Note: Make sure to create the PostgreSQL "iotdata.wise4012_8C8046" and "iotdata.wise4012_FEEAB5" tables with appropriate columns:
# CREATE TABLE iotdata.wise4012_8C8046 (
#     time TIMESTAMP,
#     s INTEGER,                    
#     q INTEGER,
#     c INTEGER,
#     di1 BOOLEAN,
#     di2 BOOLEAN,
#     di3 BOOLEAN,
#     di4 BOOLEAN,
#     do1 BOOLEAN,
#     do2 BOOLEAN
# );
# CREATE TABLE iotdata.wise4012_FEEAB5 (
#     time TIMESTAMP,
#     s INTEGER,
#     q INTEGER,
#     c INTEGER,
#     di1 BOOLEAN,
#     di2 BOOLEAN,
#     di3 BOOLEAN,
#     di4 BOOLEAN,
#     do1 BOOLEAN,
#     do2 BOOLEAN
# );
# Note: Make sure to create the PostgreSQL table `iotdata.wise4012_connection_log` with appropriate columns:
# CREATE TABLE iotdata.wise4012_connection_log (
#     status VARCHAR(50),
#     name VARCHAR(100),
#     macid VARCHAR(50),
#     ipaddr VARCHAR(50),
#     timestamp TIMESTAMP
# );
