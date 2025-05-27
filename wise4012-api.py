from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit
import paho.mqtt.client as mqtt
import json
from datetime import datetime

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins='*')  # allow all origins

# ใช้เก็บข้อมูลจาก MQTT และ HTTP POST
data_storage = []

# แปลง timestamp ให้อยู่ในรูปแบบ epoch milliseconds
def to_epoch_ms(timestamp_str):
    dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    return int(dt.timestamp() * 1000)

# ---------------------------
# MQTT Message Handler
# ---------------------------
def on_message(client, userdata, msg):
    try:
        raw_data = json.loads(msg.payload.decode())
        print(f"✅ Received MQTT from {msg.topic}: {raw_data}")
        data_storage.append(raw_data)
        socketio.emit("mqtt_data", raw_data)
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

@app.route('/feeab5/ai', methods=['GET'])
def get_feeab5_ai():
    ai_data = []
    for item in data_storage:
        if isinstance(item, dict):
            payload = item.get("data") if item.get("source") == "io_log" else item

            # เช็คว่า ai3 และ ai_st3 มีอยู่
            if "ai3" in payload and "ai_st3" in payload:
                ai_data.append({
                    "ai3": payload["ai3"],
                    "ai_st3": payload["ai_st3"],
                    "timestamp": payload.get("t", item.get("timestamp"))
                })

    return jsonify(ai_data)


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
    socketio.run(app, host='0.0.0.0', port=4000)
