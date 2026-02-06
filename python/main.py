import asyncio
import websockets
import json
from datetime import datetime, timezone, timedelta
import database
import threading
from flask import Flask, render_template, request, jsonify
from waitress import serve
import traceback

# --- Robust Timestamp Parser ---
def parse_ais_timestamp(timestamp_str):
    """
    Robustly parses various timestamp formats from the AIS stream.
    Handles nanoseconds and different timezone formats.
    """
    try:
        # Clean up the string
        timestamp_str = timestamp_str.strip().replace(' UTC', '')

        parts = timestamp_str.split(' ')
        date_part = parts[0]
        time_part = parts[1]

        # Truncate nanoseconds to microseconds
        if '.' in time_part:
            head, tail = time_part.split('.')
            time_part = f"{head}.{tail[:6]}"

        # Reconstruct a standard ISO string
        iso_str = f"{date_part}T{time_part}"
        if len(parts) > 2 and parts[2] == '+0000':
            iso_str += "+00:00"

        return datetime.fromisoformat(iso_str)
    except Exception as e:
        # This is a fallback, but the main logic should handle the format now.
        print(f"Could not parse timestamp '{timestamp_str}': {e}. Falling back to current time.")
        return datetime.now(timezone.utc)

# --- Flask App Setup ---
app = Flask(__name__)

@app.route('/')
def index():
    """Serves the main HTML application shell."""
    return render_template('index.html')

@app.route('/api/positions')
def api_positions():
    """A fast, optimized endpoint for the main map view."""
    try:
        bounds_str = request.args.get('bounds')
        if not bounds_str: return jsonify({'error': 'Map bounds not provided.'}), 400

        bounds = [float(x) for x in bounds_str.split(',')]
        hours = int(request.args.get('hours', 24))
        search = request.args.get('search', "")

        _lat, _lon, latest_ts = database.get_latest_entry_details()
        end_time = latest_ts if latest_ts else datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=hours)

        ships = database.get_latest_positions_in_bounds(start_time, end_time, bounds, search)
        return jsonify(ships)

    except Exception as e:
        print(f"API Error in /api/positions: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/geofence', methods=['POST'])
def api_geofence():
    """Handles the geo-fencing query."""
    try:
        data = request.json
        hours = int(data.get('hours', 24))
        whitelist = data.get('whitelist', [])
        blacklist = data.get('blacklist', [])

        _lat, _lon, latest_ts = database.get_latest_entry_details()
        end_time = latest_ts if latest_ts else datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=hours)

        routes = database.get_filtered_routes(start_time, end_time, "", whitelist, blacklist)
        return jsonify(routes)
    except Exception as e:
        print(f"API Error in /api/geofence: {e}")
        return jsonify({'error': str(e)}), 500

def run_flask():
    serve(app, host='0.0.0.0', port=5000)

# --- AIS Stream Handling ---
async def connect_ais_stream():
    database.init_db()
    print("Database initialized.")
    subscription = { "APIKey": "064312c15396a338d0dc1aa723827c9c42290bb8", "BoundingBoxes": [[[-90, -180], [90, 180]]], "FilterMessageTypes": ["PositionReport", "ShipStaticData"] }

    message_counter = 0

    while True:
        try:
            async with websockets.connect("wss://stream.aisstream.io/v0/stream", ping_interval=20, ping_timeout=60) as websocket:
                print("Connected to AIS Stream.")
                await websocket.send(json.dumps(subscription))
                async for message_json in websocket:
                    try:
                        message = json.loads(message_json)
                        message_type = message["MessageType"]
                        if message_type == "ShipStaticData":
                            data = message['Message']['ShipStaticData']
                            database.upsert_ship_info(data['UserID'], data.get('Name'), data.get('IMO'), data.get('CallSign'), data.get('ShipType'))
                        elif message_type == "PositionReport":
                            data = message['Message']['PositionReport']

                            # ** THE FIX IS HERE **
                            # Using the robust parser instead of the broken one-liner.
                            timestamp = parse_ais_timestamp(message['MetaData']['time_utc'])

                            database.insert_position_report(timestamp, data['UserID'], data['Latitude'], data['Longitude'])

                        message_counter += 1
                        if message_counter % 1000 == 0:
                            print(f"Processed {message_counter} total messages.")
                    except Exception as e:
                        print(f"Error processing message: {e}")
        except Exception as e:
            print(f"Websocket connection error: {e}. Retrying in 10 seconds...")
            await asyncio.sleep(10)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    print("Web server started. Go to http://127.0.0.1:5000")
    print("Starting AIS data collection for the entire world...")
    try:
        asyncio.run(connect_ais_stream())
    except KeyboardInterrupt:
        print("Stopping...")
