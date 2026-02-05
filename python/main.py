import asyncio
import websockets
import json
from datetime import datetime, timezone, timedelta
from database import insert_position_report, init_db, get_ship_route, get_latest_entry_details, get_recent_routes_in_bounds, get_all_last_updates
import threading
from flask import Flask, render_template, request, jsonify
from waitress import serve
import traceback

# --- Flask App Setup ---
app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/positions')
def api_positions():
    try:
        bounds_str = request.args.get('bounds')
        if not bounds_str:
            return jsonify({'error': 'Map bounds not provided.'}), 400

        bounds = [float(x) for x in bounds_str.split(',')]

        time_range_hours = int(request.args.get('hours', 24))

        _lat, _lon, latest_timestamp = get_latest_entry_details()
        if not latest_timestamp:
            return jsonify([])

        end_time = latest_timestamp
        start_time = end_time - timedelta(hours=time_range_hours)

        data = get_recent_routes_in_bounds(start_time, end_time, bounds)

        return jsonify(data)
    except Exception as e:
        print(f"API Error: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/route/<int:user_id>')
def route_view(user_id):
    import folium
    route = get_ship_route(user_id)
    m = folium.Map(location=route[0] if route else [50, 10], zoom_start=6)
    if route:
        folium.PolyLine(route, color="blue", weight=2.5, opacity=1).add_to(m)
        folium.Marker(route[0], popup="Start", icon=folium.Icon(color='green')).add_to(m)
        folium.Marker(route[-1], popup="End", icon=folium.Icon(color='red')).add_to(m)
    return m._repr_html_()

def run_flask():
    serve(app, host='0.0.0.0', port=5000)

# --- AIS Stream Handling ---

def parse_timestamp(timestamp_str):
    try:
        if timestamp_str.endswith(' UTC'):
            timestamp_str = timestamp_str[:-4]

        parts = timestamp_str.split(' ')
        if len(parts) >= 3:
            date_part = parts[0]
            time_part = parts[1]
            tz_part = parts[2]

            if '.' in time_part:
                head, tail = time_part.split('.')
                if len(tail) > 6:
                    time_part = f"{head}.{tail[:6]}"

            iso_str = f"{date_part}T{time_part}{tz_part}"
            return datetime.fromisoformat(iso_str)

        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except Exception as e:
        print(f"Error parsing timestamp '{timestamp_str}': {e}")
        return datetime.now(timezone.utc)

async def connect_ais_stream():
    init_db()

    # Initialize the rate-limiter from the database
    print("Initializing rate-limiter from database...")
    last_updates = get_all_last_updates()
    print(f"Rate-limiter initialized for {len(last_updates)} ships.")

    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
        subscribe_message = {
            "APIKey": "064312c15396a338d0dc1aa723827c9c42290bb8",
            "BoundingBoxes": [[[30, -25], [72, 45]]]
        }

        subscribe_message_json = json.dumps(subscribe_message)
        await websocket.send(subscribe_message_json)

        async for message_json in websocket:
            try:
                message = json.loads(message_json)

                if message.get("MessageType") == "PositionReport":
                    meta_data = message.get('MetaData')
                    ais_message = message['Message']['PositionReport']
                    user_id = ais_message['UserID']

                    timestamp = datetime.now(timezone.utc)
                    if meta_data and 'time_utc' in meta_data:
                        timestamp = parse_timestamp(meta_data['time_utc'])

                    # Rate Limiting
                    if user_id in last_updates:
                        last_time = last_updates[user_id]
                        if (timestamp - last_time).total_seconds() < 180:
                            continue

                    try:
                        insert_position_report(timestamp, user_id, ais_message['Latitude'], ais_message['Longitude'])
                        last_updates[user_id] = timestamp
                        print(f"[{timestamp}] ShipId: {user_id} Latitude: {ais_message['Latitude']} Longitude: {ais_message['Longitude']}")
                    except Exception as db_err:
                        print(f"Database error: {db_err}")

            except Exception as e:
                print(f"Error processing message: {e}")
                await asyncio.sleep(1)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()

    print("Web server started. Go to http://127.0.0.1:5000")
    print("Starting AIS data collection for Europe (Rate limited: 1 update per 3 min per ship)...")

    try:
        asyncio.run(connect_ais_stream())
    except KeyboardInterrupt:
        print("Stopping...")
