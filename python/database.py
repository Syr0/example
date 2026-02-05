import sqlite3
from datetime import datetime
import math

def get_db_connection():
    conn = sqlite3.connect('ais_data.db', timeout=30)
    conn.execute('PRAGMA journal_mode=WAL;')
    return conn

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS position_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME NOT NULL,
            UserID INTEGER NOT NULL,
            Latitude REAL NOT NULL,
            Longitude REAL NOT NULL,
            UNIQUE(timestamp, UserID)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON position_reports(timestamp);')
    c.execute('CREATE INDEX IF NOT EXISTS idx_userid_timestamp ON position_reports(UserID, timestamp DESC);')
    conn.commit()
    conn.close()

def get_all_last_updates():
    """
    Gets the most recent timestamp for every ship in the database.
    Used to initialize the rate-limiter on startup.
    """
    conn = get_db_connection()
    try:
        c = conn.cursor()
        c.execute('SELECT UserID, MAX(timestamp) FROM position_reports GROUP BY UserID')
        rows = c.fetchall()
        # Convert list of tuples to a dictionary and parse timestamps
        return {row[0]: datetime.fromisoformat(row[1]) for row in rows}
    finally:
        conn.close()

def insert_position_report(timestamp, user_id, latitude, longitude):
    conn = get_db_connection()
    try:
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO position_reports (timestamp, UserID, Latitude, Longitude) VALUES (?, ?, ?, ?)",
                  (timestamp, user_id, latitude, longitude))
        conn.commit()
    finally:
        conn.close()

def calculate_bearing(lat1, lon1, lat2, lon2):
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    diff_lon_rad = math.radians(lon2 - lon1)
    y = math.sin(diff_lon_rad) * math.cos(lat2_rad)
    x = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(diff_lon_rad)
    bearing_rad = math.atan2(y, x)
    return (math.degrees(bearing_rad) + 360) % 360

def get_recent_routes_in_bounds(start_time, end_time, bounds):
    conn = get_db_connection()
    sw_lat, sw_lon, ne_lat, ne_lon = bounds

    try:
        c = conn.cursor()
        query = '''
            WITH ShipsInView AS (
                SELECT
                    UserID
                FROM (
                    SELECT
                        UserID, Latitude, Longitude,
                        ROW_NUMBER() OVER(PARTITION BY UserID ORDER BY timestamp DESC) as rn
                    FROM position_reports
                    WHERE timestamp BETWEEN ? AND ?
                )
                WHERE rn = 1 AND Latitude BETWEEN ? AND ? AND Longitude BETWEEN ? AND ?
            ),
            RecentTrails AS (
                SELECT
                    UserID, Latitude, Longitude, timestamp,
                    ROW_NUMBER() OVER(PARTITION BY UserID ORDER BY timestamp DESC) as rn
                FROM position_reports
                WHERE UserID IN (SELECT UserID FROM ShipsInView) AND timestamp <= ?
            )
            SELECT UserID, Latitude, Longitude, timestamp FROM RecentTrails WHERE rn <= 10 ORDER BY UserID, timestamp ASC;
        '''
        c.execute(query, (start_time, end_time, sw_lat, ne_lat, sw_lon, ne_lon, end_time))
        rows = c.fetchall()
    finally:
        conn.close()

    routes = {}
    for user_id, lat, lon, ts in rows:
        if user_id not in routes:
            routes[user_id] = []
        routes[user_id].append({'lat': lat, 'lon': lon, 'ts': ts})

    result = []
    for user_id, trail in routes.items():
        if len(trail) > 0:
            latest_point = trail[-1]
            heading = 0
            if len(trail) > 1:
                prev_point = trail[-2]
                heading = calculate_bearing(prev_point['lat'], prev_point['lon'], latest_point['lat'], latest_point['lon'])

            result.append({
                'id': user_id,
                'lat': latest_point['lat'],
                'lon': latest_point['lon'],
                'ts': str(latest_point['ts']),
                'heading': heading,
                'trail': [[p['lat'], p['lon']] for p in trail]
            })

    return result

def get_ship_route(user_id):
    conn = get_db_connection()
    try:
        c = conn.cursor()
        c.execute("SELECT Latitude, Longitude FROM position_reports WHERE UserID = ? ORDER BY timestamp", (user_id,))
        route = c.fetchall()
    finally:
        conn.close()
    return route

def get_latest_entry_details():
    conn = get_db_connection()
    try:
        c = conn.cursor()
        c.execute("SELECT Latitude, Longitude, timestamp FROM position_reports ORDER BY timestamp DESC LIMIT 1")
        result = c.fetchone()
    finally:
        conn.close()
    if result:
        return result[0], result[1], datetime.fromisoformat(result[2])
    return None, None, None
