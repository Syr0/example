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
    c.execute('CREATE TABLE IF NOT EXISTS ships (UserID INTEGER PRIMARY KEY, Name TEXT, IMO INTEGER, CallSign TEXT, ShipType INTEGER)')
    c.execute('''
        CREATE TABLE IF NOT EXISTS position_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME NOT NULL,
            ShipID INTEGER NOT NULL,
            Latitude REAL NOT NULL,
            Longitude REAL NOT NULL,
            FOREIGN KEY(ShipID) REFERENCES ships(UserID),
            UNIQUE(timestamp, ShipID)
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_pos_timestamp ON position_reports(timestamp);')
    c.execute('CREATE INDEX IF NOT EXISTS idx_pos_shipid_timestamp ON position_reports(ShipID, timestamp DESC);')
    conn.commit()
    conn.close()

def upsert_ship_info(user_id, name, imo, callsign, ship_type):
    conn = get_db_connection()
    try:
        c = conn.cursor()
        c.execute('''
            INSERT INTO ships (UserID, Name, IMO, CallSign, ShipType) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(UserID) DO UPDATE SET
                Name=excluded.Name, IMO=excluded.IMO, CallSign=excluded.CallSign, ShipType=excluded.ShipType
            WHERE excluded.Name IS NOT NULL OR excluded.IMO IS NOT NULL;
        ''', (user_id, name, imo, callsign, ship_type))
        conn.commit()
    finally:
        conn.close()

def insert_position_report(timestamp, ship_id, latitude, longitude):
    conn = get_db_connection()
    try:
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO position_reports (timestamp, ShipID, Latitude, Longitude) VALUES (?, ?, ?, ?)",
                  (timestamp, ship_id, latitude, longitude))
        conn.commit()
    finally:
        conn.close()

def calculate_bearing(lat1, lon1, lat2, lon2):
    lat1_rad, lat2_rad, diff_lon_rad = map(math.radians, [lat1, lat2, lon2 - lon1])
    y = math.sin(diff_lon_rad) * math.cos(lat2_rad)
    x = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(diff_lon_rad)
    return (math.degrees(math.atan2(y, x)) + 360) % 360

def get_latest_positions_in_bounds(start_time, end_time, bounds, search_term=""):
    conn = get_db_connection()
    sw_lat, sw_lon, ne_lat, ne_lon = bounds
    try:
        c = conn.cursor()
        search_clause = ""
        params = [start_time, end_time, sw_lat, ne_lat, sw_lon, ne_lon, end_time]
        if search_term:
            search_clause = "AND (s.Name LIKE ? OR s.UserID LIKE ?)"
            params.extend([f'%{search_term}%', f'%{search_term}%'])

        # ** THE FIX IS HERE **
        # The query now correctly includes `pr.timestamp` in all relevant parts.
        query = f'''
            WITH ShipsInView AS (
                SELECT s.UserID
                FROM position_reports pr JOIN ships s ON pr.ShipID = s.UserID
                WHERE pr.id IN (SELECT id FROM position_reports WHERE (ShipID, timestamp) IN (SELECT ShipID, MAX(timestamp) FROM position_reports GROUP BY ShipID))
                AND pr.timestamp BETWEEN ? AND ? AND pr.Latitude BETWEEN ? AND ? AND pr.Longitude BETWEEN ? AND ? {search_clause}
            ),
            RecentTrails AS (
                SELECT pr.ShipID, pr.Latitude, pr.Longitude, pr.timestamp, s.Name,
                       ROW_NUMBER() OVER(PARTITION BY pr.ShipID ORDER BY pr.timestamp DESC) as rn
                FROM position_reports pr JOIN ships s ON pr.ShipID = s.UserID
                WHERE pr.ShipID IN (SELECT UserID FROM ShipsInView) AND pr.timestamp <= ?
            )
            SELECT ShipID, Name, Latitude, Longitude, timestamp FROM RecentTrails WHERE rn <= 10 ORDER BY ShipID, timestamp ASC;
        '''
        c.execute(query, params)
        rows = c.fetchall()
    finally:
        conn.close()

    routes = {}
    for ship_id, name, lat, lon, ts in rows:
        if ship_id not in routes: routes[ship_id] = {'name': name, 'trail_points': []}
        routes[ship_id]['trail_points'].append({'lat': lat, 'lon': lon, 'ts': ts})

    result = []
    for ship_id, data in routes.items():
        trail = data['trail_points']
        if len(trail) > 0:
            latest_point = trail[-1]
            heading = 0
            if len(trail) > 1:
                prev_point = trail[-2]
                heading = calculate_bearing(prev_point['lat'], prev_point['lon'], latest_point['lat'], latest_point['lon'])

            result.append({
                'id': ship_id,
                'name': data['name'],
                'lat': latest_point['lat'],
                'lon': latest_point['lon'],
                'ts': str(latest_point['ts']),
                'heading': heading,
                'trail': [[p['lat'], p['lon']] for p in trail]
            })
    return result

def get_filtered_routes(start_time, end_time, search_term="", whitelist_zones=[], blacklist_zones=[]):
    # This function is for the geo-fence feature and is already correct.
    # ... (code remains the same)
    pass

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
# Full get_filtered_routes for completeness
def get_filtered_routes(start_time, end_time, search_term="", whitelist_zones=[], blacklist_zones=[]):
    conn = get_db_connection()
    try:
        c = conn.cursor()
        blacklisted_ship_ids = set()
        if blacklist_zones:
            for zone in blacklist_zones:
                sw_lat, sw_lon, ne_lat, ne_lon = zone['bounds']
                c.execute('SELECT DISTINCT ShipID FROM position_reports WHERE Latitude BETWEEN ? AND ? AND Longitude BETWEEN ? AND ?', (sw_lat, ne_lat, sw_lon, ne_lon))
                for row in c.fetchall(): blacklisted_ship_ids.add(row[0])
        whitelisted_ship_ids = set()
        if whitelist_zones:
            zone_conditions = []
            params = []
            for zone in whitelist_zones:
                sw_lat, sw_lon, ne_lat, ne_lon = zone['bounds']
                zone_conditions.append("SUM(CASE WHEN Latitude BETWEEN ? AND ? AND Longitude BETWEEN ? AND ? THEN 1 ELSE 0 END) > 0")
                params.extend([sw_lat, ne_lat, sw_lon, ne_lon])
            query = f"SELECT ShipID FROM position_reports GROUP BY ShipID HAVING {' AND '.join(zone_conditions)}"
            c.execute(query, params)
            whitelisted_ship_ids = {row[0] for row in c.fetchall()}
        final_ship_ids = None
        if whitelist_zones:
            final_ship_ids = whitelisted_ship_ids
        if final_ship_ids is not None:
            final_ship_ids -= blacklisted_ship_ids
        else:
            c.execute("SELECT DISTINCT UserID FROM ships")
            all_ship_ids = {row[0] for row in c.fetchall()}
            final_ship_ids = all_ship_ids - blacklisted_ship_ids
        if not final_ship_ids: return []
        if search_term:
            c.execute(f"SELECT UserID FROM ships WHERE UserID IN ({','.join('?' for _ in final_ship_ids)}) AND (Name LIKE ? OR UserID LIKE ?)", list(final_ship_ids) + [f'%{search_term}%', f'%{search_term}%'])
            final_ship_ids = {row[0] for row in c.fetchall()}
        if not final_ship_ids: return []
        c.execute(f"SELECT pr.ShipID, s.Name, pr.Latitude, pr.Longitude FROM position_reports pr JOIN ships s ON pr.ShipID = s.UserID WHERE pr.ShipID IN ({','.join('?' for _ in final_ship_ids)}) AND pr.timestamp BETWEEN ? AND ? ORDER BY pr.ShipID, pr.timestamp ASC", list(final_ship_ids) + [start_time, end_time])
        rows = c.fetchall()
    finally:
        conn.close()
    routes = {}
    for ship_id, name, lat, lon in rows:
        if ship_id not in routes: routes[ship_id] = {'name': name, 'trail': []}
        routes[ship_id]['trail'].append([lat, lon])
    return [{'id': ship_id, 'name': data['name'], 'trail': data['trail']} for ship_id, data in routes.items()]
