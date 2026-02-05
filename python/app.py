from flask import Flask, render_template, request
from database import get_positions_in_range, get_ship_route, init_db, get_latest_entry_details
import folium
from datetime import timedelta

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/map')
def map_view():
    # Get the latest data point to center the map and define the time range
    latest_lat, latest_lon, latest_timestamp = get_latest_entry_details()

    # If there's no data, default to a wide view of Europe
    if not latest_timestamp:
        return folium.Map(location=[50, 10], zoom_start=4)._repr_html_()

    # Default to a 1-hour window before the latest data point
    end_time = latest_timestamp
    start_time = end_time - timedelta(hours=1)

    # Allow user to override the time range
    if 'start' in request.args and 'end' in request.args and request.args['start'] and request.args['end']:
        start_time = datetime.fromisoformat(request.args['start'])
        end_time = datetime.fromisoformat(request.args['end'])

    positions = get_positions_in_range(start_time, end_time)

    # Center the map on the latest known position
    m = folium.Map(location=[latest_lat, latest_lon], zoom_start=8)

    for user_id, lat, lon in positions:
        folium.Marker(
            [lat, lon],
            popup=f'<a href="/route/{user_id}" target="_blank">Ship {user_id}</a>'
        ).add_to(m)

    return m._repr_html_()

@app.route('/route/<int:user_id>')
def route_view(user_id):
    route = get_ship_route(user_id)
    # Center the map on the first point of the route
    m = folium.Map(location=route[0] if route else [50, 10], zoom_start=10)
    if route:
        folium.PolyLine(route, color="blue", weight=2.5, opacity=1).add_to(m)
        for point in route:
            folium.CircleMarker(point, radius=3, color='red').add_to(m)
    return m._repr_html_()

if __name__ == '__main__':
    init_db()
    app.run(debug=True)
