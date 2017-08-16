from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from tethys_sdk.gizmos import MapView, MVView, MVLayer, DataTableView

from .model import get_all_sensors

@login_required()
def home(request):
    """
    Controller for the app home page.
    """
    # Get list of sensors and create sensors MVLayer:
    sensors = get_all_sensors()
    features = []
    lat_list = []
    lng_list = []

    for sensor in sensors:
        lat_list.append(sensor.latitude)
        lng_list.append(sensor.longitude)

        sensor_feature = {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [sensor.longitude, sensor.latitude]
            },
            'properties': {
                'id': sensor.id,
                'timest': sensor.updatets
            }
        }
        features.append(sensor_feature)

    # Define GeoJSON FeatureCollection
    sensors_feature_collection = {
        'type': 'FeatureCollection',
        'crs': {
            'type': 'name',
            'properties': {
                'name': 'EPSG:4326'
            }
        },
        'features': features
    }

    # Create a Map View Layer
    sensors_layer = MVLayer(
        source='GeoJSON',
        options=sensors_feature_collection,
        legend_title='Sensors',
        layer_options={
            'style': {
                'image': {
                    'circle': {
                        'radius': 10,
                        'fill': {'color':  '#d84e1f'},
                        'stroke': {'color': '#ffffff', 'width': 1},
                    }
                }
            }
        },
        feature_selection=True
    )


    # Define view centered on sensor locations
    try:
        view_center = [sum(lng_list) / float(len(lng_list)), sum(lat_list) / float(len(lat_list))]
    except ZeroDivisionError:
        view_center = [-98.6, 39.8]

    view_options = MVView(
        projection='EPSG:4326',
        center=view_center,
        zoom=4.5,
        maxZoom=18,
        minZoom=2
    )

    sensor_map = MapView(
        height='100%',
        width='100%',
        layers=[sensors_layer],
        basemap='OpenStreetMap',
        view=view_options
    )

    context = {
        'sensor_map': sensor_map,
    }

    return render(request, 'open_air/home.html', context)

@login_required()
def list_sensors(request):
    """
    Show all Sensors in a table
    """
    sensors = get_all_sensors()
    table_rows = []

    for sensor in sensors:
        table_rows.append(
            (
                sensor.id, sensor.longitude, sensor.latitude
            )
        )

    sensor_table = DataTableView(
        column_names = ('id', 'latitude', 'longitude'),
        rows = table_rows,
        searching = False,
        orderClasses = False,
        lengthMenu=[ [10, 25, 50, -1], [10, 25, 50, "All"] ]
    )

    context = {
        'sensor_table' : sensor_table
    }

    return render(request, 'open_air/list_sensors.html', context)
