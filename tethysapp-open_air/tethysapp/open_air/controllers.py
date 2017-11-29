from django.shortcuts import render, reverse, redirect
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from tethys_sdk.gizmos import MapView, MVView, MVLayer, DataTableView, TextInput, Button

from .model import get_all_sensors, Sensor
from .model import update_sensor as updatesensor
from .app import OpenAir as app
from .helpers import create_temperature_graph


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

    if sensors is not None:
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
                    'timest': sensor.updatets,
                    'latitude': sensor.latitude,
                    'longitude': sensor.longitude
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
                        'radius': 8,
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

    if sensors is not None:
        for sensor in sensors:
            table_rows.append(
                (
                    sensor.id, sensor.longitude, sensor.latitude, sensor.updatets
                )
            )

    sensor_table = DataTableView(
        column_names = ('id', 'latitude', 'longitude', 'update time'),
        rows = table_rows,
        searching = False,
        orderClasses = False,
        lengthMenu=[ [10, 25, 50, -1], [10, 25, 50, "All"] ]
    )

    context = {
        'sensor_table' : sensor_table
    }

    return render(request, 'open_air/list_sensors.html', context)

@login_required()
def update_sensor(request):
    """
    Controller for Update Sensor page
    """
    # Get sensors from database
    Session = app.get_persistent_store_database('sensor_db', as_sessionmaker=True)
    session = Session()
    all_sensors = session.query(Sensor).all()

    # Defaults
    selected_sensor = None

    # Errors
    sensor_select_errors = ''

    if request.POST and 'update-button' in request.POST:
        has_errors = False
        selected_sensor = request.POST.get('sensor-select', None)

        if not selected_sensor:
            has_errors = True
            sensor_select_errors = 'Sensor is required.'

        if not has_errors:
            success = updatesensor(selected_sensor)

            # Provide feedback
            if success:
                messages.info(request, 'Successfully updated sensor')
            else:
                messages.info(request, 'Unable to update sensor')
            return redirect(reverse('open_air:home'))

        messages.error(request, "Please fix errors")

    sensor_select_input = TextInput(
        display_text='Sensor',
        name='sensor-select',
        initial=selected_sensor,
        error=sensor_select_errors
    )

    update_button = Button(
        display_text='Update',
        name='update-button',
        icon='glyphicon glyphicon-plus',
        style='success',
        attributes={'form': 'update-sensor-form'},
        submit=True
    )

    cancel_button = Button(
        display_text='Cancel',
        name='cancel-button',
        href=reverse('open_air:home')
    )

    context = {
        'sensor_select_input': sensor_select_input,
        'update_button': update_button,
        'cancel_button': cancel_button,
    }

    session.close()

    return render(request, 'open_air/update_sensors.html', context)


@login_required()
def temperature_graph(request, temperature_graph_id):
    """
    Controller for the temperature graph page.
    """

    # Update sensor before viewing
    if not updatesensor(sensor_id):
        messages.info(request, 'Unable to update sensor')

    temperature_graph_plot = create_temperature_graph(temperature_graph_id)

    context = {
        'temperature_graph_plot': temperature_graph_plot,
    }
    return render(request, 'open_air/graphs.html', context)

@login_required()
def graphs_ajax(request, sensor_id):
    """
    Controller for the graphs ajax page.
    """
    # Update sensor before viewing
    if not updatesensor(sensor_id):
        messages.info(request, 'Unable to update sensor')

    # Get sensors from database
    Session = app.get_persistent_store_database('sensor_db', as_sessionmaker=True)
    session = Session()
    sensor = session.query(Sensor).get(int(sensor_id))

    if sensor.temperature_graph:
        temperature_graph_plot = create_temperature_graph(sensor.temperature_graph.id, height='300px')
    else:
        temperature_graph_plot = None

    context = {
        'temperature_graph_plot': temperature_graph_plot,
    }

    session.close()
    return render(request, 'open_air/graphs_ajax.html', context)
