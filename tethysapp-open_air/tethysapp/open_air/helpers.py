from plotly import graph_objs as go
from tethys_gizmos.gizmo_options import PlotlyView

from tethysapp.open_air.app import OpenAir as app
from tethysapp.open_air.model import TemperatureGraph, OzoneGraph

def create_temperature_graph(temperature_graph_id, height='520px', width='100%'):
    """
    Generates a plotly view of a temperature graph.
    """
    # Get objects from database
    Session = app.get_persistent_store_database('sensor_db', as_sessionmaker=True)
    session = Session()
    temperature_graph = session.query(TemperatureGraph).get(int(temperature_graph_id))
    sensor = temperature_graph.sensor
    time = []
    temp = []
    for point in temperature_graph.points:
        time.append(point.time.hour)
        temp.append(point.temperature)

    # Build up Plotly plot
    temperature_graph_go = go.Scatter(
        x=time,
        y=temp,
        name='Temperature Graph for Sensor {0}'.format(sensor.id),
        line={'color': '#0080ff', 'width': 4, 'shape': 'spline'},
    )
    data = [temperature_graph_go]
    layout = {
        'title': 'Temperature Graph for Sensor {0}'.format(sensor.id),
        'xaxis': {'title': 'Time (min)'},
        'yaxis': {'title': 'Temp (F)'},
    }
    figure = {'data': data, 'layout': layout}
    temperature_graph_plot = PlotlyView(figure, height=height, width=width)
    session.close()
    return temperature_graph_plot

def create_ozone_graph(ozone_graph_id, height='520px', width='100%'):
    """
    Generates a plotly view of a ozone graph.
    """
    # Get objects from database
    Session = app.get_persistent_store_database('sensor_db', as_sessionmaker=True)
    session = Session()
    ozone_graph = session.query(OzoneGraph).get(int(ozone_graph_id))
    sensor = ozone_graph.sensor
    time = []
    ppb = []
    for point in ozone_graph.points:
        time.append(point.time)
        ppb.append(point.ppb)

    # Build up Plotly plot
    ozone_graph_go = go.Scatter(
        x=time,
        y=ppb,
        name='Ozone Graph for Sensor {0}'.format(sensor.id),
        line={'color': '#0080ff', 'width': 4, 'shape': 'spline'},
    )
    data = [ozone_graph_go]
    layout = {
        'title': 'Ozone Graph for Sensor {0}'.format(sensor.id),
        'xaxis': {'title': 'Time'},
        'yaxis': {'title': 'ppb'},
    }
    figure = {'data': data, 'layout': layout}
    ozone_graph_plot = PlotlyView(figure, height=height, width=width)
    session.close()
    return ozone_graph_plot
