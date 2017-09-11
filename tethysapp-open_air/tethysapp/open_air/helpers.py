from plotly import graph_objs as go
from tethys_gizmos.gizmo_options import PlotlyView

from tethysapp.open_air.app import OpenAir as app
from tethysapp.open_air.model import TemperatureGraph
from datetime import datetime

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
        time.append(point.time)
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

def str2datetime(time_string):
    year = int(time_string[0:4])
    month = int(time_string[4:6])
    day = int(time_string[6:8])
    hour = int(time_string[8:10])
    minute = int(time_string[10:12])
    second = int(time_string[12:14])
    return datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)

def datetime2str(dt):
    time_string = str(dt.year)
    time_string += str(dt.month)
    time_string += str(dt.day)
    time_string += str(dt.hour)
    time_string += str(dt.minute)
    time_string += str(dt.second)
    return time_string
