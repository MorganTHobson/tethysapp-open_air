from datetime import timedelta
from plotly import graph_objs as go
from tethys_gizmos.gizmo_options import PlotlyView

from tethysapp.open_air.app import OpenAir as app
from tethysapp.open_air.model import TemperatureGraph, OzoneGraph, NO2Graph, H2SGraph, SO2Graph
from tethysapp.open_air.model import OzonePoint, NO2Point, H2SPoint, SO2Point

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
        'xaxis': {'title': 'Time (min)',
                  'range': [max(time) - timedelta(days=30), max(time)]},
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
    ozone_graph = session.query(OzoneGraph).get(ozone_graph_id)
    ozone_points = session.query(OzonePoint).filter(OzonePoint.ozone_graph == ozone_graph, 
                                                    OzonePoint.time >= ozone_graph.updatets - timedelta(days=7))
    time = []
    ppb = []
    error = []
    for point in ozone_points:
        time.append(point.time)
        ppb.append(point.ppb)
        error.append(point.std)

    # Build up Plotly plot
    ozone_graph_go = go.Scatter(
        x=time,
        y=ppb,
        name='Ozone Graph for Sensor {0}'.format(ozone_graph_id),
        mode='markers',
        marker={'color': '#0080ff', 'size': 10},
    )
    data = [ozone_graph_go]
    layout = {
        'title': 'Ozone Graph for Sensor {0}'.format(ozone_graph_id),
        'xaxis': {'title': 'Time',
                  'range': [max(time) - timedelta(days=7), max(time)]},
        'yaxis': {'title': 'ppb'},
    }
    figure = {'data': data, 'layout': layout}
    ozone_graph_plot = PlotlyView(figure, height=height, width=width)
    session.close()
    return ozone_graph_plot

def create_no2_graph(no2_graph_id, height='520px', width='100%'):
    """
    Generates a plotly view of a no2 graph.
    """
    # Get objects from database
    Session = app.get_persistent_store_database('sensor_db', as_sessionmaker=True)
    session = Session()
    no2_graph = session.query(NO2Graph).get(no2_graph_id)
    no2_points = session.query(NO2Point).filter(NO2Point.no2_graph == no2_graph,
                                                NO2Point.time >= no2_graph.updatets - timedelta(days=7))
    time = []
    ppb = []
    error = []
    for point in no2_points:
        time.append(point.time)
        ppb.append(point.ppb)
        error.append(point.std)

    # Build up Plotly plot
    no2_graph_go = go.Scatter(
        x=time,
        y=ppb,
        name='NO2 Graph for Sensor {0}'.format(no2_graph_id),
        mode='markers',
        marker={'color': '#0080ff', 'size': 10},
    )
    data = [no2_graph_go]
    layout = {
        'title': 'NO2 Graph for Sensor {0}'.format(no2_graph_id),
        'xaxis': {'title': 'Time',
                  'range': [max(time) - timedelta(days=7), max(time)]},
        'yaxis': {'title': 'ppb'},
    }
    figure = {'data': data, 'layout': layout}
    no2_graph_plot = PlotlyView(figure, height=height, width=width)
    session.close()
    return no2_graph_plot

def create_h2s_graph(h2s_graph_id, height='520px', width='100%'):
    """
    Generates a plotly view of a h2s graph.
    """
    # Get objects from database
    Session = app.get_persistent_store_database('sensor_db', as_sessionmaker=True)
    session = Session()
    h2s_graph = session.query(H2SGraph).get(h2s_graph_id)
    h2s_points = session.query(H2SPoint).filter(H2SPoint.h2s_graph == h2s_graph,
                                                H2SPoint.time >= h2s_graph.updatets - timedelta(days=7))
    time = []
    ppb = []
    error = []
    for point in h2s_points:
        time.append(point.time)
        ppb.append(point.ppb)
        error.append(point.std)

    # Build up Plotly plot
    h2s_graph_go = go.Scatter(
        x=time,
        y=ppb,
        name='H2S Graph for Sensor {0}'.format(h2s_graph_id),
        mode='markers',
        marker={'color': '#0080ff', 'size': 10},
    )
    data = [h2s_graph_go]
    layout = {
        'title': 'H2S Graph for Sensor {0}'.format(h2s_graph_id),
        'xaxis': {'title': 'Time',
                  'range': [max(time) - timedelta(days=7), max(time)]},
        'yaxis': {'title': 'ppb'},
    }
    figure = {'data': data, 'layout': layout}
    h2s_graph_plot = PlotlyView(figure, height=height, width=width)
    session.close()
    return h2s_graph_plot

def create_so2_graph(so2_graph_id, height='520px', width='100%'):
    """
    Generates a plotly view of a so2 graph.
    """
    # Get objects from database
    Session = app.get_persistent_store_database('sensor_db', as_sessionmaker=True)
    session = Session()
    so2_graph = session.query(SO2Graph).get(so2_graph_id)
    so2_points = session.query(SO2Point).filter(SO2Point.so2_graph == so2_graph,
                                                SO2Point.time >= so2_graph.updatets - timedelta(days=7))
    time = []
    ppb = []
    error = []
    for point in so2_points:
        time.append(point.time)
        ppb.append(point.ppb)
        error.append(point.std)

    # Build up Plotly plot
    so2_graph_go = go.Scatter(
        x=time,
        y=ppb,
        name='SO2 Graph for Sensor {0}'.format(so2_graph_id),
        mode='markers',
        marker={'color': '#0080ff', 'size': 10},
    )
    data = [so2_graph_go]
    layout = {
        'title': 'SO2 Graph for Sensor {0}'.format(so2_graph_id),
        'xaxis': {'title': 'Time',
                  'range': [max(time) - timedelta(days=7), max(time)]},
        'yaxis': {'title': 'ppb'},
    }
    figure = {'data': data, 'layout': layout}
    so2_graph_plot = PlotlyView(figure, height=height, width=width)
    session.close()
    return so2_graph_plot
