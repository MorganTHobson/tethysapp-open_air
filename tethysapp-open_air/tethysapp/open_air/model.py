import json
import boto3
import decimal
import pandas as pd
import numpy as np
from boto3.dynamodb.conditions import Key, Attr
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, ForeignKey, DateTime
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime


from .app import OpenAir as app
from .conversion_helpers import datetime2str, str2datetime
from .dynamo_pull import pull_db

Base = declarative_base()

data_file = 'data.csv'
callibrated_file = 'callibrated.csv'

# SQLAlchemy ORM definition for the sensor table
class Sensor(Base):
    """
    SQLAlchemy Dam DB Model
    """
    __tablename__ = 'sensors'

    # Columns
    id = Column(Integer, primary_key=True)
    latitude = Column(Float)
    longitude = Column(Float)
    m_o3 = Column(Float)
    m_no2 = Column(Float)
    m_h2s = Column(Float)
    m_so2 = Column(Float)

    # Relationships
    temperature_graph = relationship('TemperatureGraph', back_populates='sensor', uselist=False)
    ozone_graph = relationship('OzoneGraph', back_populates='sensor', uselist=False)
    no2_graph = relationship('NO2Graph', back_populates='sensor', uselist=False)
    h2s_graph = relationship('H2SGraph', back_populates='sensor', uselist=False)
    so2_graph = relationship('SO2Graph', back_populates='sensor', uselist=False)


class OzoneGraph(Base):
    """
    SQLAlchemy Ozone Graph DB Model
    """
    __tablename__ = 'ozone_graphs'

    # Columns
    id = Column(Integer, primary_key=True)
    sensor_id = Column(ForeignKey('sensors.id'))
    updatets = Column(DateTime)

    # Relationships
    sensor = relationship('Sensor', back_populates='ozone_graph')
    points = relationship('OzonePoint', back_populates='ozone_graph')

class NO2Graph(Base):
    """
    SQLAlchemy NO2 Graph DB Model
    """
    __tablename__ = 'no2_graphs'

    # Columns
    id = Column(Integer, primary_key=True)
    sensor_id = Column(ForeignKey('sensors.id'))
    updatets = Column(DateTime)

    # Relationships
    sensor = relationship('Sensor', back_populates='no2_graph')
    points = relationship('NO2Point', back_populates='no2_graph')

class H2SGraph(Base):
    """
    SQLAlchemy H2S Graph DB Model
    """
    __tablename__ = 'h2s_graphs'

    # Columns
    id = Column(Integer, primary_key=True)
    sensor_id = Column(ForeignKey('sensors.id'))
    updatets = Column(DateTime)

    # Relationships
    sensor = relationship('Sensor', back_populates='h2s_graph')
    points = relationship('H2SPoint', back_populates='h2s_graph')

class SO2Graph(Base):
    """
    SQLAlchemy SO2 Graph DB Model
    """
    __tablename__ = 'so2_graphs'

    # Columns
    id = Column(Integer, primary_key=True)
    sensor_id = Column(ForeignKey('sensors.id'))
    updatets = Column(DateTime)

    # Relationships
    sensor = relationship('Sensor', back_populates='so2_graph')
    points = relationship('SO2Point', back_populates='so2_graph')

class TemperatureGraph(Base):
    """
    SQLAlchemy Temperature Graph DB Model
    """
    __tablename__ = 'temperature_graphs'

    # Columns
    id = Column(Integer, primary_key=True)
    sensor_id = Column(ForeignKey('sensors.id'))
    updatets = Column(DateTime)

    # Relationships
    sensor = relationship('Sensor', back_populates='temperature_graph')
    points = relationship('TemperaturePoint', back_populates='temperature_graph')

class OzonePoint(Base):
    """
    SQLAlchemy Ozone Point DB Model
    """
    __tablename__ = 'ozone_points'

    # Columns
    id = Column(Integer, primary_key=True)
    ozone_graph_id = Column(ForeignKey('ozone_graphs.id'))
    time = Column(DateTime)  #: generic python datetime object
    ppb = Column(Float) #: parts per billion
    std = Column(Float) #: Standard deviation

    # Relationships
    ozone_graph = relationship('OzoneGraph', back_populates='points')

class NO2Point(Base):
    """
    SQLAlchemy NO2 Point DB Model
    """
    __tablename__ = 'no2_points'

    # Columns
    id = Column(Integer, primary_key=True)
    no2_graph_id = Column(ForeignKey('no2_graphs.id'))
    time = Column(DateTime)  #: generic python datetime object
    ppb = Column(Float) #: parts per billion
    std = Column(Float) #: Standard deviation

    # Relationships
    no2_graph = relationship('NO2Graph', back_populates='points')

class H2SPoint(Base):
    """
    SQLAlchemy H2S Point DB Model
    """
    __tablename__ = 'h2s_points'

    # Columns
    id = Column(Integer, primary_key=True)
    h2s_graph_id = Column(ForeignKey('h2s_graphs.id'))
    time = Column(DateTime)  #: generic python datetime object
    ppb = Column(Float) #: parts per billion
    std = Column(Float) #: Standard deviation

    # Relationships
    h2s_graph = relationship('H2SGraph', back_populates='points')

class SO2Point(Base):
    """
    SQLAlchemy SO2 Point DB Model
    """
    __tablename__ = 'so2_points'

    # Columns
    id = Column(Integer, primary_key=True)
    so2_graph_id = Column(ForeignKey('so2_graphs.id'))
    time = Column(DateTime)  #: generic python datetime object
    ppb = Column(Float) #: parts per billion
    std = Column(Float) #: Standard deviation

    # Relationships
    so2_graph = relationship('SO2Graph', back_populates='points')

class TemperaturePoint(Base):
    """
    SQLAlchemy Temperature Point DB Model
    """
    __tablename__ = 'temperature_points'

    # Columns
    id = Column(Integer, primary_key=True)
    temperature_graph_id = Column(ForeignKey('temperature_graphs.id'))
    time = Column(DateTime)  #: generic python datetime object
    temperature = Column(Float)  #: fahrenheit, maybe

    # Relationships
    temperature_graph = relationship('TemperatureGraph', back_populates='points')


def get_all_sensors():
    """
    Get all persisted sensors.
    """
    Session = app.get_persistent_store_database('sensor_db', as_sessionmaker=True)
    session = Session()

    # Query for all sensor records
    sensors = session.query(Sensor).all()


    #session.commit()
    #session.close()

    return sensors

def init_sensor_db(engine, first_time):
    """
    Initializer for the primary database.
    """
    # Create all the tables
    Base.metadata.create_all(engine)

    # Add data
    if first_time:
        # Make session
        Session = sessionmaker(bind=engine)
        session = Session()

        df = pd.read_csv(data_file)
        df = df[['id', 'Location:Latitude', 'Location:Longitude', 'qr_ozone', 'qr_no2', 'qr_h2s', 'qr_so2']].dropna()
        #df = df.set_index('id', drop=False).drop_duplicates().drop('28')
        df = df.set_index('id', drop=False).drop_duplicates()
        
        for index, row in df.iterrows():
            sensor = Sensor(
                id = row['id'],
                latitude = row['Location:Latitude'],
                longitude = row['Location:Longitude'],
                m_o3 = abs(float(row['qr_ozone'].rsplit(" ", 1)[1])),
                m_no2 = abs(float(row['qr_no2'].rsplit(" ", 1)[1])),
                m_h2s = abs(float(row['qr_h2s'].rsplit(" ", 1)[1])),
                m_so2 = abs(float(row['qr_so2'].rsplit(" ", 1)[1]))
            )
            session.add(sensor)

        # Add the sensors to the session, commit, and close
        session.commit()
        session.close()

def update_sensor(sensor_id):
    """
    Update the graph data of a particular sensor
    """

    try:

        Session = app.get_persistent_store_database('sensor_db', as_sessionmaker=True)
        session = Session()

        # Get sensor object
        sensor = session.query(Sensor).get(int(sensor_id))

        ozone_graph = sensor.ozone_graph
        no2_graph = sensor.no2_graph
        h2s_graph = sensor.h2s_graph
        so2_graph = sensor.so2_graph

        # Create graphs if none exist
        if not ozone_graph:
            ozone_graph = OzoneGraph(updatets = datetime(1, 1, 1))
            sensor.ozone_graph = ozone_graph
        if not no2_graph:
            no2_graph = NO2Graph(updatets = datetime(1, 1, 1))
            sensor.no2_graph = no2_graph
        if not h2s_graph:
            h2s_graph = H2SGraph(updatets = datetime(1, 1, 1))
            sensor.h2s_graph = h2s_graph
        if not so2_graph:
            so2_graph = SO2Graph(updatets = datetime(1, 1, 1))
            sensor.so2_graph = so2_graph


        ozone_points = ozone_graph.points
        no2_points = no2_graph.points
        so2_points = so2_graph.points
        h2s_points = h2s_graph.points


        df = pull_db(sensor_id, 30)
        
        ozone_newtimes = []
        no2_newtimes = []
        h2s_newtimes = []
        so2_newtimes = []

        for index, row in df.iterrows():
            time = str2datetime(str(row["timest"][0]))
            if time > ozone_graph.updatets:
                ozone_points.append(OzonePoint(time=time, ppb = float(row["O3_avg"][0])/sensor.m_o3, std = float(row["O3_std"][0])/sensor.m_o3))
                ozone_newtimes.append(time)
            if time > no2_graph.updatets:
                no2_points.append(NO2Point(time=time, ppb = float(row["NO2_avg"][0])/sensor.m_no2, std = float(row["NO2_std"][0])/sensor.m_no2))
                no2_newtimes.append(time)
            if time > h2s_graph.updatets:
                h2s_points.append(H2SPoint(time=time, ppb = float(row["H2S_avg"][0])/sensor.m_h2s, std = float(row["H2S_std"][0])/sensor.m_h2s))
                h2s_newtimes.append(time)
            if time > so2_graph.updatets:
                so2_points.append(SO2Point(time=time, ppb = float(row["SO2_avg"][0])/sensor.m_so2, std = float(row["SO2_std"][0])/sensor.m_so2))
                so2_newtimes.append(time)

        if len(ozone_newtimes) != 0:
            ozone_graph.updatets = max(ozone_newtimes)
        if len(no2_newtimes) != 0:
            no2_graph.updatets = max(no2_newtimes)
        if len(h2s_newtimes) != 0:
            h2s_graph.updatets = max(h2s_newtimes)
        if len(so2_newtimes) != 0:
            so2_graph.updatets = max(so2_newtimes)


        # Wrap up db session
        session.commit()
        session.close()

    except Exception as e:
        print(e)
        return False

    return True
