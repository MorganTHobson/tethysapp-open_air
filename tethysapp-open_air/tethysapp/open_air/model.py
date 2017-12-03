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

Base = declarative_base()


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

    # Relationships
    temperature_graph = relationship('TemperatureGraph', back_populates='sensor', uselist=False)
    ozone_graph = relationship('OzoneGraph', back_populates='sensor', uselist=False)


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

    # Relationships
    ozone_graph = relationship('OzoneGraph', back_populates='points')

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

        df = pd.read_csv('/home/mhobson5/tethysapp-open_air/tethysapp-open_air/tethysapp/open_air/data.csv')
        df = df[['id', 'Location:Latitude', 'Location:Longitude']].dropna()
        df = df.set_index('id', drop=False).drop_duplicates().drop('28')
        
        for index, row in df.iterrows():
            sensor = Sensor(
                id = row['id'],
                latitude = row['Location:Latitude'],
                longitude = row['Location:Longitude']
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

        # Create graphs if none exist
        if not ozone_graph:
            ozone_graph = OzoneGraph(updatets = datetime(1, 1, 1))
            sensor.ozone_graph = ozone_graph


        ozone_points = ozone_graph.points


        df = pd.read_csv('/home/mhobson5/tethysapp-open_air/tethysapp-open_air/tethysapp/open_air/callibrated.csv')
        df = df[['time', str(sensor_id)]].dropna()
        df = df.set_index('time', drop=False).drop_duplicates()
        
        for index, row in df.iterrows():
            time = datetime(year=int(index[:4]), month=int(index[5:7]), day=int(index[8:10]), hour=int(index[11:13]))
            if time > ozone_graph.updatets:
                ozone_points.append(OzonePoint(time=time, ppb = float(row[str(sensor_id)])))
                ozone_graph.updatets = time

        ## Get DynamoDB table
        #idynamodb = boto3.resource('dynamodb')
        #table = dynamodb.Table('TethysTestData') # Remember to update table here

        #response = table.query(
        #    KeyConditionExpression=Key('id').eq(int(sensor.id)) & Key('timest').gt(int(datetime2str(sensor.updatets)))
        #)

        # Extract points from table response
        #for entry in response['Items']:
        #    temperature_points.append(TemperaturePoint(time = str2datetime(entry['timest'])), temperature = float(entry['temp']))
        #    # Update time stamp
        #    if int(entry['timest']) > int(datetime2str(sensor.updatets)):
        #        sensor.updatets = str2datetime(entry['timest'])

        # Wrap up db session
        session.commit()
        session.close()

    except Exception as e:
        print(e)
        return False

    return True
