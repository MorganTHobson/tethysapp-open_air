import json
import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String, ForeignKey, DateTime
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

from .app import OpenAir as app


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
    updatets = Column(DateTime)

    # Relationships
    temperature_graph = relationship('TemperatureGraph', back_populates='sensor', uselist=False)


class TemperatureGraph(Base):
    """
    SQLAlchemy Temperature Graph DB Model
    """
    __tablename__ = 'temperature_graphs'

    # Columns
    id = Column(Integer, primary_key=True)
    sensor_id = Column(ForeignKey('sensors.id'))

    # Relationships
    sensor = relationship('Sensor', back_populates='temperature_graph')
    points = relationship('TemperaturePoint', back_populates='temperature_graph')


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
    session.close()

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

        # Initialize database with all sensors
        for i in range(1, 11):
            sensor = Sensor(
                id = i,
                latitude = 5 * i,
                longitude = -5 * i,
                updatets = 0
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
        temperature_graph = sensor.temperature_graph

        # Create graphs if none exist
        if not temperature_graph:
            temperature_graph = TemperatureGraph()
            sensor.temperature_graph = temperature_graph

        temperature_points = temperature_graph.points

        # Get DynamoDB table
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('TethysTestData') # Remember to update table here

        response = table.query(
            KeyConditionExpression=Key('id').eq(int(sensor.id)) & Key('timest').gt(int(sensor.updatets))
        )

        # Extract points from table response
        for entry in response['Items']:
            temperature_points.append(TemperaturePoint(time = int(entry['timest']), temperature = int(entry['temp'])))
            # Update time stamp
            if int(entry['timest']) > int(sensor.updatets):
                sensor.updatets = entry['timest']

        # Wrap up db session
        session.commit()
        session.close()

    except Exception as e:
        print(e)
        return False

    return True
