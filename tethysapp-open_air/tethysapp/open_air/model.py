import json
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Float, String
from sqlalchemy.orm import sessionmaker

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
    updatets = Column(String)

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

        # Add the dams to the session, commit, and close
        session.commit()
        session.close()
