import random
import argparse
import json
from ..utils import utils

def generate_random_telemetry_data(num_records=10, race_id="RCE202501", num_teams=6):
    """
    Genera datos de telemetría de helicópteros simulados.

    Args:
        num_records (int): El número de registros de telemetría a generar.
        race_id (str): El ID de la carrera a la que pertenecen los datos.

    Yields:
        dict: Un diccionario que representa un registro de telemetría de helicóptero.
    """

    helicopter_teams = utils.get_helicopter_teams_id(num_teams)

    current_lap = {hid: 1 for hid in helicopter_teams}
    current_time = {hid: 0.0 for hid in helicopter_teams}
    current_fuel = {hid: 100 for hid in helicopter_teams}
    current_engine_temp = {hid: 80 for hid in helicopter_teams}
    current_rotor_rpm = {hid: 400 for hid in helicopter_teams}
    current_lat = {hid: 34.0522 for hid in helicopter_teams}
    current_lon = {hid: -118.2437 for hid in helicopter_teams}


    for _ in range(num_records):
        helicopter_id = random.choice(list(helicopter_teams))

        # Simular progresión en el tiempo y las vueltas
        current_time[helicopter_id] += round(random.uniform(5.0, 15.0), 1)
        if current_time[helicopter_id] > 300 * current_lap[helicopter_id]: # Simular vuelta cada ~300s
             current_lap[helicopter_id] += 1

        # Simular variaciones realistas en las métricas
        speed = round(random.uniform(250, 300), 1)
        altitude = round(random.uniform(60, 90), 1)

        current_fuel[helicopter_id] = max(0, round(current_fuel[helicopter_id] - random.uniform(0.1, 0.5), 1))
        current_engine_temp[helicopter_id] = round(max(85, min(100, current_engine_temp[helicopter_id] + random.uniform(-1, 2))), 1)
        current_rotor_rpm[helicopter_id] = round(max(420, min(480, current_rotor_rpm[helicopter_id] + random.uniform(-5, 5))))

        # Simular pequeños cambios en latitud y longitud
        current_lat[helicopter_id] += random.uniform(-0.001, 0.001)
        current_lon[helicopter_id] += random.uniform(-0.001, 0.001)

        telemetry_record = {
            "HelicopterID": helicopter_id,
            "RaceID": race_id,
            "LapNumber": current_lap[helicopter_id],
            "Time_s": round(current_time[helicopter_id], 1),
            "Speed_km_h": speed,
            "Altitude_m": altitude,
            "FuelLevel_percentage": current_fuel[helicopter_id],
            "EngineTemp_c": current_engine_temp[helicopter_id],
            "RotorRPM": current_rotor_rpm[helicopter_id],
            "Latitude": round(current_lat[helicopter_id], 4),
            "Longitude": round(current_lon[helicopter_id], 4)
        }
        yield json.dumps(telemetry_record)

