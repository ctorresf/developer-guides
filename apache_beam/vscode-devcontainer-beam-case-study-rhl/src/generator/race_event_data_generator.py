

import random
import json
from ..utils import utils

def generate_random_race_event_data(num_events=10, race_id="RCE202501", num_teams=6, initial_timestamp=0):
    """
    Genera datos de eventos de carrera simulados.

    Args:
        num_events (int): El número de eventos de carrera a generar.
        race_id (str): El ID de la carrera a la que pertenecen los datos.
        initial_timestamp (int): El punto de partida en segundos para los timestamps.

    Yields:
        dict: Un diccionario que representa un registro de evento de carrera.
    """
    helicopter_ids = utils.get_helicopter_teams_id(num_teams)
    event_counter = 0
    current_timestamp = initial_timestamp

    # Definir tipos de eventos con sus detalles e impactos asociados
    event_types_data = {
        "Lap Completion": {
            "details": ["completes lap"], # La vuelta se añade dinámicamente
            "impacts": ["None"]
        },
        "Overtake": {
            "details": ["overtakes"], # El segundo helicóptero se añade dinámicamente
            "impacts": ["Position Change"]
        },
        "Minor Mechanical Glitch": {
            "details": [
                "Brief power dip detected in",
                "Minor engine misfire in",
                "Rotor instability in"
            ],
            "impacts": ["Temporary Speed Reduction", "Minor Performance Fluctuation"]
        },
        "Speed Boost": {
            "details": ["activates temporary speed boost"],
            "impacts": ["Speed Increase"]
        },
        "Pilot Maneuver": {
            "details": [
                "performs evasive maneuver",
                "executes precision turn",
                "makes daring aerial move"
            ],
            "impacts": ["None", "Minor Speed Adjustment"]
        },
        "Collision": { # Evento menos frecuente
            "details": ["collides with"],
            "impacts": ["Significant Speed Reduction", "Race Retirement", "Minor Damage"]
        },
        "Fuel Warning": {
            "details": ["low fuel alert"],
            "impacts": ["Monitor Fuel"]
        }
    }

    # Mantener un registro de las vueltas completadas por cada helicóptero
    laps_completed = {hid: 0 for hid in helicopter_ids}

    for i in range(num_events):
        event_counter += 1
        current_timestamp += random.randint(5, 60) # Los eventos ocurren en intervalos de 5 a 60 segundos

        event_type = random.choice(list(event_types_data.keys()))
        
        # Seleccionar un HelicopterID principal para el evento
        helicopter_id = random.choice(helicopter_ids)
        
        details = ""
        impact = ""

        # Lógica para construir detalles e impactos específicos del evento
        if event_type == "Lap Completion":
            laps_completed[helicopter_id] += 1
            details = f"{helicopter_id} {random.choice(event_types_data[event_type]['details'])} {laps_completed[helicopter_id]}"
            impact = random.choice(event_types_data[event_type]['impacts'])
        elif event_type == "Overtake" or event_type == "Collision":
            # Asegurarse de que los dos helicópteros sean diferentes
            other_helicopter_id = random.choice([hid for hid in helicopter_ids if hid != helicopter_id])
            details = f"{helicopter_id} {random.choice(event_types_data[event_type]['details'])} {other_helicopter_id}"
            impact = random.choice(event_types_data[event_type]['impacts'])
        else:
            # Para otros tipos de eventos que involucran un solo helicóptero
            details = f"{random.choice(event_types_data[event_type]['details'])} {helicopter_id}"
            impact = random.choice(event_types_data[event_type]['impacts'])
        
        event_record = {
            "EventID": f"EVT{event_counter:03d}",
            "RaceID": race_id,
            "Timestamp_s": current_timestamp,
            "EventType": event_type,
            "Details": details,
            "HelicopterID": helicopter_id, # El helicóptero principal involucrado en el evento
            "Impact": impact
        }
        yield json.dumps(event_record)
