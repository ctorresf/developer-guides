import random
from datetime import datetime, timedelta
import json

def generate_random_data_generator(num_records=10, race_id="RCE202501", start_time="2025-06-03 20:00:00"):
    """
    Genera datos de interacción de fanáticos y consumo de contenido simulados.

    Args:
        num_records (int): El número de registros de interacción a generar.
        race_id (str): El ID de la carrera a la que pertenecen los datos.
        start_time (str): La fecha y hora de inicio en formato "YYYY-MM-DD HH:MM:SS".

    Yields:
        dict: Un diccionario que representa un registro de interacción de fanático.
    """

    current_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    fan_counter = 0

    device_types = ["Mobile", "SmartTV", "Desktop", "Tablet", "Other"]
    # Países basados en el caso de estudio (enfoque en mercados emergentes y global)
    viewer_locations = [
        "Brazil", "India", "Germany", "Japan", "Mexico", "Indonesia", "South Africa",
        "Argentina", "Nigeria", "Vietnam", "Australia", "Canada", "France", "UK", "USA"
    ]

    for _ in range(num_records):
        fan_counter += 1
        fan_id = f"F{fan_counter:03d}"

        # Incrementar el tiempo para simular eventos en diferentes momentos
        time_increment = random.randint(30, 300) # Eventos cada 30 segundos a 5 minutos
        current_time += timedelta(seconds=time_increment)
        timestamp_str = current_time.strftime("%Y-%m-%d %H:%M:%S")

        viewer_location = random.choice(viewer_locations)
        device_type = random.choice(device_types)

        # Simular segundos vistos (entre 30 segundos y 3600 segundos = 1 hora)
        engagement_metric = random.randint(30, 3600)

        # Probabilidad de que el fan haga clic en predicciones o merchandising
        prediction_clicked = random.choice([True, False, False]) # Más probable que no hagan clic
        merchandising_clicked = random.choice([True, False, False, False]) # Menos probable aún

        fan_record = {
            "FanID": fan_id,
            "RaceID": race_id,
            "Timestamp": timestamp_str,
            "ViewerLocationCountry": viewer_location,
            "DeviceType": device_type,
            "EngagementMetric_secondswatched": engagement_metric,
            "PredictionClicked": prediction_clicked,
            "MerchandisingClicked": merchandising_clicked
        }
        yield json.dumps(fan_record)

