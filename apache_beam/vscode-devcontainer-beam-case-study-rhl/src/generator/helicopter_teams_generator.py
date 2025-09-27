import json
import os
import random
from faker import Faker
import argparse

def generate_random_helicopter_teams(num_teams=6):
    """
    Genera un diccionario con datos de equipos de helicópteros aleatorios.

    Args:
        num_teams (int): El número de equipos de helicópteros aleatorios a generar.

    Returns:
        dict: Un diccionario donde las claves son HelicopterID (ej. "HRL001")
              y los valores son diccionarios con los detalles del equipo.
    """
    fake = Faker()
    teams_data = {}

    # Listas para selección aleatoria
    countries = ["USA", "Russia", "Japan", "Brazil", "UK", "China", "Germany", "France", "Canada", "Australia", "India", "South Africa", "Mexico", "Indonesia", "Spain"]
    aircraft_models = ["Raptor X10", "Vostok 7", "Sakura Stealth", "Amazonia Turbo", "Phantom VTOL", "Celestial Swift", "EagleWing Pro", "SkyHunter Elite", "AeroJet X2", "StormBreaker A1", "Horizon VTOL", "Apex Flyer"]
    
    for i in range(1, num_teams + 1):
        helicopter_id = f"HRL{i:03d}" # Genera IDs como HRL001, HRL002, etc.
        
        # Genera datos aleatorios para cada campo
        team_name = f"{fake.color_name().capitalize()} {fake.word().capitalize()}s" # Ej: "Red Hawks"
        pilot_name = fake.name()
        country = random.choice(countries)
        aircraft_model = random.choice(aircraft_models)
        max_speed_kmh = random.randint(300, 350) # Velocidad máxima entre 300 y 350 km/h
        endurance_hours = round(random.uniform(2.0, 3.0), 1) # Resistencia entre 2.0 y 3.0 horas
        sponsor = fake.company()

        teams_data = {
            "HelicopterID": helicopter_id,
            "TeamName": team_name,
            "PilotName": pilot_name,
            "Country": country,
            "AircraftModel": aircraft_model,
            "MaxSpeed_km_h": max_speed_kmh,
            "Endurance_hours": endurance_hours,
            "Sponsor": sponsor
        }
        yield json.dumps(teams_data)
    #return teams_data

