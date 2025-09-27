import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import json
import logging

# Configuración de logs
logging.basicConfig(level=logging.INFO)

# --- Configuración de Archivos ---
# ADVERTENCIA: Estos nombres deben coincidir exactamente con los archivos subidos.
TELEMETRY_INPUT = 'race_01_telemetry-000-of-001.json'
TEAMS_INPUT = 'race_01_teams-000-of-001.json'
EVENTS_INPUT = 'race_01_events-000-of-001.json'
OUTPUT_FILE = 'joined_multi_race_data_optimized.jsonl' # Nuevo nombre para indicar optimización

# --------------------------------------------------------------------------------
# Clase DoFn para procesar los resultados de CoGroupByKey y formar el esquema final
# --------------------------------------------------------------------------------
class JoinDataFn(beam.DoFn):
    """
    Combina los datos de Telemetría y Eventos agrupados por (RaceID, HelicopterID).
    Utiliza el Side Input 'team_data_map' para adjuntar la información estática del equipo.
    """
    def __init__(self, team_data_map_side_input):
        # team_data_map_side_input es un PCollectionView de un diccionario
        self.team_data_map = team_data_map_side_input
        
    def process(self, element, team_map):
        # element es una tupla: ((RaceID, HelicopterID), {'telemetry': [...], 'events': [...]})
        # team_map es el diccionario cargado desde el Side Input
        
        # Desempaquetamos la clave compuesta
        try:
            (race_id, helicopter_id), grouped_data = element
        except ValueError:
            logging.error(f"Clave de agrupación no válida: {element[0]}")
            return

        telemetry_records = grouped_data.get('telemetry', [])
        event_records = grouped_data.get('events', [])

        # 1. Obtener la información del equipo usando el Side Input
        # Buscamos la información por HelicopterID en el diccionario en memoria.
        team_info = team_map.get(helicopter_id, {"TeamName": "UNKNOWN"})

        # 2. Ordenar Telemetría y Eventos por tiempo
        try:
            telemetry_records.sort(key=lambda x: x.get('Time_s', 0))
        except TypeError:
            logging.warning(f"No se pudo ordenar telemetría para {helicopter_id} en {race_id}")
        
        try:
            event_records.sort(key=lambda x: x.get('Timestamp_s', 0))
        except TypeError:
            logging.warning(f"No se pudo ordenar eventos para {helicopter_id} en {race_id}")
            
        # 3. Construir el registro final
        joined_record = {
            "RaceID": race_id,
            "HelicopterID": helicopter_id,
            "TeamInfo": team_info, # Adjuntado desde el Side Input
            "TelemetryHistory": telemetry_records,
            "RaceEvents": event_records
        }
        
        # Emitir el registro como una cadena JSON (JSON Lines)
        yield json.dumps(joined_record, ensure_ascii=False)

# --------------------------------------------------------------------------------
# Función principal de la pipeline
# --------------------------------------------------------------------------------
def run_pipeline(argv=None):
    """Ejecuta la pipeline de Apache Beam."""
    options = PipelineOptions(argv)
    
    with beam.Pipeline(options=options) as p:
        
        # Helper function para extraer la clave compuesta (RaceID, HelicopterID)
        def make_compound_key(record):
            race_id = record.get('RaceID', 'unknown_race')
            helicopter_id = record.get('HelicopterID')
            if not helicopter_id:
                raise ValueError(f"Registro sin HelicopterID: {record}")
            # La clave es una tupla de dos elementos
            return (race_id, helicopter_id), record
        
        # --- 1. PROCESAMIENTO DEL SIDE INPUT (TEAMS) ---
        # 1a. Leer el archivo de Equipos
        teams_collection = (
            p 
            | 'ReadTeams' >> ReadFromText(TEAMS_INPUT)
            | 'ParseTeamsJson' >> beam.Map(json.loads)
        )
        
        # 1b. Convertir la colección de equipos en un diccionario (clave: HelicopterID)
        # Esto se carga en memoria de los workers
        team_data_map_pcollection = (
            teams_collection
            | 'MapTeamsToDict' >> beam.Map(lambda x: (x.pop('HelicopterID'), x))
            | 'ViewAsDict' >> beam.AsDict()
        )

        # --- 2. PROCESAMIENTO DE TELEMETRÍA Y EVENTOS (COGROUPBYKEY) ---
        
        # 2a. Leer y mapear el archivo de Telemetría: clave = (RaceID, HelicopterID)
        telemetry_data = (
            p 
            | 'ReadTelemetry' >> ReadFromText(TELEMETRY_INPUT)
            | 'ParseTelemetryJson' >> beam.Map(json.loads)
            | 'KeyTelemetryByCompoundId' >> beam.Map(make_compound_key) # Clave compuesta
        )
        
        # 2b. Leer y mapear el archivo de Eventos: clave = (RaceID, HelicopterID)
        events_data = (
            p 
            | 'ReadEvents' >> ReadFromText(EVENTS_INPUT)
            | 'ParseEventsJson' >> beam.Map(json.loads)
            | 'KeyEventsByCompoundId' >> beam.Map(make_compound_key) # Clave compuesta
        )
        
        # 2c. Realizar el CoGroupByKey (Join) solo entre Telemetría y Eventos
        joined_telemetry_events = (
            {'telemetry': telemetry_data, 'events': events_data}
            | 'CoGroupTelemetryAndEvents' >> beam.CoGroupByKey()
        )
        
        # 3. Procesar, adjuntar el Side Input, y formatear la salida
        formatted_output = (
            joined_telemetry_events
            # Adjuntamos el Side Input usando el argumento `pvalue`
            | 'FormatAndAttachTeamInfo' >> beam.ParDo(
                JoinDataFn(team_data_map_pcollection),
                team_data_map_pcollection
            )
        )

        # 4. Escribir los resultados en el archivo de salida
        formatted_output | 'WriteResults' >> WriteToText(OUTPUT_FILE, shard_name_template='', num_shards=1)

# Calls the pipeline function to execute the script
if __name__ == '__main__':
    run_pipeline()
