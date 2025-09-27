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
# Nota: La lógica está diseñada para que, si los archivos futuros incluyen el RaceID,
# el sistema lo maneje. Asumo que el archivo de TEAMS es estático por HelicopterID
# a menos que se indique lo contrario.
TELEMETRY_INPUT = 'output/landing_zone/race_01/telemetry/*.json'
TEAMS_INPUT = 'output/landing_zone/race_01/teams/*.json'
EVENTS_INPUT = 'output/landing_zone/race_01/events/*.json'
OUTPUT_FILE = 'joined_multi_race_data.jsonl' # Nuevo nombre para indicar soporte multi-carrera

# --------------------------------------------------------------------------------
# Clase DoFn para procesar los resultados de CoGroupByKey y formar el esquema final
# --------------------------------------------------------------------------------
class JoinDataFn(beam.DoFn):
    """
    Combina los datos de Telemetría, Equipos y Eventos, agrupados por (RaceID, HelicopterID).
    Asume que la información del equipo es la misma para el HelicopterID, independientemente de la carrera.
    """
    def process(self, element):
        # element es una tupla: ((RaceID, HelicopterID), {'telemetry': [...], 'teams': [...], 'events': [...]})
        # Desempaquetamos la clave compuesta
        try:
            (race_id, helicopter_id), grouped_data = element
        except ValueError:
            # Manejar el caso si la clave no es una tupla de dos elementos (solo en caso de datos inesperados)
            logging.error(f"Clave de agrupación no válida: {element[0]}")
            return

        telemetry_records = grouped_data.get('telemetry', [])
        team_records = grouped_data.get('teams', [])
        event_records = grouped_data.get('events', [])

        # 1. Extraer la información del equipo (Usamos el primer registro si existe)
        # NOTA: Los datos de TEAMS se asumen estáticos por HelicopterID, por eso
        # extraemos el 'RaceID' del primer evento o telemetría si los datos de equipo
        # no tienen 'RaceID'. El archivo 'teams' suele ser estático.
        team_info = team_records[0] if team_records else {"TeamName": "UNKNOWN"}

        # 2. Ordenar Telemetría y Eventos por tiempo para un análisis secuencial
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
            "RaceID": race_id, # Nueva clave de salida
            "HelicopterID": helicopter_id,
            "TeamInfo": team_info,
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
        # Asume que el RaceID debe estar en el JSON
        def make_compound_key(record):
            race_id = record.get('RaceID', 'unknown_race')
            helicopter_id = record.get('HelicopterID')
            if not helicopter_id:
                raise ValueError(f"Registro sin HelicopterID: {record}")
            # La clave es una tupla de dos elementos
            return (race_id, helicopter_id), record
        
        # 1. Leer y mapear el archivo de Equipos (Asumiendo que TeamInfo no tiene RaceID)
        # Como TeamInfo es estático, lo mapeamos solo por HelicopterID, pero necesitamos 
        # RaceID para el join. Si TeamInfo no contiene RaceID, asumimos que se aplica a 
        # todas las carreras o que lo inyectaremos del otro lado.
        # **IMPORTANTE**: Si el archivo teams fuera por carrera, se usaría make_compound_key.
        # Aquí, lo unimos solo por HelicopterID, y CoGroupByKey lo tratará como una lista.
        teams_data = (
            p 
            | 'ReadTeams' >> ReadFromText(TEAMS_INPUT)
            | 'ParseTeamsJson' >> beam.Map(json.loads)
            # Solo key por HelicopterID. (RaceID será desconocido para CoGroupByKey en este lado)
            # Esto significa que el registro del equipo se adjuntará a CUALQUIER RaceID 
            # que coincida con ese HelicopterID.
            | 'KeyTeamsById' >> beam.Map(lambda x: (x.pop('HelicopterID'), x)) 
        )

        # 2. Leer y mapear el archivo de Telemetría: clave = (RaceID, HelicopterID)
        telemetry_data = (
            p 
            | 'ReadTelemetry' >> ReadFromText(TELEMETRY_INPUT)
            | 'ParseTelemetryJson' >> beam.Map(json.loads)
            | 'KeyTelemetryByCompoundId' >> beam.Map(make_compound_key) # Clave compuesta
        )
        
        # 3. Leer y mapear el archivo de Eventos: clave = (RaceID, HelicopterID)
        events_data = (
            p 
            | 'ReadEvents' >> ReadFromText(EVENTS_INPUT)
            | 'ParseEventsJson' >> beam.Map(json.loads)
            | 'KeyEventsByCompoundId' >> beam.Map(make_compound_key) # Clave compuesta
        )
        
        # 4. Ajustar datos de Equipos para CoGroupByKey
        # Necesitamos que los datos de equipos también sean clave por (RaceID, HelicopterID).
        # Como los datos de Telemetría y Eventos tienen el RaceID, usamos el PCollection
        # de Telemetría para inferir todas las combinaciones únicas (RaceID, HelicopterID)
        # y luego realizar un "side join" del TeamInfo.
        
        # Extraemos las claves únicas (RaceID, HelicopterID) de la telemetría
        compound_keys = (
            telemetry_data 
            | 'ExtractCompoundKeys' >> beam.Keys()
            | 'UniqueCompoundKeys' >> beam.Distinct()
        )

        # Realizamos el Join "lateral" (Side Join) con los datos de equipos
        # 1. Mapear las claves compuestas a la clave simple HelicopterID
        simple_keys_to_compound = (
            compound_keys
            | 'MapToSimpleKey' >> beam.Map(lambda key: (key[1], key))
        )
        
        # 2. Unir el simple key (HelicopterID) con la información del equipo
        joined_teams = (
            {'compound_key': simple_keys_to_compound, 'teams': teams_data}
            | 'JoinKeysWithTeams' >> beam.CoGroupByKey()
            | 'FlattenTeams' >> beam.FlatMap(lambda element: [
                (compound_key, team_record) 
                for team_record in element[1]['teams']
                for compound_key in element[1]['compound_key']
            ])
            # La salida es: ((RaceID, HelicopterID), TeamInfo)
            | 'RemapTeamsToCompoundKey' >> beam.Map(lambda x: (x[0], x[1]))
        )
        
        # 5. Realizar el CoGroupByKey (Join) con las tres colecciones (usando la clave compuesta)
        # Telemetría y Eventos ya tienen la clave compuesta. Usamos el 'joined_teams' ajustado.
        joined_data = (
            {'telemetry': telemetry_data, 'teams': joined_teams, 'events': events_data}
            | 'CoGroupAllData' >> beam.CoGroupByKey()
        )
        
        # 6. Procesar y formatear la salida final
        formatted_output = (
            joined_data
            | 'FormatFinalOutput' >> beam.ParDo(JoinDataFn())
        )

        # 7. Escribir los resultados en el archivo de salida
        formatted_output | 'WriteResults' >> WriteToText(OUTPUT_FILE, shard_name_template='', num_shards=1)

# Calls the pipeline function to execute the script
if __name__ == '__main__':
    run_pipeline()
