import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import json
import logging
import argparse

# Log Configuration
logging.basicConfig(level=logging.INFO)

# --------------------------------------------------------------------------------
# DoFn class to attach the Side Input (TeamInfo) to each Event record
# --------------------------------------------------------------------------------
class AttachTeamInfoToEventFn(beam.DoFn):
    """
    Takes an event record and attaches the static team information
    using a Side Input that contains a map of HelicopterID -> TeamInfo.
    """
    # team_map is the Side Input injected by Beam
    def process(self, event_record, team_map):
        try:
            helicopter_id = event_record.get('HelicopterID')
            
            if not helicopter_id:
                logging.warning(f"Event record without HelicopterID, skipping: {event_record}")
                return

            # 1. Get team information using the Side Input
            # Look up the information by HelicopterID in the in-memory dictionary.
            team_info = team_map.get(helicopter_id, {"TeamName": "UNKNOWN"})
            
            # 2. Build the final record
            # Include all fields from the event and nest the team info
            joined_record = event_record.copy()
            joined_record["TeamInfo"] = team_info

            # Emit the record as a JSON string (JSON Lines)
            yield json.dumps(joined_record, ensure_ascii=False)
            
        except Exception as e:
            logging.error(f"Error processing event record: {e}")

# --------------------------------------------------------------------------------
# Main function of the pipeline
# --------------------------------------------------------------------------------
def run_pipeline(argv=None):
    """Run the Apache Beam pipeline to link events and teams."""

    parser = argparse.ArgumentParser()
    parser.add_argument('--input_events',
                        dest='input_events',
                        default='events.json',
                        help='Input Event file pattern to process.')
    parser.add_argument('--input_teams',
                        dest='input_teams',
                        default='teams.json',
                        help='Input Team file pattern to process.')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Pass the parsed arguments to the Beam pipeline.
    # Beam will automatically handle its own arguments (like --runner).
    options = beam.options.pipeline_options.PipelineOptions(pipeline_args)

    with beam.Pipeline(options=options) as p:
        # --- 1. SIDE INPUT PROCESSING (TEAMS) ---
        # Read the Teams file and convert it to a dictionary {HelicopterID:TeamInfo}
        team_data_map_pcollection = (
            p 
            | 'ReadTeams' >> ReadFromText(known_args.input_teams)
            | 'ParseTeamsJson' >> beam.Map(json.loads)
            # Restructure to (HelicopterID, TeamInfo)
            | 'MapTeamsToDict' >> beam.Map(lambda x: (x.pop('HelicopterID'), x)) # HelicopterID is removed from TeamInfo 
        )

        # --- 2. EVENT PROCESSING (MAIN INPUT) ---
        # Read the main collection (Events)
        events_data = (
            p 
            | 'ReadEvents' >> ReadFromText(known_args.input_events)
            | 'ParseEventsJson' >> beam.Map(json.loads)
        )
        
        # 3. Attach the Side Input and format the output
        formatted_output = (
            events_data
            | 'AttachTeamInfo' >> beam.ParDo(
                AttachTeamInfoToEventFn(), # Instantiate with the PCollectionView
                beam.pvalue.AsDict(team_data_map_pcollection)       # Pass as Side Input argument to .process()
            )
        )

        # 4. Escribir los resultados en el archivo de salida
        formatted_output | 'WriteResults' >> WriteToText(known_args.output, num_shards=1, file_name_suffix='.jsonl')

# Calls the pipeline function to execute the script
if __name__ == '__main__':
    run_pipeline()
