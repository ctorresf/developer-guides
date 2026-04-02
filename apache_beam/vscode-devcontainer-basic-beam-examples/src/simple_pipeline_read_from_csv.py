import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions

def parse_raw_data(line):
    if "id_meter" in line: return None # Exclude header
    print(line)
    columns = line.split(',')
    try:
        # Basic integrity validation, critical fields must exist
        if not columns[1] or columns[2] in ['NULL', '']: return None
        
        return {
            'id': columns[0].strip().lower(), # Standardization
            'ts': columns[1].strip(),
            'kwh': float(columns[2]),
            'v': int(columns[3]),
            'status': columns[4].strip()
        }
    except Exception:
        return None # We discard record values with incorrect format

options = PipelineOptions()
output_dir = "output/my-company/"
with beam.Pipeline(options=options) as p:
    
    # Node 1: READING
    silver_data = (
        p | "Reading Meters" >> beam.io.ReadFromText('data/energy_readings.csv')
          | "Parse and Validate" >> beam.Map(parse_raw_data)
          | "Null Filter" >> beam.Filter(lambda x: x is not None)
    )

    # Node 2: BRANCHING - OVERLOAD ALERTS (> 10 kWh)
    limit = 10.0
    (
        silver_data 
        | "Detect Overload" >> beam.Filter(lambda x: x['kwh'] > 10.0)
        | "Format Alert" >> beam.Map(lambda x: f"DANGER: Meter {x['id']} with {x['kwh']}kWh exceeded the limit of {limit}kWh")
        | "Save Alerts" >> beam.io.WriteToText(f"{output_dir}alert", file_name_suffix='.txt')
    )

    # Node 3: BRANCHING - ANOMALY DETECTION (Consumption 0)
    (
        silver_data
        | "Detect Zero Consumption" >> beam.Filter(lambda x: x['kwh'] == 0.0)
        | "Dict a JSON" >> beam.Map(lambda x: json.dumps(x))
        | "Save Anomalies" >> beam.io.WriteToText(f"{output_dir}anomalies", file_name_suffix='.json')
    )

    # Node 4: BRANCHING - HISTORICAL PROCESSING (SILVER)
    (
        silver_data
        | "Add Metadata" >> beam.Map(lambda x: {**x, 'source': 'SmartGrid-V1', 'company': 'My-Company'})
        | "Dict a CSV Line" >> beam.Map(lambda x: f"{x['id']},{x['ts']},{x['kwh']},{x['v']},{x['status']},{x['source']},{x['company']}")
        | "Store Silver Data" >> beam.io.WriteToText(f"{output_dir}silver_data", file_name_suffix='.csv')
    )

print("Pipeline complete. Please check the 3 files in the /output folder.")