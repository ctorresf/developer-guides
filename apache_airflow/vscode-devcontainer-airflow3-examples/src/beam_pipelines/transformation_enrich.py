import apache_beam as beam
import argparse


class EnrichData(beam.DoFn):
    """PTransform 2: Add the time zone and a high value flag."""
    LOCATION_MAP = {
        'US_NY': 'America/New York', 
        'GB_LDN': 'Europe/London', 
        'US_CA': 'America/Los Angeles', 
        'FR_PAR': 'Europe/Paris'
    }

    def process(self, element):
        print("Enriching Row: ")
        row =  beam.Row(
                        name=element.name,
                        country_city_code=element.country_city_code,
                        user_id=element.user_id,
                        order_value=element.order_value,
                        is_active_flag=element.is_active_flag,
                        timezone=self.LOCATION_MAP.get(element.country_city_code, 'Unknown'),
                        is_high_value=int(element.order_value) > 100
                       )
        print("Enriched Row: ")
        print(row)
        yield row

def run_pipeline(argv=None):
    """
    Main function to run the Beam pipeline.
    """
    # 1. Create a custom argument parser and get the Beam options.
    #    This is the standard way to handle command-line parameters in Beam.
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input file path to read data from.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file prefix.')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Pass the parsed arguments to the Beam pipeline.
    # Beam will automatically handle its own arguments (like --runner).
    options = beam.options.pipeline_options.PipelineOptions(pipeline_args)

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadFromVSA_T1' >> beam.io.ReadFromCsv(known_args.input + "*", header=0, encoding='utf-8')
            | 'EnrichData' >> beam.ParDo(EnrichData())
            | 'WriteFinalToVSA' >> beam.io.WriteToCsv(
                known_args.output, 
                num_shards=1 # Write a single file
            )
        )

if __name__ == '__main__':
    run_pipeline()