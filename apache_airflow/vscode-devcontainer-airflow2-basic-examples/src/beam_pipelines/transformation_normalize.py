import apache_beam as beam
import argparse


class NormalizeName(beam.DoFn):
    """PTransform 1: Normalizes the name (Capitalizes) and renames the column."""
    def process(self, element):
        yield beam.Row(
            name=element.name_raw.title(),
            country_city_code=element.location_code,
            user_id=int(element.user_id),
            order_value=float(element.order_value),
            is_active_flag=int(element.is_active_flag)
        )

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
            | 'ReadFromVSA' >> beam.io.ReadFromCsv(known_args.input, header=0, encoding='utf-8')
            | 'NormalizeFields' >> beam.ParDo(NormalizeName())
            | 'WriteToVSA' >> beam.io.WriteToCsv(
                known_args.output, 
            )
        )

if __name__ == '__main__':
    run_pipeline()