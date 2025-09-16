import argparse
import re
import apache_beam as beam

# A ParDo that splits a line of text into words.
# It yields multiple words for a single line of input.
class SplitLinesDoFn(beam.DoFn):
    def process(self, line):
        words = re.findall(r'[a-zA-Z\']+', line)
        for word in words:
            yield word

# A simple ParDo that formats the final output.
class FormatCountsDoFn(beam.DoFn):
    def process(self, word_count):
        # word_count is a tuple (word, count)
        yield f"{word_count[0]}: {word_count[1]}"

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default='input.txt',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file to write results to.')

    known_args, pipeline_args = parser.parse_known_args()

    # Create the pipeline object.
    with beam.Pipeline(argv=pipeline_args) as pipeline:

        # PCollection 1: Read the text file.
        lines = pipeline | 'ReadFromText' >> beam.io.ReadFromText(known_args.input)

        # PCollection 2: Split each line into individual words using our ParDo.
        words = lines | 'SplitLinesIntoWords' >> beam.ParDo(SplitLinesDoFn())

        # PCollection 3: Count the occurrences of each word.
        # This is a built-in Beam transform, not a ParDo.
        word_counts = words | 'CountWords' >> beam.combiners.Count.PerElement()

        # PCollection 4: Format the output using another ParDo.
        formatted_output = word_counts | 'FormatCounts' >> beam.ParDo(FormatCountsDoFn())
        
        # Load the final results into a text file.
        formatted_output | 'WriteToText' >> beam.io.WriteToText(known_args.output)

if __name__ == '__main__':
    run()