import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from src.transforms import CleanDataFn
import logging

def run():
    # 'DirectRunner' runs locally on your machine
    options = PipelineOptions(flags=[], runner='DirectRunner',direct_running_mode='in_memory',direct_num_workers=1,experiments=['beam_fn_api'])
    
    with beam.Pipeline(options=options) as p:
        (
            p 
            | "ReadCSV" >> beam.io.ReadFromText("data/input.csv") # Source
            | "Clean"   >> beam.ParDo(CleanDataFn())             # Transform
            | "Write"   >> beam.io.WriteToText("data/output_1703_new")    # Sink
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.ERROR)
    run()
    print("Pipeline completed successfully!")