import apache_beam as beam
import logging
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions, SetupOptions

# 1. Define the transform directly in this file
class CleanDataFn(beam.DoFn):
    """Production-grade transformation logic."""
    def process(self, element):
        try:
            columns = element.split(',')
            cleaned = [col.strip() for col in columns]
            yield ",".join(cleaned)
        except Exception as e:
            logging.error(f"Error processing: {e}")

def run():
    project_id = 'de-project-1703' 
    bucket = 'gs://de-project-input-files'

    options = PipelineOptions() # Create a PipelineOptions object to configure the pipeline.
    
    # 2. FIX: Since we are using one file, we use save_main_session.
    # This sends the CleanDataFn class definition to the workers.
    options.view_as(SetupOptions).save_main_session = True # This is crucial for ensuring that the worker nodes can access the CleanDataFn class when executing the pipeline.
    
    # 3. Cloud Metadata
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = project_id
    gcp_options.region = 'us-west1' 
    gcp_options.job_name = 'clean-data-v1703-final'
    gcp_options.temp_location = f'{bucket}/temp'
    gcp_options.staging_location = f'{bucket}/staging'

    # 4. Worker Configuration
    worker_options = options.view_as(WorkerOptions)
    worker_options.machine_type = 'e2-medium' 

    # 5. Set the Runner
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # 6. The Pipeline
    with beam.Pipeline(options=options) as p:
        (
            p 
            | "ReadFromCloud" >> beam.io.ReadFromText(f"{bucket}/input.csv")
            | "CleanRows"     >> beam.ParDo(CleanDataFn())
            | "WriteToCloud"  >> beam.io.WriteToText(f"{bucket}/results/output")
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
    print("Job submitted to Dataflow!")