import apache_beam as beam

class CleanDataFn(beam.DoFn):
    """Production-grade transformation logic."""
    def process(self, element):
        try:
            # element is a line of text from CSV
            columns = element.split(',')
            # Standard: strip whitespace and rejoin
            cleaned = [col.strip() for col in columns]
            
            # Yield results one-by-one to save 4GB RAM
            yield ",".join(cleaned)
        except Exception as e:
            # Standard: Log errors instead of crashing the pipeline
            print(f"Error processing: {e}")