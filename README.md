# Dataflow Python Pipeline

A memory-efficient Apache Beam pipeline designed to run on resource-constrained environments (4GB RAM) and deployable to Google Cloud Dataflow.

## Project Structure
- `src/`: Contains the pipeline logic and transformations.
- `data/`: Local directory for testing input/output.
- `requirements.txt`: Project dependencies.

## Local Development
1. **Setup Environment:**
   ```powershell
   uv venv
   .\.venv\Scripts\activate
   $env:PYTHONPATH = "."
#To run pipeline
python src/pipeline.py

Designed for Google Cloud Dataflow. Uses DirectRunner for local testing and DataflowRunner for cloud execution.


---

### **Step 2: Create a .gitignore**
You don't want to upload your 100MB virtual environment or your local test data to GitHub. Create a file named `.gitignore` and add:
```text
.venv/
__pycache__/
data/output*
.uv/