JOB_STATUS_FILE = "job_status.json"
"""
Name of a file in a job bucket that contains the status information
"""

DEFAULT_PAYLOAD_FILE = "input"
"""
Default name for a file in a job bucket that contains the only input for a runner
"""

DEFAULT_OUTPUT_FILE = "output"
"""
Default name for a file in a job bucket that contains the output of a runner
"""

INPUT_DIR = "inputs"
"""
Default name for a folder in a job bucket that contains the input data for a runner
"""

OUTPUT_DIR = "outputs"
"""
Default name for a folder in a job bucket that contains the output data of a runner
"""

DEFAULT_PAYLOAD_PATH = INPUT_DIR + "/" + DEFAULT_PAYLOAD_FILE
"""
Default path to the only input for a runner in a job bucket
"""

DEFAULT_OUTPUT_PATH = OUTPUT_DIR + "/" + DEFAULT_OUTPUT_FILE
"""
Default path to the output of a runner in a job bucket
"""
