import boto3
import sys
import os
import subprocess
import logging
import json

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def spark_submit(s3_bucket_script: str,input_script: str, event: dict)-> None:
    """
    Submits a local Spark script using spark-submit.
    """
     # Set the environment variables for the Spark application
    # pyspark_submit_args = event.get('PYSPARK_SUBMIT_ARGS', '')
    # # Source input and output if available in event
    # input_path = event.get('INPUT_PATH','')
    # output_path = event.get('OUTPUT_PATH', '')

    for key,value in event.items():
        os.environ[key] = value
    # Run the spark-submit command on the local copy of teh script
    try:
        logger.info(f'Spark-Submitting the Spark script {input_script} from {s3_bucket_script}')
        prosessed = subprocess.run(["spark-submit", "./script/sample-accommodations-to-iceberg.py", "--event", json.dumps(event)], capture_output=True, text=True, check=True, env=os.environ)
        stdoutstring = prosessed.stdout
        print(stdoutstring)
        returncodestring = prosessed.returncode
        print(stdoutstring)
        stderrstring = prosessed.stderr
        print(stderrstring)

    except subprocess.CalledProcessError as e:
        sys.stderr.write('!!!!!!subprocess.CalledProcessError!!!!!!\n')
        sys.stderr.write('----------------\n')
        logger.error(f'Error Spark-Submit with exception: {e}')
        sys.stderr.write('----------------\n')
        sys.stderr.write('ret='+str(e.returncode)+'\n')
        sys.stderr.write('----------------\n')
        sys.stderr.write('cmd='+str(e.cmd)+'\n')
        sys.stderr.write('----------------\n')
        sys.stderr.write('output='+e.output+'\n')
        raise e

    except Exception as e :
        logger.error(f'Error Spark-Submit with exception: {e}')
        raise e
    else:
        logger.info(f'Script {input_script} successfully submitted')

def lambda_handler(event, context):

    """
    Lambda_handler is called when the AWS Lambda
    is triggered. The function is downloading file 
    from Amazon S3 location and spark submitting 
    the script in AWS Lambda
    """

    logger.info("******************Start AWS Lambda Handler************")
    s3_bucket_script = os.environ['SCRIPT_BUCKET']
    input_script = os.environ['SPARK_SCRIPT']
    os.environ['INPUT_PATH'] = event.get('INPUT_PATH','')
    os.environ['OUTPUT_PATH'] = event.get('OUTPUT_PATH', '')
    
    # Set the environment variables for the Spark application
    spark_submit(s3_bucket_script,input_script, event)
