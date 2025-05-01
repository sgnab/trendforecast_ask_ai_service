# src/sfn_proxy_lambda.py
import json
import logging
import os
import boto3
from botocore.exceptions import ClientError

# --- Configuration ---
# Read State Machine ARN from environment variable
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2") # Get region

# --- Initialize Logger ---
logger = logging.getLogger()
log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
if log_level_str not in valid_log_levels: log_level_str = "INFO"
logger.setLevel(log_level_str)
logger.info(f"Logger initialized with level: {log_level_str}")
logger.info(f"Target State Machine ARN: {STATE_MACHINE_ARN}")

# --- Initialize Boto3 SFN Client ---
sfn_client = None
BOTO3_CLIENT_ERROR = None
try:
    session = boto3.session.Session()
    sfn_client = session.client(service_name='stepfunctions', region_name=AWS_REGION)
except Exception as e:
    logger.exception("CRITICAL ERROR initializing Boto3 Step Functions client!")
    BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 SFN client: {e}"


# --- Main Lambda Handler ---
def lambda_handler(event, context):
    """
    Receives request from API Gateway, triggers Step Function synchronously,
    parses the output string, and returns the result object.
    """
    logger.debug(f"Proxy received event: {json.dumps(event)}")

    # --- Initial Checks ---
    if BOTO3_CLIENT_ERROR:
        logger.error(f"Boto3 init failure: {BOTO3_CLIENT_ERROR}")
        return {"statusCode": 500, "body": json.dumps({"error": "Configuration Error", "message": BOTO3_CLIENT_ERROR})}
    if not STATE_MACHINE_ARN:
        logger.error("STATE_MACHINE_ARN environment variable not set.")
        return {"statusCode": 500, "body": json.dumps({"error": "Configuration Error", "message": "State Machine ARN not configured."})}

    # --- Extract Input for Step Function ---
    # API Gateway HTTP API (Lambda Proxy Integration assumed) passes body under 'body' key
    sfn_input_string = event.get("body", "{}") # Default to empty object string if body is missing

    # --- Call Step Function StartSyncExecution ---
    try:
        logger.info(f"Starting sync execution for {STATE_MACHINE_ARN}")
        logger.debug(f"Step Function Input String: {sfn_input_string}")

        response = sfn_client.start_sync_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=sfn_input_string # Input must be a JSON string
        )

        logger.info(f"Step Function execution status: {response.get('status')}")

        # Check for execution errors reported by StartSyncExecution
        if response.get('status') == 'FAILED':
            error = response.get('error', 'UnknownError')
            cause = response.get('cause', '{}')
            logger.error(f"Step Function execution failed. Error: {error}, Cause: {cause}")
            # Try to parse cause as JSON for better error reporting
            try:
                cause_details = json.loads(cause)
            except json.JSONDecodeError:
                cause_details = cause # Keep as string if not JSON
            # Return a 500 error, passing along SFN error info
            return {
                "statusCode": 500,
                "body": json.dumps({
                    "error": f"WorkflowExecutionError: {error}",
                    "cause": cause_details
                })
            }
        elif response.get('status') != 'SUCCEEDED':
             # Handle other statuses like TIMED_OUT, ABORTED if necessary
             logger.error(f"Step Function execution ended with unexpected status: {response.get('status')}")
             return {
                "statusCode": 500,
                "body": json.dumps({
                    "error": "WorkflowError",
                    "message": f"Workflow ended with status: {response.get('status')}"
                })
             }

        # --- Parse the Stringified Output from Step Function ---
        sfn_output_string = response.get('output', '{}')
        logger.debug(f"Step Function raw output string: {sfn_output_string}")

        try:
            final_output_object = json.loads(sfn_output_string)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Step Function output string: {e}")
            logger.error(f"Raw output was: {sfn_output_string}")
            return {
                 "statusCode": 500,
                 "body": json.dumps({
                     "error": "OutputParsingError",
                     "message": "Failed to parse final workflow output."
                 })
             }

        # --- Return Parsed Object ---
        # API Gateway Lambda Proxy integration will automatically stringify this dictionary
        logger.info("Successfully executed workflow and parsed output.")
        return {
            "statusCode": 200,
            "body": json.dumps(final_output_object) # Return the already-parsed JSON for Bubble
        }

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        logger.error(f"Boto3 ClientError calling StartSyncExecution: {error_code}", exc_info=True)
        return {"statusCode": 502, "body": json.dumps({"error": "AWS API Error", "message": f"Failed to start workflow: {error_code}"})}
    except Exception as e:
        logger.exception("Unexpected error during Step Function invocation.")
        return {"statusCode": 500, "body": json.dumps({"error": "Internal Server Error", "message": str(e)})}