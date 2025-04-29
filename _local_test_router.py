# _local_test_router.py
import json
import logging
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock # To mock boto3 if needed later

# --- Setup Project Root and Add src to Path ---
project_root = Path(__file__).resolve().parent
src_path = project_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))
    print(f"Added {src_path} to sys.path")

# --- Load .env for local environment variables ---
try:
    from dotenv import load_dotenv
    dotenv_path = project_root / '.env'
    if dotenv_path.is_file():
        print(f"Loading .env file from: {dotenv_path}")
        load_dotenv(dotenv_path=dotenv_path, override=True)
    else:
        print("No .env file found, relying on system environment variables.")
except ImportError:
    print("python-dotenv not installed, relying on system environment variables.")

# --- Configure Logging ---
# Configure logging AFTER potentially loading .env which might set LOG_LEVEL
log_level = os.environ.get("LOG_LEVEL", "DEBUG").upper() # Default to DEBUG for testing router
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("LocalTestRouter")

# --- Set Dummy Environment Variables for Target Lambdas ---
# These are needed by fetch_internal_router_v2 during import/init
os.environ['TREND_MAIN_LAMBDA_NAME'] = "dummy_trend_main_lambda"
os.environ['MEGA_TRENDS_LAMBDA_NAME'] = "dummy_mega_trends_lambda"
os.environ['CHART_DETAILS_LAMBDA_NAME'] = "dummy_chart_details_lambda"
# Ensure AWS_REGION is set if not already in .env or system env
os.environ['AWS_REGION'] = os.environ.get('AWS_REGION', 'us-west-2')

logger.info(f"Using Dummy Lambda Names: TREND={os.environ['TREND_MAIN_LAMBDA_NAME']}, MEGA={os.environ['MEGA_TRENDS_LAMBDA_NAME']}, CHART={os.environ['CHART_DETAILS_LAMBDA_NAME']}")
logger.info(f"Using AWS Region: {os.environ['AWS_REGION']}")

# --- Import Handler AFTER setting environment variables ---
try:
    # Assuming your file is named fetch_internal_router_v2.py in src/
    from fetch_internal_router_v2 import lambda_handler
except ImportError as e:
    logger.error(f"Error importing lambda handler: {e}", exc_info=True)
    sys.exit(1)
except Exception as e:
    logger.error(f"Error during import or initial module load: {e}", exc_info=True)
    sys.exit(1)


# --- Define Sample Test Event (Output from InterpretQuery_V2) ---
# Use the actual output from the previous successful test run
test_event_data = {
  "status": "success",
  "primary_task": "get_trend",
  "required_sources": ["internal_trends_category", "web_search"], # Based on actual run
  "query_subjects": {
    "specific_known": [{"subject": "Blue", "type": "color"}], # Based on actual run
    "unmapped_items": [] # Based on actual run
  },
  "timeframe_reference": None, # Based on actual run
  "attributes": [],
  "clarification_needed": None,
  "original_context": {
    "category": "Shirts",
    "country": "United States", # Use full name as tested
    "query": "are blue shirts trending in united states"
  }
}

# --- Call the Handler ---
logger.info(" === Calling fetch_internal_router_v2.lambda_handler locally === ")

# Mocking boto3 lambda client to prevent actual AWS calls
# We only want to verify the logic *before* the invoke call
mock_lambda_client = MagicMock()
mock_invoke_result = MagicMock()
# Simulate a successful invocation response structure (without FunctionError)
# You might need to adjust the payload content based on what the router code expects
# For now, just ensuring it doesn't have 'FunctionError' is enough for basic test.
mock_invoke_result.get.return_value = None # No FunctionError
mock_invoke_result.read.return_value.decode.return_value = json.dumps({"result": "dummy success"}) # Dummy success payload

# Configure the mock invoke method
mock_lambda_client.invoke.return_value = {
    'Payload': MagicMock(read=MagicMock(return_value=json.dumps({"mock_result": "success"}).encode('utf-8'))),
    'StatusCode': 200
}
# Alternatively, to simulate FunctionError:
# mock_lambda_client.invoke.return_value = {
#     'FunctionError': 'Unhandled',
#     'Payload': MagicMock(read=MagicMock(return_value=json.dumps({"errorMessage": "Simulated error", "errorType": "Exception"}).encode('utf-8'))),
#     'StatusCode': 200
# }


# Use patch context manager to replace the global lambda_client during the call
try:
    with patch('fetch_internal_router_v2.lambda_client', mock_lambda_client):
        logger.info("Boto3 Lambda client patched with mock.")
        result = lambda_handler(test_event_data, None) # Pass the dictionary directly
        logger.info(" === lambda_handler finished === ")

    # --- Print the Result ---
    print("\n----- Lambda Result -----")
    print(json.dumps(result, indent=2)) # Pretty print the result dict
    print("----------------------")

    # --- Verify Mock Calls (Optional) ---
    print("\n----- Mock Call Verification -----")
    # How many times was invoke called? Should match expected triggers.
    print(f"lambda_client.invoke called {mock_lambda_client.invoke.call_count} times.")
    # Print details of each call
    for i, call in enumerate(mock_lambda_client.invoke.call_args_list):
        print(f"\nCall {i+1}:")
        print(f"  FunctionName: {call.kwargs.get('FunctionName')}")
        payload_str = call.kwargs.get('Payload', '{}')
        try:
            payload_dict = json.loads(payload_str)
            print(f"  Payload:\n{json.dumps(payload_dict, indent=4)}")
        except json.JSONDecodeError:
            print(f"  Payload (raw): {payload_str}")
    print("-----------------------------")


except Exception as handler_err:
    logger.exception("Exception occurred during lambda_handler execution!")
    print("\n----- Lambda Execution Error -----")
    print(handler_err)
    print("-----------------------------")