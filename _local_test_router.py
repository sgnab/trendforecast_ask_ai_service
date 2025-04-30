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

# _local_test_router.py
import json
import logging
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock, call # Import 'call' for checking args

# --- Setup Project Root and Add src to Path ---
# ... (same as before) ...

# --- Load .env ---
# ... (same as before) ...

# --- Configure Logging ---
# ... (same as before) ...

# --- Set Dummy Environment Variables ---
# ... (same as before) ...

# --- Import Handler ---
try:
    from fetch_internal_router_v2 import lambda_handler, TREND_MAIN_LAMBDA_NAME, MEGA_TRENDS_LAMBDA_NAME, CHART_DETAILS_LAMBDA_NAME # Import names too
except ImportError as e: # ... (rest of import error handling) ...
    logger.error(f"Error importing lambda handler: {e}", exc_info=True)
    sys.exit(1)
except Exception as e: # ... (rest of import error handling) ...
    logger.error(f"Error during import or initial module load: {e}", exc_info=True)
    sys.exit(1)

# --- Define Sample Test Event (Output from InterpretQuery_V2) ---
# Using the same event that triggers trend_main and chart_details
test_event_data = {
  "status": "success",
  "primary_task": "get_trend",
  "required_sources": ["internal_trends_category", "web_search"],
  "query_subjects": {
    "specific_known": [{"subject": "Blue", "type": "color"}],
    "unmapped_items": []
  },
  "timeframe_reference": None,
  "attributes": [],
  "clarification_needed": None,
  "original_context": {
    "category": "Shirts",
    "country": "United States",
    "query": "are blue shirts trending in united states"
  }
}

# --- Define Mock Response Payloads ---
# Shortened versions based on your examples

MOCK_TREND_ANALYSIS_PAYLOAD = {
    "country_category": {
        "category_name": "Shirts", "growth_recent": -18.92, "average_volume": 157000,
        "chart_data": [{"date": "2024-12-28 00:00:00", "search_volume": 201000}] # Sample chart data
    },
    "country_category_style": [
        {"style_name": "Athleisure Shirts", "average_volume": 70, "growth_recent": -19, "chart_data": [{"date": "2024-12-28 00:00:00", "search_volume": 90}]},
        {"style_name": "Baggy Shirts", "average_volume": 5467, "growth_recent": -12, "chart_data": [{"date": "2024-12-28 00:00:00", "search_volume": 6600}]}
    ],
    "country_color_category": [
         {"color_name": "Beige Shirts", "average_volume": 4133, "growth_recent": -24, "chart_data": [{"date": "2024-12-28 00:00:00", "search_volume": 6600}]},
         {"color_name": "Black Shirts", "average_volume": 40500, "growth_recent": -4, "chart_data": [{"date": "2024-12-28 00:00:00", "search_volume": 40500}]}
    ]
}

# Mock payload for chart details (for Shirts_Blue)
MOCK_CHART_DETAILS_PAYLOAD = {
    "category_name": "Shirts",
    "category_subject": "Shirts_Blue", # Matches expected request
    "f2": 15, "f3": 20, "f6": 25, # Example forecast data
    "avg2": 18000, "avg3": 19000, "avg6": 20000, # Example avg data
    "growth_recent": 18, # From example
    "average_volume": 15900, # From example
    "chart_data": [ # Example data
        {"date": "2024-12-28 00:00:00", "search_volume": 18100},
        {"date": "2025-01-28 00:00:00", "search_volume": 14800},
        {"date": "2025-02-28 00:00:00", "search_volume": 14800},
        {"date": "2025-03-31 00:00:00", "search_volume": 21446}
    ]
}

# Mock payload if Mega Trends were requested (not used in this specific test_event_data)
MOCK_MEGA_TRENDS_PAYLOAD = {
    "query_category": [
        {"query_name": "pink palm puff pajamas", "category_name": "Pajamas", "category_subject": "Pajamas_pink palm puff pajamas", "average_volume": 60334, "growth_recent": 9049900, "chart_data": [...]},
        {"query_name": "mark hamill pants", "category_name": "Pants", "category_subject": "Pants_mark hamill pants", "average_volume": 7401, "growth_recent": 2219900, "chart_data": [...]}
    ]
}


# --- Call the Handler ---
logger.info(" === Calling fetch_internal_router_v2.lambda_handler locally === ")

# Mock boto3 lambda client
mock_lambda_client = MagicMock()

# Configure the mock's side_effect to return different payloads based on FunctionName
def invoke_side_effect(*args, **kwargs):
    function_name = kwargs.get('FunctionName')
    payload_bytes = b'{}' # Default empty json
    status_code = 200
    function_error = None

    logger.debug(f"Mock invoke called for FunctionName: {function_name}")

    if function_name == TREND_MAIN_LAMBDA_NAME:
        logger.debug(f"Mock returning payload for {TREND_MAIN_LAMBDA_NAME}")
        payload_bytes = json.dumps(MOCK_TREND_ANALYSIS_PAYLOAD).encode('utf-8')
    elif function_name == CHART_DETAILS_LAMBDA_NAME:
         logger.debug(f"Mock returning payload for {CHART_DETAILS_LAMBDA_NAME}")
         payload_bytes = json.dumps(MOCK_CHART_DETAILS_PAYLOAD).encode('utf-8')
    elif function_name == MEGA_TRENDS_LAMBDA_NAME:
         logger.debug(f"Mock returning payload for {MEGA_TRENDS_LAMBDA_NAME}")
         payload_bytes = json.dumps(MOCK_MEGA_TRENDS_PAYLOAD).encode('utf-8')
    else:
        logger.warning(f"Mock invoke called with unexpected FunctionName: {function_name}")
        # Simulate an error for unexpected calls if desired
        # status_code = 404
        # function_error = 'ResourceNotFoundException'
        # payload_bytes = json.dumps({"errorMessage": f"Function {function_name} not found"}).encode('utf-8')

    response = {'StatusCode': status_code, 'Payload': MagicMock(read=MagicMock(return_value=payload_bytes))}
    if function_error:
        response['FunctionError'] = function_error

    return response

mock_lambda_client.invoke.side_effect = invoke_side_effect


# Use patch context manager to replace the global lambda_client during the call
try:
    with patch('fetch_internal_router_v2.lambda_client', mock_lambda_client):
        logger.info("Boto3 Lambda client patched with mock using side_effect.")
        result = lambda_handler(test_event_data, None) # Pass the dictionary directly
        logger.info(" === lambda_handler finished === ")

    # --- Print the Result ---
    print("\n----- Lambda Result -----")
    print(json.dumps(result, indent=2))
    print("----------------------")

    # --- Verify Mock Calls (Optional) ---
    print("\n----- Mock Call Verification -----")
    print(f"lambda_client.invoke called {mock_lambda_client.invoke.call_count} times.")

    # Define expected calls for this specific test event
    expected_calls = [
        call(FunctionName=TREND_MAIN_LAMBDA_NAME, InvocationType='RequestResponse', Payload=json.dumps({'queryStringParameters': {'country': 'United States', 'category': 'Shirts', 'time_frame': '3'}})),
        call(FunctionName=CHART_DETAILS_LAMBDA_NAME, InvocationType='RequestResponse', Payload=json.dumps({'queryStringParameters': {'country': 'United States', 'category_subject': 'Shirts_Blue', 'category': 'Shirts', 'time_frame': '48', 'mode': 'color', 'forecast': 'False'}}))
    ]

    # Check if the actual calls match the expected calls (order might matter depending on implementation)
    # For simplicity, just check counts and print actual calls for manual verification
    # mock_lambda_client.assert_has_calls(expected_calls, any_order=True) # Use this for stricter checking if needed

    for i, actual_call in enumerate(mock_lambda_client.invoke.call_args_list):
        print(f"\nActual Call {i+1}:")
        print(f"  FunctionName: {actual_call.kwargs.get('FunctionName')}")
        payload_str = actual_call.kwargs.get('Payload', '{}')
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