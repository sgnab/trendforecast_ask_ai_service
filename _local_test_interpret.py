# _local_test_interpret.py
import json
import logging
from pathlib import Path
import sys
import os # Needed if not using dotenv

# --- Load .env file if it exists ---
try:
    from dotenv import load_dotenv
    # Load .env from the script's directory (project root)
    dotenv_path = Path(__file__).resolve().parent / '.env'
    if dotenv_path.is_file():
         print(f"Loading .env file from: {dotenv_path}")
         load_dotenv(dotenv_path=dotenv_path, override=True) # Override existing env vars if needed for test
    else:
         print("No .env file found in project root, relying on system environment variables.")
except ImportError:
    print("python-dotenv not installed, relying on system environment variables.")
    # Ensure necessary env vars are set manually if dotenv isn't used


# --- Add src directory to path to find the module ---
project_root = Path(__file__).resolve().parent
src_path = project_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))
    print(f"Added {src_path} to sys.path")

# --- Now import the handler ---
try:
     # Assuming your file is named interpret_query_v2.py in src/
    from interpret_query_v2 import lambda_handler
except ImportError as e:
     print(f"Error importing lambda handler: {e}")
     print(f"Attempted import from: {src_path / 'interpret_query_v2.py'}")
     print("Ensure the handler file exists in src/ and dependencies are installed in .venv.")
     sys.exit(1)
except Exception as e:
     # Catch potential errors during module loading (like failed CSV reads)
     print(f"Error during import or initial module load: {e}")
     sys.exit(1)


# Configure logging AFTER potentially loading .env which might set LOG_LEVEL
# Use basicConfig for simple local testing output to console
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("LocalTestRunner") # Get a specific logger


# --- Define Sample Test Event ---
# Mimic the structure the handler expects (API Gateway proxy format)
# Modify query/category/country for your test case
test_event = {
    "body": json.dumps({
        "query": "What are the latest trends for red polo shirts in the USA?",
        "category": "Shirts",
        "country": "USA"
    }),
    "httpMethod": "POST", # Usually included by API GW, handler might not use it
    "headers": {"Content-Type": "application/json"}, # Usually included by API GW
    "isBase64Encoded": False
}

# --- Call the Handler ---
logger.info(" === Calling lambda_handler locally === ")
# The 'context' argument is often not needed for local testing, pass None
try:
    result = lambda_handler(test_event, None)
    logger.info(" === lambda_handler finished === ")
    # --- Print the Result ---
    print("\n----- Lambda Result -----")
    print(f"Status Code: {result.get('statusCode')}")
    print("Body:")
    try:
        # Try to pretty-print the JSON body
        body_json = json.loads(result.get('body', '{}'))
        print(json.dumps(body_json, indent=2))
    except:
        # Print raw body if not JSON or parse fails
        print(result.get('body'))
    print("----------------------")

except Exception as handler_err:
     logger.exception("Exception occurred during lambda_handler execution!")
     print("\n----- Lambda Execution Error -----")
     print(handler_err)
     print("-----------------------------")