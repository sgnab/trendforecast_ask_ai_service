# _local_test_external.py
import json
import logging
import os
import sys
import time
from pathlib import Path
from unittest.mock import patch, MagicMock
from typing import Dict, Optional # Add Dict and Optional here

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
log_level = os.environ.get("LOG_LEVEL", "DEBUG").upper() # Default to DEBUG for testing
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("LocalTestExternal")

# --- Set Environment Variables for the Lambda ---

# These are needed by fetch_external_context during import/init
os.environ['CACHE_TABLE_NAME'] = os.environ.get('CACHE_TABLE_NAME', 'TrendForecastAskAiCache') # Use real name or override
os.environ['SECRET_NAME'] = os.environ.get('SECRET_NAME', 'YourSecretsName') # Ensure this matches .env or set here
os.environ['AWS_REGION'] = os.environ.get('AWS_REGION', 'us-west-2')
os.environ['IS_LOCAL'] = 'true' # Enable direct reading of TAVILY_API_KEY from .env
# Ensure TAVILY_API_KEY is set in your .env file for the get_secret_value helper

logger.info(f"Using Cache Table: {os.environ['CACHE_TABLE_NAME']}")
logger.info(f"Using Secret Name: {os.environ['SECRET_NAME']}")
logger.info(f"Using AWS Region: {os.environ['AWS_REGION']}")
logger.info(f"IS_LOCAL set to: {os.environ['IS_LOCAL']}")
# Check if Tavily key is likely present (don't log the key itself)
if os.environ.get('TAVILY_API_KEY'):
    logger.info("TAVILY_API_KEY environment variable found.")
else:
    logger.warning("TAVILY_API_KEY environment variable NOT found. Ensure it's in .env for local testing.")


# --- Import Handler AFTER setting environment variables ---
try:
    # Assuming your file is named fetch_external_context.py in src/
    from fetch_external_context import lambda_handler
except ImportError as e:
    # Handle case where Tavily SDK might be missing
    if "tavily" in str(e).lower():
         logger.error(f"Error importing handler: {e}. Is the tavily-python package installed (`pip install tavily-python`)?", exc_info=True)
    else:
        logger.error(f"Error importing handler: {e}", exc_info=True)
    sys.exit(1)
except Exception as e:
    logger.error(f"Error during import or initial module load: {e}", exc_info=True)
    sys.exit(1)


# --- Define Sample Test Event (Output from InterpretQuery_V2) ---
# Ensure "web_search" is in required_sources to trigger the logic
test_event_data = {
  "status": "success",
  "primary_task": "get_trend",
  "required_sources": ["internal_trends_category", "web_search"], # <<< web_search included
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

# --- Mock Configuration ---

# Mock DynamoDB Table Resource
# We patch 'fetch_external_context.cache_table' which is created during module init
mock_ddb_table = MagicMock()

# Mock Tavily Client
# We patch 'fetch_external_context.TavilyClient' itself
mock_tavily_client_instance = MagicMock()
mock_tavily_client_constructor = MagicMock(return_value=mock_tavily_client_instance)

# --- Test Scenario Functions ---
# Define functions to configure mocks for different scenarios

def configure_mocks_cache_miss(mock_ddb, mock_tavily):
    """Simulate cache miss and successful Tavily API call."""
    logger.info("Configuring mocks for: CACHE MISS, API SUCCESS")
    # Cache Miss: get_item returns no 'Item'
    mock_ddb.get_item.return_value = {}
    # API Success: search returns some results
    mock_tavily.search.return_value = {
        "query": "Mocked search query",
        "results": [
            {"title": "Mock Result 1", "url": "http://example.com/1", "content": "Content 1..."},
            {"title": "Mock Result 2", "url": "http://example.com/2", "content": "Content 2..."}
        ]
    }
    # Cache Write Success (Optional Check): put_item returns standard success response
    mock_ddb.put_item.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}

def configure_mocks_cache_hit(mock_ddb, mock_tavily):
    """Simulate cache hit."""
    logger.info("Configuring mocks for: CACHE HIT")
    # Cache Hit: get_item returns item with valid TTL and data
    cached_data = [
        {"title": "Cached Result A", "url": "http://cache.example.com/a", "content": "Cached content A..."},
        {"title": "Cached Result B", "url": "http://cache.example.com/b", "content": "Cached content B..."}
    ]
    mock_ddb.get_item.return_value = {
        'Item': {
            'search_key': 'dummy_hash_key',
            'search_result_json': json.dumps(cached_data),
            'ttl': int(time.time()) + 3600 # TTL is 1 hour in the future
        }
    }
    # Reset Tavily mock just in case (it shouldn't be called on cache hit)
    mock_tavily.search.reset_mock()

def configure_mocks_cache_expired(mock_ddb, mock_tavily):
    """Simulate cache hit but TTL is expired."""
    logger.info("Configuring mocks for: CACHE HIT (EXPIRED), API SUCCESS")
    # Cache Expired: get_item returns item with past TTL
    cached_data = [{"title": "Expired Cache", "url": "http://expired.com", "content": "Expired..."}]
    mock_ddb.get_item.return_value = {
        'Item': {
            'search_key': 'dummy_hash_key',
            'search_result_json': json.dumps(cached_data),
            'ttl': int(time.time()) - 3600 # TTL is 1 hour in the past
        }
    }
    # API Success: search returns fresh results
    mock_tavily.search.return_value = {
        "query": "Mocked search query",
        "results": [
            {"title": "Fresh Result 1", "url": "http://fresh.example.com/1", "content": "Fresh Content 1..."}
        ]
    }
    mock_ddb.put_item.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}

def configure_mocks_api_error(mock_ddb, mock_tavily):
    """Simulate cache miss and Tavily API call failure."""
    logger.info("Configuring mocks for: CACHE MISS, API ERROR")
    # Cache Miss
    mock_ddb.get_item.return_value = {}
    # API Error: search raises an exception
    mock_tavily.search.side_effect = Exception("Simulated Tavily API Error (e.g., network timeout)")


# --- Run Test Scenario ---
def run_test(scenario_config_func):
    logger.info(f"\n===== Running Test Scenario: {scenario_config_func.__name__} =====")

    # Use patch context managers to replace dependencies during the call
    # Patch the TavilyClient constructor and the cache_table instance
    with patch('fetch_external_context.cache_table', mock_ddb_table), \
         patch('fetch_external_context.TavilyClient', mock_tavily_client_constructor):

        # Reset mocks before configuring for the specific scenario
        mock_ddb_table.reset_mock()
        mock_tavily_client_constructor.reset_mock()
        mock_tavily_client_instance.reset_mock()

        # Configure mocks for the current scenario
        scenario_config_func(mock_ddb_table, mock_tavily_client_instance)

        logger.info("Dependencies patched. Calling lambda_handler...")
        try:
            result = lambda_handler(test_event_data, None) # Pass the dictionary directly
            logger.info("lambda_handler finished.")

            # --- Print the Result ---
            print("\n----- Lambda Result -----")
            print(json.dumps(result, indent=2))
            print("----------------------")

            # --- Verify Mock Calls ---
            print("\n----- Mock Call Verification -----")
            # DynamoDB get_item should always be called if web_search is required
            print(f"DynamoDB get_item called: {mock_ddb_table.get_item.call_count > 0}")
            if mock_ddb_table.get_item.call_count > 0:
                 print(f"  get_item Key: {mock_ddb_table.get_item.call_args.kwargs.get('Key')}")

            # TavilyClient constructor should be called only on cache miss/expired
            print(f"TavilyClient() called: {mock_tavily_client_constructor.call_count > 0}")
            # Tavily search should be called only on cache miss/expired
            print(f"TavilyClient.search called: {mock_tavily_client_instance.search.call_count > 0}")
            if mock_tavily_client_instance.search.call_count > 0:
                 print(f"  search query: {mock_tavily_client_instance.search.call_args.kwargs.get('query')}")

            # DynamoDB put_item should be called only on cache miss/expired AND successful API call
            print(f"DynamoDB put_item called: {mock_ddb_table.put_item.call_count > 0}")
            if mock_ddb_table.put_item.call_count > 0:
                 print(f"  put_item contains 'ttl': {'ttl' in mock_ddb_table.put_item.call_args.kwargs.get('Item', {})}")

            print("-----------------------------")


        except Exception as handler_err:
            logger.exception("Exception occurred during lambda_handler execution!")
            print("\n----- Lambda Execution Error -----")
            print(handler_err)
            print("-----------------------------")
    logger.info(f"===== Finished Test Scenario: {scenario_config_func.__name__} =====")


# --- Execute Different Scenarios ---
if __name__ == "__main__":
    run_test(configure_mocks_cache_miss)
    run_test(configure_mocks_cache_hit)
    run_test(configure_mocks_cache_expired)
    run_test(configure_mocks_api_error)

    # Add a test case where web_search is NOT required
    logger.info("\n===== Running Test Scenario: Web Search Not Required =====")
    event_no_web_search = test_event_data.copy()
    event_no_web_search["required_sources"] = ["internal_trends_category"] # Remove web_search
    try:
         result = lambda_handler(event_no_web_search, None)
         print("\n----- Lambda Result (Web Search Not Required) -----")
         print(json.dumps(result, indent=2))
         print("----------------------")
         # Verify status is 'success_skipped'
         assert result.get("status") == "success_skipped"
         logger.info("Web search skipped test successful.")
    except Exception as e:
         logger.error(f"Error during 'web search not required' test: {e}", exc_info=True)
    logger.info("===== Finished Test Scenario: Web Search Not Required =====")