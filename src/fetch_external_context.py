# src/fetch_external_context.py

import json
import logging
import os
import time
from hashlib import sha256
from typing import Dict, Optional, List, Any # Added List, Any

import boto3
from botocore.exceptions import ClientError

# Use try-except for optional Tavily import
try:
    from tavily import TavilyClient
    TAVILY_SDK_AVAILABLE = True
except ImportError:
    TavilyClient = None
    TAVILY_SDK_AVAILABLE = False
    logging.basicConfig(level="ERROR")
    logging.error("CRITICAL: tavily-python SDK not found! Install it (`pip install tavily-python`).")

# --- Configuration ---
CACHE_TABLE_NAME = os.environ.get("CACHE_TABLE_NAME", "TrendForecastAskAiCache")
SECRET_NAME = os.environ.get("SECRET_NAME", "YourSecretsName")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", 3 * 60 * 60)) # 3 hours

# --- Initialize Logger ---
logger = logging.getLogger()
log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
if log_level_str not in valid_log_levels: log_level_str = "INFO"
logger.setLevel(log_level_str)
logger.info(f"Logger initialized with level: {log_level_str}")
logger.info(f"CACHE_TABLE_NAME: {CACHE_TABLE_NAME}")
logger.info(f"CACHE_TTL_SECONDS: {CACHE_TTL_SECONDS}")
logger.info(f"SECRET_NAME: {SECRET_NAME}")

# --- Initialize Boto3 Clients ---
dynamodb_resource = None
secrets_manager = None
BOTO3_CLIENT_ERROR = None
try:
    session = boto3.session.Session()
    dynamodb_resource = session.resource('dynamodb', region_name=AWS_REGION)
    secrets_manager = session.client(service_name='secretsmanager', region_name=AWS_REGION)
    cache_table = dynamodb_resource.Table(CACHE_TABLE_NAME)
    logger.info(f"Initialized DynamoDB table resource for: {CACHE_TABLE_NAME}")
except Exception as e:
    logger.exception("CRITICAL ERROR initializing Boto3 clients!")
    BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 clients: {e}"

# --- API Key Caching ---
API_KEY_CACHE: Dict[str, Optional[str]] = {}

# --- Helper Function to Get Tavily API Key ---
def get_secret_value(secret_name: str, key_name: str) -> Optional[str]:
    """Retrieves Tavily API key from Secrets Manager or ENV."""
    is_local = os.environ.get("IS_LOCAL", "false").lower() == "true"
    if is_local:
        direct_key = os.environ.get(key_name)
        if direct_key: logger.info(f"Using direct env var '{key_name}' (local mode)"); return direct_key
        else: logger.warning(f"Direct env var '{key_name}' not found. Trying Secrets Manager...")

    global API_KEY_CACHE
    cache_key = f"{secret_name}:{key_name}"
    if cache_key in API_KEY_CACHE: logger.debug(f"Using cached secret key: {cache_key}"); return API_KEY_CACHE[cache_key]

    if BOTO3_CLIENT_ERROR: logger.error(f"Boto3 client error: {BOTO3_CLIENT_ERROR}"); return None
    if not secrets_manager: logger.error("Secrets Manager client not initialized."); return None

    try:
        logger.info(f"Fetching secret '{secret_name}' to get key '{key_name}'")
        response = secrets_manager.get_secret_value(SecretId=secret_name)
        secret_dict = None
        if 'SecretString' in response:
            try: secret_dict = json.loads(response['SecretString'])
            except json.JSONDecodeError as e: logger.error(f"Failed JSON parse: {e}"); return None
        else: logger.error("Secret value not found."); return None

        if not isinstance(secret_dict, dict): logger.error("Parsed secret not dict."); return None
        key_value = secret_dict.get(key_name)
        if not key_value or not isinstance(key_value, str):
            logger.error(f"Key '{key_name}' not found/not string in '{secret_name}'.")
            API_KEY_CACHE[cache_key] = None; return None

        API_KEY_CACHE[cache_key] = key_value
        logger.info(f"Key '{key_name}' successfully retrieved and cached.")
        return key_value
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        logger.error(f"AWS ClientError for '{secret_name}': {error_code}")
        API_KEY_CACHE[cache_key] = None; return None
    except Exception as e:
        logger.exception(f"Unexpected error retrieving secret '{secret_name}'.")
        API_KEY_CACHE[cache_key] = None; return None


# --- Main Lambda Handler ---
def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    # --- Initial Checks ---
    if not TAVILY_SDK_AVAILABLE: logger.error("Tavily SDK unavailable."); raise ImportError("Tavily SDK not importable.")
    if BOTO3_CLIENT_ERROR: logger.error(f"Boto3 init failure: {BOTO3_CLIENT_ERROR}"); raise Exception(f"Config Error: {BOTO3_CLIENT_ERROR}")

    # --- 1. Parse Input & Check Requirement ---
    try:
        interpretation_result = event
        required_sources = interpretation_result.get("required_sources", [])
        query_subjects = interpretation_result.get("query_subjects", {})
        original_context = interpretation_result.get("original_context", {})
        user_query_text = original_context.get("query", "")

        if "web_search" not in required_sources:
            logger.info("Web search not required. Skipping.")
            return {"status": "success_skipped", "query_used": None, "answer": None, "results": [], "error": None} # Added answer: None

        logger.info("Web search required.")
    except (TypeError, AttributeError) as e:
        logger.error(f"Failed to parse input: {e}", exc_info=True)
        raise ValueError(f"Invalid input structure: {e}")

    # --- 2. Formulate Search Query ---
    # Simple approach: use original query + category + country + specific items
    specific_subjects_str = ", ".join([item.get('subject', '') for item in query_subjects.get("specific_known", []) if item.get('subject')])
    search_query_parts = [
        user_query_text,
        f"category: {original_context.get('category')}",
        f"country: {original_context.get('country')}",
    ]
    if specific_subjects_str: search_query_parts.append(f"specific items: {specific_subjects_str}")
    search_query = " ".join(filter(None, search_query_parts))
    search_query = search_query[:1000] # Truncate
    logger.info(f"Formulated Tavily search query: {search_query}")

    # --- 3. Check Cache ---
    cache_key = sha256(search_query.encode()).hexdigest()
    logger.debug(f"Using cache key: {cache_key}")
    try:
        response = cache_table.get_item(Key={'search_key': cache_key})
        item = response.get('Item')
        if item and 'ttl' in item and item['ttl'] >= int(time.time()):
            logger.info(f"Cache hit for key: {cache_key}")
            # Load the entire cached response (dict expected)
            cached_response_data = json.loads(item.get('tavily_response_json', '{}'))
            return { # Return structure consistent with API call success
                "status": "success_cached",
                "query_used": search_query,
                "answer": cached_response_data.get("answer"), # Get answer if cached
                "results": cached_response_data.get("results", []), # Get results if cached
                "error": None
            }
        elif item: logger.info(f"Cache expired/TTL missing for key: {cache_key}")
        else: logger.info(f"Cache miss for key: {cache_key}")
    except ClientError as e: logger.error(f"DynamoDB cache read error: {e.response['Error']['Code']}", exc_info=True)
    except Exception as e: logger.exception("Unexpected cache read error.")

    # --- 4. Call Tavily API (Cache Miss/Expired) ---
    logger.info("Calling Tavily API...")
    tavily_api_key = get_secret_value(SECRET_NAME, "TAVILY_API_KEY")
    if not tavily_api_key:
         return {"status": "error", "query_used": search_query, "answer": None, "results": [], "error": "API key config error (Tavily)."}

    tavily_response_data = {} # Store the whole response dict
    error_message = None
    try:
        client = TavilyClient(api_key=tavily_api_key)
        # Using updated parameters
        tavily_response_data = client.search(
            query=search_query,
            search_depth="advanced",
            include_answer="advanced", # <<< Include synthesized answer
            time_range="month",     # <<< Specify time range (adjust if needed)
            max_results=5             # Limit raw results
        )
        answer = tavily_response_data.get("answer")
        results_list = tavily_response_data.get("results", [])
        logger.info(f"Received {len(results_list)} results from Tavily.")
        if answer: logger.info("Tavily provided a synthesized answer.")
        logger.debug(f"Tavily Raw Response Snippet: {str(tavily_response_data)[:500]}...")

    except Exception as e:
        logger.error(f"Tavily API call failed: {e}", exc_info=True)
        error_message = f"Tavily API call failed: {str(e)}"
        return {"status": "error", "query_used": search_query, "answer": None, "results": [], "error": error_message}

    # --- 5. Store Result in Cache ---
    # Cache only if API call was successful (no error) and returned something
    if not error_message and tavily_response_data:
        try:
            ttl_timestamp = int(time.time()) + CACHE_TTL_SECONDS
            logger.info(f"Writing Tavily response to cache with TTL: {ttl_timestamp}")
            # Store the entire response dictionary as JSON string
            cache_table.put_item(
                Item={
                    'search_key': cache_key,
                    'search_query_text': search_query,
                    'tavily_response_json': json.dumps(tavily_response_data), # <<< Store full response
                    'timestamp': int(time.time()),
                    'ttl': ttl_timestamp
                }
            )
            logger.info(f"Successfully wrote response to cache for key: {cache_key}")
        except ClientError as e: logger.error(f"DynamoDB cache write error: {e.response['Error']['Code']}", exc_info=True)
        except Exception as e: logger.exception("Unexpected cache write error.")

    # --- 6. Return Tavily Results ---
    return {
        "status": "success_api",
        "query_used": search_query,
        "answer": tavily_response_data.get("answer"), # <<< Return answer
        "results": tavily_response_data.get("results", []), # <<< Return raw results too
        "error": None
    }