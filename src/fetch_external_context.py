import json
import logging
import os
import time
from hashlib import sha256 # For creating cache keys

import boto3
from botocore.exceptions import ClientError

# Use try-except for optional Tavily import
try:
    from tavily import TavilyClient
    TAVILY_SDK_AVAILABLE = True
except ImportError:
    TavilyClient = None
    TAVILY_SDK_AVAILABLE = False
    # Log error early if SDK is missing
    logging.basicConfig(level="ERROR")
    logging.error("CRITICAL: tavily-python SDK not found! Install it (`pip install tavily-python`).")


# --- Configuration ---
CACHE_TABLE_NAME = os.environ.get("CACHE_TABLE_NAME", "TrendForecastAskAiCache") # DynamoDB table name
SECRET_NAME = os.environ.get("SECRET_NAME", "YourSecretsName") # Secret containing Tavily key
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", 3 * 60 * 60)) # Default cache TTL: 3 hours

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


# --- API Key Caching (similar to interpreter lambda) ---
API_KEY_CACHE: Dict[str, Optional[str]] = {}

# --- Helper Function to Get Tavily API Key ---
# (This is nearly identical to the get_secret_value in interpret_query_v2)
def get_secret_value(secret_name: str, key_name: str) -> Optional[str]:
    """Retrieves Tavily API key from Secrets Manager or ENV."""
    is_local = os.environ.get("IS_LOCAL", "false").lower() == "true"
    if is_local:
        direct_key = os.environ.get(key_name) # Check ENV for TAVILY_API_KEY
        if direct_key:
            logger.info(f"Using direct env var '{key_name}' (local mode)")
            return direct_key
        else: logger.warning(f"Direct env var '{key_name}' not found. Trying Secrets Manager...")

    global API_KEY_CACHE
    cache_key = f"{secret_name}:{key_name}"
    if cache_key in API_KEY_CACHE:
        logger.debug(f"Using cached secret key: {cache_key}")
        return API_KEY_CACHE[cache_key]

    if BOTO3_CLIENT_ERROR: logger.error(f"Boto3 client error: {BOTO3_CLIENT_ERROR}"); return None
    if not secrets_manager: logger.error("Secrets Manager client not initialized."); return None

    try:
        logger.info(f"Fetching secret '{secret_name}' to get key '{key_name}'")
        response = secrets_manager.get_secret_value(SecretId=secret_name)
        secret_dict = None
        if 'SecretString' in response:
            try: secret_dict = json.loads(response['SecretString'])
            except json.JSONDecodeError as e: logger.error(f"Failed JSON parse: {e}"); return None
        # Add binary handling if needed
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
    """
    Fetches external context using Tavily API if required by the interpretation.
    Uses DynamoDB for caching results.
    """
    logger.info(f"Received event: {json.dumps(event)}")

    # --- Initial Checks ---
    if not TAVILY_SDK_AVAILABLE:
        logger.error("Tavily SDK is not available.")
        raise ImportError("Tavily SDK not installed or importable.")
    if BOTO3_CLIENT_ERROR:
        logger.error(f"Boto3 init failure: {BOTO3_CLIENT_ERROR}")
        raise Exception(f"Configuration Error: {BOTO3_CLIENT_ERROR}") # Fail fast

    # --- 1. Parse Input and Check if Web Search is Required ---
    try:
        interpretation_result = event # Expecting output from InterpretQuery_V2
        required_sources = interpretation_result.get("required_sources", [])
        query_subjects = interpretation_result.get("query_subjects", {})
        original_context = interpretation_result.get("original_context", {})
        primary_task = interpretation_result.get("primary_task", "unknown")
        user_query_text = original_context.get("query", "")

        # Check if web search is actually needed
        if "web_search" not in required_sources:
            logger.info("Web search not required by interpretation. Skipping external fetch.")
            # Return empty/null result structure consistent with expected output
            return {
                "status": "success_skipped",
                "query_used": None,
                "results": [],
                "error": None
            }

        logger.info("Web search required. Proceeding with external fetch.")

    except (TypeError, AttributeError) as e:
        logger.error(f"Failed to parse input event: {e}", exc_info=True)
        raise ValueError(f"Invalid input structure: {e}") # Fail fast

    # --- 2. Formulate Search Query for Tavily ---
    # TODO: Refine query formulation logic. Combine context/subjects for a good search query.
    # Simple approach: use original query + category + country + specific items
    specific_subjects_str = ", ".join([item.get('subject', '') for item in query_subjects.get("specific_known", []) if item.get('subject')])
    unmapped_items_str = ", ".join(query_subjects.get("unmapped_items", []))

    search_query_parts = [
        user_query_text,
        f"category: {original_context.get('category')}",
        f"country: {original_context.get('country')}",
    ]
    if specific_subjects_str:
        search_query_parts.append(f"specific items: {specific_subjects_str}")
    if unmapped_items_str:
         search_query_parts.append(f"related terms: {unmapped_items_str}")

    search_query = " ".join(filter(None, search_query_parts)) # Combine non-empty parts
    search_query = search_query[:1000] # Truncate if excessively long

    logger.info(f"Formulated Tavily search query: {search_query}")

    # --- 3. Check Cache ---
    # Use a hash of the search query as the cache key for consistency
    cache_key = sha256(search_query.encode()).hexdigest()
    logger.debug(f"Using cache key: {cache_key}")
    cached_result = None
    try:
        response = cache_table.get_item(Key={'search_key': cache_key})
        item = response.get('Item')
        if item:
            # Check TTL
            if 'ttl' in item and item['ttl'] >= int(time.time()):
                logger.info(f"Cache hit for key: {cache_key}")
                cached_result = json.loads(item.get('search_result_json', 'null')) # Load stored JSON
                # Return cached result directly
                return {
                    "status": "success_cached",
                    "query_used": search_query,
                    "results": cached_result, # Assume stored results match desired format
                    "error": None
                }
            else:
                logger.info(f"Cache expired or TTL missing for key: {cache_key}")
        else:
             logger.info(f"Cache miss for key: {cache_key}")

    except ClientError as e:
        logger.error(f"DynamoDB cache read error: {e.response['Error']['Code']}", exc_info=True)
        # Proceed without cache, but log the error
    except Exception as e:
         logger.exception("Unexpected error during cache read.")
         # Proceed without cache


    # --- 4. Call Tavily API (Cache Miss) ---
    logger.info("Calling Tavily API...")
    tavily_api_key = get_secret_value(SECRET_NAME, "TAVILY_API_KEY")
    if not tavily_api_key:
         # Error already logged by helper
         # Return error structure
         return {
             "status": "error",
             "query_used": search_query,
             "results": [],
             "error": "API key configuration error (Tavily)."
         }

    tavily_results = []
    error_message = None
    try:
        client = TavilyClient(api_key=tavily_api_key)
        # TODO: Determine best Tavily search options (max_results, include_answer, etc.)
        response = client.search(
            query=search_query,
            search_depth="advanced", # Or "basic"
            max_results=5 # Example limit
        )
        # Structure the results as needed for downstream processing
        # Tavily Python SDK returns a dict, often with 'results' key containing a list of dicts
        tavily_results = response.get('results', []) # Extract the list of result objects
        logger.info(f"Received {len(tavily_results)} results from Tavily.")
        logger.debug(f"Tavily Raw Response Snippet: {str(response)[:500]}...")

    except Exception as e:
        logger.error(f"Tavily API call failed: {e}", exc_info=True)
        error_message = f"Tavily API call failed: {str(e)}"
        # Return error structure
        return {
            "status": "error",
            "query_used": search_query,
            "results": [],
            "error": error_message
        }

    # --- 5. Store Result in Cache ---
    if not error_message and tavily_results: # Only cache successful results
        try:
            ttl_timestamp = int(time.time()) + CACHE_TTL_SECONDS
            logger.info(f"Writing results to cache with TTL timestamp: {ttl_timestamp}")
            cache_table.put_item(
                Item={
                    'search_key': cache_key, # Partition key
                    'search_query_text': search_query, # Store original query for reference
                    'search_result_json': json.dumps(tavily_results), # Store results as JSON string
                    'timestamp': int(time.time()),
                    'ttl': ttl_timestamp # DynamoDB TTL attribute
                }
            )
            logger.info(f"Successfully wrote results to cache for key: {cache_key}")
        except ClientError as e:
            logger.error(f"DynamoDB cache write error: {e.response['Error']['Code']}", exc_info=True)
            # Continue without caching, just log error
        except Exception as e:
             logger.exception("Unexpected error during cache write.")
             # Continue without caching

    # --- 6. Return Tavily Results ---
    return {
        "status": "success_api",
        "query_used": search_query,
        "results": tavily_results, # Return the list of result objects
        "error": None # No error in this path
    }