# # src/fetch_external_context.py
#
# import json
# import logging
# import os
# import time
# from hashlib import sha256
# from typing import Dict, Optional, List, Any # Added List, Any
#
# import boto3
# from botocore.exceptions import ClientError
#
# # Use try-except for optional Tavily import
# try:
#     from tavily import TavilyClient
#     TAVILY_SDK_AVAILABLE = True
# except ImportError:
#     TavilyClient = None
#     TAVILY_SDK_AVAILABLE = False
#     logging.basicConfig(level="ERROR")
#     logging.error("CRITICAL: tavily-python SDK not found! Install it (`pip install tavily-python`).")
#
# # --- Configuration ---
# CACHE_TABLE_NAME = os.environ.get("CACHE_TABLE_NAME", "TrendForecastAskAiCache")
# SECRET_NAME = os.environ.get("SECRET_NAME", "YourSecretsName")
# AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
# CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", 3 * 60 * 60)) # 3 hours
#
# # --- Initialize Logger ---
# logger = logging.getLogger()
# log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
# valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
# if log_level_str not in valid_log_levels: log_level_str = "INFO"
# logger.setLevel(log_level_str)
# logger.info(f"Logger initialized with level: {log_level_str}")
# logger.info(f"CACHE_TABLE_NAME: {CACHE_TABLE_NAME}")
# logger.info(f"CACHE_TTL_SECONDS: {CACHE_TTL_SECONDS}")
# logger.info(f"SECRET_NAME: {SECRET_NAME}")
#
# # --- Initialize Boto3 Clients ---
# dynamodb_resource = None
# secrets_manager = None
# BOTO3_CLIENT_ERROR = None
# try:
#     session = boto3.session.Session()
#     dynamodb_resource = session.resource('dynamodb', region_name=AWS_REGION)
#     secrets_manager = session.client(service_name='secretsmanager', region_name=AWS_REGION)
#     cache_table = dynamodb_resource.Table(CACHE_TABLE_NAME)
#     logger.info(f"Initialized DynamoDB table resource for: {CACHE_TABLE_NAME}")
# except Exception as e:
#     logger.exception("CRITICAL ERROR initializing Boto3 clients!")
#     BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 clients: {e}"
#
# # --- API Key Caching ---
# API_KEY_CACHE: Dict[str, Optional[str]] = {}
#
# # --- Helper Function to Get Tavily API Key ---
# def get_secret_value(secret_name: str, key_name: str) -> Optional[str]:
#     """Retrieves Tavily API key from Secrets Manager or ENV."""
#     is_local = os.environ.get("IS_LOCAL", "false").lower() == "true"
#     if is_local:
#         direct_key = os.environ.get(key_name)
#         if direct_key: logger.info(f"Using direct env var '{key_name}' (local mode)"); return direct_key
#         else: logger.warning(f"Direct env var '{key_name}' not found. Trying Secrets Manager...")
#
#     global API_KEY_CACHE
#     cache_key = f"{secret_name}:{key_name}"
#     if cache_key in API_KEY_CACHE: logger.debug(f"Using cached secret key: {cache_key}"); return API_KEY_CACHE[cache_key]
#
#     if BOTO3_CLIENT_ERROR: logger.error(f"Boto3 client error: {BOTO3_CLIENT_ERROR}"); return None
#     if not secrets_manager: logger.error("Secrets Manager client not initialized."); return None
#
#     try:
#         logger.info(f"Fetching secret '{secret_name}' to get key '{key_name}'")
#         response = secrets_manager.get_secret_value(SecretId=secret_name)
#         secret_dict = None
#         if 'SecretString' in response:
#             try: secret_dict = json.loads(response['SecretString'])
#             except json.JSONDecodeError as e: logger.error(f"Failed JSON parse: {e}"); return None
#         else: logger.error("Secret value not found."); return None
#
#         if not isinstance(secret_dict, dict): logger.error("Parsed secret not dict."); return None
#         key_value = secret_dict.get(key_name)
#         if not key_value or not isinstance(key_value, str):
#             logger.error(f"Key '{key_name}' not found/not string in '{secret_name}'.")
#             API_KEY_CACHE[cache_key] = None; return None
#
#         API_KEY_CACHE[cache_key] = key_value
#         logger.info(f"Key '{key_name}' successfully retrieved and cached.")
#         return key_value
#     except ClientError as e:
#         error_code = e.response.get("Error", {}).get("Code")
#         logger.error(f"AWS ClientError for '{secret_name}': {error_code}")
#         API_KEY_CACHE[cache_key] = None; return None
#     except Exception as e:
#         logger.exception(f"Unexpected error retrieving secret '{secret_name}'.")
#         API_KEY_CACHE[cache_key] = None; return None
#
#
# # --- Main Lambda Handler ---
# def lambda_handler(event, context):
#     logger.info(f"Received event: {json.dumps(event)}")
#
#     # --- Initial Checks ---
#     if not TAVILY_SDK_AVAILABLE: logger.error("Tavily SDK unavailable."); raise ImportError("Tavily SDK not importable.")
#     if BOTO3_CLIENT_ERROR: logger.error(f"Boto3 init failure: {BOTO3_CLIENT_ERROR}"); raise Exception(f"Config Error: {BOTO3_CLIENT_ERROR}")
#
#     # --- 1. Parse Input & Check Requirement ---
#     try:
#         primary_task = interpretation_result.get("primary_task")
#         interpretation_result = event
#         required_sources = interpretation_result.get("required_sources", [])
#         query_subjects = interpretation_result.get("query_subjects", {})
#         original_context = interpretation_result.get("original_context", {})
#         user_query_text = original_context.get("query", "")
#
#         if "web_search" not in required_sources:
#             logger.info("Web search not required. Skipping.")
#             return {"status": "success_skipped", "query_used": None, "answer": None, "results": [], "error": None} # Added answer: None
#
#         logger.info("Web search required.")
#     except (TypeError, AttributeError) as e:
#         logger.error(f"Failed to parse input: {e}", exc_info=True)
#         raise ValueError(f"Invalid input structure: {e}")
#
#     # --- 2. Formulate Search Query ---
#     # Simple approach: use original query + category + country + specific items
#     specific_subjects_str = ", ".join([item.get('subject', '') for item in query_subjects.get("specific_known", []) if item.get('subject')])
#     if primary_task !="summarize_web_trends":
#         search_query_parts = [
#             user_query_text,
#             f"category: {original_context.get('category')}",
#             f"country: {original_context.get('country')}",
#         ]
#         if specific_subjects_str: search_query_parts.append(f"specific items: {specific_subjects_str}")
#         search_query = " ".join(filter(None, search_query_parts))
#         try:
#             search_query = search_query.replace("Request: ", "").strip()
#         except:
#             pass
#     else:
#         search_query_parts = [
#             user_query_text
#         ]
#         search_query = " ".join(filter(None, search_query_parts))
#         try:
#             search_query = search_query.replace("Request: ", "").strip()
#         except:
#             pass
#     search_query = search_query[:1000] # Truncate
#     logger.info(f"Formulated Tavily search query: {search_query}")
#     # primary_task = interpretation_result.get("primary_task")  # Get the task indicator
#     #
#     # search_query = ""  # Initialize search_query
#     #
#     # if primary_task == "summarize_web_trends":
#     #     # For general web trends, the user_query_text should be sufficient.
#     #     # It's already formatted like "Request: General fashion trends in United States"
#     #     search_query = user_query_text
#     #     # Optional: Clean up the "Request:" part if Tavily doesn't like it
#     #     search_query = search_query.replace("Request: ", "").strip()
#     #     logger.info("Using refined query directly for general web search.")
#     #
#     # elif primary_task == "analyze_brand_deep_dive":
#     #     # For brand analysis, combine the query with brand and country context
#     #     target_brand = query_subjects.get("target_brand", "the requested brand")
#     #     # Use the original user query text which might have more context than just the brand name
#     #     # Append country for localization if needed
#     #     search_query = f"{user_query_text} {original_context.get('country')}"
#     #     logger.info("Using user query + country for brand analysis web search.")
#     #
#     # # Add elif for other specific tasks needing custom queries if necessary...
#     #
#     # else:  # Default formulation for standard category/product queries
#     #     logger.info("Using standard query formulation with category/country/subjects.")
#     #     specific_subjects_str = ", ".join(
#     #         [item.get('subject', '') for item in query_subjects.get("specific_known", []) if item.get('subject')])
#     #     search_query_parts = [
#     #         user_query_text,
#     #         # DO NOT add placeholder category/country if they are already in user_query_text
#     #         # Let's keep it simple for now and focus on the user_query_text for standard tasks too,
#     #         # as the Interpreter LLM should ideally make it specific enough.
#     #         # We can add back category/country/subjects if needed for better results.
#     #         # f"category: {original_context.get('category')}",
#     #         # f"country: {original_context.get('country')}",
#     #     ]
#     #     # Maybe just add specific items if found?
#     #     if specific_subjects_str:
#     #         search_query_parts.append(f"focus on: {specific_subjects_str}")
#     #     search_query = " ".join(filter(None, search_query_parts))
#     #     # If search query is still empty after joining, default to user_query_text
#     #     if not search_query.strip():
#     #         search_query = user_query_text
#     #
#     # # Final cleanup and truncation
#     # search_query = search_query.strip()[:1000]
#     # logger.info(f"Formulated Tavily search query (Task: {primary_task}): {search_query}")
#
#     # --- 3. Check Cache ---
#     cache_key = sha256(search_query.encode()).hexdigest()
#     logger.debug(f"Using cache key: {cache_key}")
#     try:
#         response = cache_table.get_item(Key={'search_key': cache_key})
#         item = response.get('Item')
#         if item and 'ttl' in item and item['ttl'] >= int(time.time()):
#             logger.info(f"Cache hit for key: {cache_key}")
#             # Load the entire cached response (dict expected)
#             cached_response_data = json.loads(item.get('tavily_response_json', '{}'))
#             return { # Return structure consistent with API call success
#                 "status": "success_cached",
#                 "query_used": search_query,
#                 "answer": cached_response_data.get("answer"), # Get answer if cached
#                 "results": cached_response_data.get("results", []), # Get results if cached
#                 "error": None
#             }
#         elif item: logger.info(f"Cache expired/TTL missing for key: {cache_key}")
#         else: logger.info(f"Cache miss for key: {cache_key}")
#     except ClientError as e: logger.error(f"DynamoDB cache read error: {e.response['Error']['Code']}", exc_info=True)
#     except Exception as e: logger.exception("Unexpected cache read error.")
#
#     # --- 4. Call Tavily API (Cache Miss/Expired) ---
#     logger.info("Calling Tavily API...")
#     tavily_api_key = get_secret_value(SECRET_NAME, "TAVILY_API_KEY")
#     if not tavily_api_key:
#          return {"status": "error", "query_used": search_query, "answer": None, "results": [], "error": "API key config error (Tavily)."}
#
#     tavily_response_data = {} # Store the whole response dict
#     error_message = None
#     try:
#         client = TavilyClient(api_key=tavily_api_key)
#         # Using updated parameters
#         tavily_response_data = client.search(
#             query=search_query,
#             search_depth="advanced",
#             include_answer="advanced", # <<< Include synthesized answer
#             time_range="month",     # <<< Specify time range (adjust if needed)
#             max_results=5             # Limit raw results
#         )
#         answer = tavily_response_data.get("answer")
#         results_list = tavily_response_data.get("results", [])
#         logger.info(f"Received {len(results_list)} results from Tavily.")
#         if answer: logger.info("Tavily provided a synthesized answer.")
#         logger.debug(f"Tavily Raw Response Snippet: {str(tavily_response_data)[:500]}...")
#
#     except Exception as e:
#         logger.error(f"Tavily API call failed: {e}", exc_info=True)
#         error_message = f"Tavily API call failed: {str(e)}"
#         return {"status": "error", "query_used": search_query, "answer": None, "results": [], "error": error_message}
#
#     # --- 5. Store Result in Cache ---
#     # Cache only if API call was successful (no error) and returned something
#     if not error_message and tavily_response_data:
#         try:
#             ttl_timestamp = int(time.time()) + CACHE_TTL_SECONDS
#             logger.info(f"Writing Tavily response to cache with TTL: {ttl_timestamp}")
#             # Store the entire response dictionary as JSON string
#             cache_table.put_item(
#                 Item={
#                     'search_key': cache_key,
#                     'search_query_text': search_query,
#                     'tavily_response_json': json.dumps(tavily_response_data), # <<< Store full response
#                     'timestamp': int(time.time()),
#                     'ttl': ttl_timestamp
#                 }
#             )
#             logger.info(f"Successfully wrote response to cache for key: {cache_key}")
#         except ClientError as e: logger.error(f"DynamoDB cache write error: {e.response['Error']['Code']}", exc_info=True)
#         except Exception as e: logger.exception("Unexpected cache write error.")
#
#     # --- 6. Return Tavily Results ---
#     return {
#         "status": "success_api",
#         "query_used": search_query,
#         "answer": tavily_response_data.get("answer"), # <<< Return answer
#         "results": tavily_response_data.get("results", []), # <<< Return raw results too
#         "error": None
#     }

import json
import logging
import os
import time
from hashlib import sha256
from typing import Dict, Optional, List, Any
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation # Needed for replace_decimals
import re

# --- Configuration ---
CACHE_TABLE_NAME = os.environ.get("CACHE_TABLE_NAME", "TrendForecastAskAiCache")
SECRET_NAME = os.environ.get("SECRET_NAME", "YourSecretsName") # Match case from your file
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", 3 * 60 * 60))

# --- SDK Check (Do this early) ---
try:
    from tavily import TavilyClient
    TAVILY_SDK_AVAILABLE = True
except ImportError:
    TavilyClient = None
    TAVILY_SDK_AVAILABLE = False
    # Log but don't prevent basic logging setup
    logging.warning("CRITICAL: tavily-python SDK not found! Install it (`pip install tavily-python`). Web search will fail.")

# --- Logger Setup ---
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
cache_table = None
BOTO3_CLIENT_ERROR = None
DDB_RESOURCE_AVAILABLE = False # Track availability
try:
    session = boto3.session.Session()
    dynamodb_resource = session.resource('dynamodb', region_name=AWS_REGION)
    secrets_manager = session.client(service_name='secretsmanager', region_name=AWS_REGION)
    if dynamodb_resource: # Check if resource init succeeded
         cache_table = dynamodb_resource.Table(CACHE_TABLE_NAME)
         cache_table.load() # Check table connection
         logger.info(f"Initialized DynamoDB table resource for: {CACHE_TABLE_NAME}")
         DDB_RESOURCE_AVAILABLE = True
    else:
         BOTO3_CLIENT_ERROR = "Failed to initialize DynamoDB resource."
except Exception as e:
    logger.exception("CRITICAL ERROR initializing Boto3 clients!")
    BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 clients: {e}"
    DDB_RESOURCE_AVAILABLE = False

# --- API Key Caching ---
API_KEY_CACHE: Dict[str, Optional[str]] = {}

# --- Helper Function to Get Tavily API Key ---
def get_secret_value(secret_name: str, key_name: str) -> Optional[str]:
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
        else: logger.error("Secret value 'SecretString' not found."); return None # Assume string secret

        if not isinstance(secret_dict, dict): logger.error("Parsed secret not dict."); return None
        key_value = secret_dict.get(key_name)
        if not key_value or not isinstance(key_value, str):
            logger.error(f"Key '{key_name}' not found/not string in '{secret_name}'."); API_KEY_CACHE[cache_key] = None; return None

        API_KEY_CACHE[cache_key] = key_value; logger.info(f"Key '{key_name}' successfully retrieved and cached."); return key_value
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code"); logger.error(f"AWS ClientError for '{secret_name}': {error_code}"); API_KEY_CACHE[cache_key] = None; return None
    except Exception as e:
        logger.exception(f"Unexpected error retrieving secret '{secret_name}'."); API_KEY_CACHE[cache_key] = None; return None

# --- Helper to clean domain ---
def clean_domain(input_domain: str) -> str:
    if not isinstance(input_domain, str): return ""
    cleaned = re.sub(r'^https?:\/\/', '', input_domain.strip(), flags=re.IGNORECASE)
    cleaned = re.sub(r'^www\.', '', cleaned, flags=re.IGNORECASE)
    if cleaned.endswith('/'): cleaned = cleaned[:-1]
    return cleaned.lower()

# --- Helper to convert Decimals ---
def replace_decimals(obj):
    if isinstance(obj, list): return [replace_decimals(x) for x in obj]
    elif isinstance(obj, dict): return {k: replace_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal): return float(obj) if obj % 1 != 0 else int(obj)
    else: return obj


# --- Main Lambda Handler ---
def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    # --- Initial Checks ---
    if not TAVILY_SDK_AVAILABLE:
        logger.error("Tavily SDK unavailable.")
        return {"status": "error", "query_used": None, "answer": None, "results": [], "error": "Configuration Error: Tavily SDK missing."}
    if BOTO3_CLIENT_ERROR or not DDB_RESOURCE_AVAILABLE:
        logger.error(f"Boto3 init failure or DDB unavailable: {BOTO3_CLIENT_ERROR}")
        return {"status": "error", "query_used": None, "answer": None, "results": [], "error": f"Configuration Error: {BOTO3_CLIENT_ERROR}"}

    # --- 1. Parse Input & Check Requirement ---
    # Define variables with default values BEFORE try block
    interpretation_result = None
    required_sources = []
    query_subjects = {}
    original_context = {}
    user_query_text = ""
    primary_task = None
    target_brand = None

    try:
        interpretation_result = event # Assume event IS the interpretation result
        if not isinstance(interpretation_result, dict):
            raise ValueError("Input event is not a dictionary.")

        required_sources = interpretation_result.get("required_sources", [])
        if not isinstance(required_sources, list): # Ensure it's a list
             required_sources = []

        query_subjects = interpretation_result.get("query_subjects", {})
        original_context = interpretation_result.get("original_context", {})
        user_query_text = original_context.get("query", "") # Use original query from context
        primary_task = interpretation_result.get("primary_task")
        target_brand = query_subjects.get("target_brand") # Needed for query formulation

        # Check if web search is actually needed
        if "web_search" not in required_sources:
            logger.info("Web search not required by Interpreter. Skipping.")
            return {"status": "success_skipped", "query_used": None, "answer": None, "results": [], "error": None}

        logger.info("Web search required. Proceeding...")

    except (TypeError, AttributeError, ValueError, KeyError) as e:
        logger.error(f"Failed to parse input event structure: {e}", exc_info=True)
        # Return error in expected format
        return {"status": "error", "query_used": None, "answer": None, "results": [], "error": f"Invalid input structure: {e}"}

    # --- 2. Formulate Search Query (Using variables defined above) ---
    search_query = "" # Initialize search_query
    try:
        if primary_task == "summarize_web_trends":
            search_query = user_query_text.replace("Request:", "").strip()
            logger.info("Using refined query directly for general web search.")
        elif primary_task == "analyze_brand_deep_dive":
            brand = target_brand or "the brand"
            search_query = f"{user_query_text} {original_context.get('country', '')}" # Keep it simple
            logger.info("Using user query + country for brand analysis web search.")
        else: # Default formulation
            logger.info("Using standard query formulation.")
            specific_subjects_str = ", ".join([item.get('subject', '') for item in query_subjects.get("specific_known", []) if item.get('subject')])
            search_query_parts = [user_query_text]
            if specific_subjects_str: search_query_parts.append(f"focus on: {specific_subjects_str}")
            search_query = " ".join(filter(None, search_query_parts))
            if not search_query.strip(): search_query = user_query_text # Fallback

        search_query = search_query.strip()[:1000] # Ensure stripped and truncated
        logger.info(f"Formulated Tavily search query (Task: {primary_task}): {search_query}")
    except Exception as e:
        logger.error(f"Error during search query formulation: {e}", exc_info=True)
        return {"status": "error", "query_used": None, "answer": None, "results": [], "error": "Failed to formulate search query."}


    # --- 3. Check Cache ---
    cache_key = sha256(search_query.encode()).hexdigest()
    logger.debug(f"Using cache key: {cache_key}")
    try:
        response = cache_table.get_item(Key={'search_key': cache_key})
        item = response.get('Item')
        if item and 'ttl' in item and item['ttl'] >= int(time.time()):
            logger.info(f"Cache hit for key: {cache_key}")
            cached_response_data = json.loads(item.get('tavily_response_json', '{}'), parse_float=Decimal) # Parse numbers as Decimal
            return {
                "status": "success_cached", "query_used": search_query,
                "answer": cached_response_data.get("answer"),
                "results": replace_decimals(cached_response_data.get("results", [])), # Convert Decimals back for output
                "error": None
            }
        elif item: logger.info(f"Cache expired/TTL missing for key: {cache_key}")
        else: logger.info(f"Cache miss for key: {cache_key}")
    except ClientError as e: logger.error(f"DynamoDB cache read error: {e.response['Error']['Code']}", exc_info=True)
    except Exception as e: logger.exception("Unexpected cache read error.")


    # --- 4. Call Tavily API ---
    logger.info("Calling Tavily API...")
    tavily_api_key = get_secret_value(SECRET_NAME, "TAVILY_API_KEY")
    if not tavily_api_key:
         return {"status": "error", "query_used": search_query, "answer": None, "results": [], "error": "API key config error (Tavily)."}

    tavily_response_data = {}
    error_message = None
    try:
        client = TavilyClient(api_key=tavily_api_key)
        tavily_response_data = client.search(
            query=search_query,
            search_depth="advanced",
            include_answer="advanced",
            max_results=5
            # Removed time_range="month" for broader results initially
        )
        answer = tavily_response_data.get("answer")
        results_list = tavily_response_data.get("results", [])
        logger.info(f"Received {len(results_list)} results from Tavily.")
        if answer: logger.info("Tavily provided a synthesized answer.")
        logger.debug(f"Tavily Raw Response Snippet: {str(tavily_response_data)[:500]}...")

    except Exception as e:
        logger.error(f"Tavily API call failed: {e}", exc_info=True)
        error_message = f"Tavily API call failed: {str(e)}"
        # Return error structure consistent with success structure
        return {"status": "error", "query_used": search_query, "answer": None, "results": [], "error": error_message}

    # --- 5. Store Result in Cache ---
    if not error_message and tavily_response_data:
        try:
            ttl_timestamp = int(time.time()) + CACHE_TTL_SECONDS
            logger.info(f"Writing Tavily response to cache with TTL: {ttl_timestamp}")
            # Store raw Tavily response (which might have floats)
            cache_table.put_item(
                Item={
                    'search_key': cache_key,
                    'search_query_text': search_query,
                    'tavily_response_json': json.dumps(tavily_response_data), # Store raw response
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
        "answer": tavily_response_data.get("answer"),
        # Convert decimals just before returning
        "results": replace_decimals(tavily_response_data.get("results", [])),
        "error": None
    }