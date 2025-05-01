# # src/interpret_query_v2.py
#
# import logging
# import os
# import json
# import csv
# from pathlib import Path
# from typing import Dict, List, Any, Optional, Set
# import re # Added for timeframe parsing if needed later, though not used in this version's prompt
#
# # AWS SDK for Secrets Manager
# import boto3
# from botocore.exceptions import ClientError # Import specific exception
#
# # LLM SDK
# try:
#     import google.generativeai as genai
#     GEMINI_SDK_AVAILABLE = True
# except ImportError:
#     genai = None
#     GEMINI_SDK_AVAILABLE = False
#     logging.basicConfig(level="ERROR")
#     logging.error("CRITICAL: google-generativeai SDK not found! Install it.")
#
# # --- Configuration ---
# LAMBDA_ROOT = Path(__file__).resolve().parent
# CONFIG_DIR = LAMBDA_ROOT / "config_data"
# CATEGORIES_CSV = CONFIG_DIR / "categories.csv"
# STYLES_CSV = CONFIG_DIR / "styles.csv"       # Expects single column, header 'styles'
# COLORS_CSV = CONFIG_DIR / "colors.csv"       # Expects single column, header 'colors'
#
# SECRET_NAME = os.environ.get("SECRET_NAME", "YourGeminiSecretName")
# LLM_MODEL_NAME = os.environ.get("INTERPRET_LLM_MODEL", "gemini-1.5-flash-latest")
# AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
#
# # --- Initialize Logger ---
# logger = logging.getLogger()
# log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
# valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
# if log_level_str not in valid_log_levels: log_level_str = "INFO"
# logger.setLevel(log_level_str)
# logger.info(f"Logger initialized with level: {log_level_str}")
#
# # --- Globals for Loaded Config Data ---
# KNOWN_CATEGORIES: Set[str] = set()
# KNOWN_STYLES: Set[str] = set() # Single set for all styles
# KNOWN_COLORS: Set[str] = set() # Single set for all colors
# CONFIG_LOAD_ERROR: Optional[str] = None
#
# # --- Revised Function to Load Config Data ---
# def load_config_csvs():
#     """Loads known lists from single-column CSVs. Populates globals."""
#     global KNOWN_CATEGORIES, KNOWN_STYLES, KNOWN_COLORS, CONFIG_LOAD_ERROR
#     logger.info(f"Attempting to load config data from: {CONFIG_DIR}")
#     KNOWN_CATEGORIES.clear(); KNOWN_STYLES.clear(); KNOWN_COLORS.clear(); CONFIG_LOAD_ERROR = None
#     try:
#         # Load Categories
#         if not CATEGORIES_CSV.is_file(): raise FileNotFoundError(f"Categories CSV not found at {CATEGORIES_CSV}")
#         with open(CATEGORIES_CSV, mode='r', encoding='utf-8-sig') as infile:
#             reader = csv.reader(infile); header = next(reader); logger.debug(f"Categories CSV header: {header}"); count = 0
#             for row in reader:
#                 if row and row[0].strip(): KNOWN_CATEGORIES.add(row[0].strip().lower()); count += 1
#             logger.info(f"Loaded {count} categories.")
#             if count == 0: logger.warning(f"'{CATEGORIES_CSV.name}' contained no data rows.")
#         # Load Styles
#         if STYLES_CSV.is_file():
#             with open(STYLES_CSV, mode='r', encoding='utf-8-sig') as infile:
#                 reader = csv.reader(infile); header = next(reader); logger.debug(f"Styles CSV header: {header}"); count = 0
#                 for row in reader:
#                     if row and row[0].strip():
#                         style = row[0].strip().lower()
#                         if style not in KNOWN_STYLES: KNOWN_STYLES.add(style); count += 1
#                 logger.info(f"Loaded {count} unique styles.")
#                 if count == 0: logger.warning(f"'{STYLES_CSV.name}' contained no data rows.")
#         else: logger.warning(f"Styles CSV not found at {STYLES_CSV}, style checking unavailable.")
#         # Load Colors
#         if COLORS_CSV.is_file():
#              with open(COLORS_CSV, mode='r', encoding='utf-8-sig') as infile:
#                 reader = csv.reader(infile); header = next(reader); logger.debug(f"Colors CSV header: {header}"); count = 0
#                 for row in reader:
#                      if row and row[0].strip():
#                          color = row[0].strip().lower()
#                          if color not in KNOWN_COLORS: KNOWN_COLORS.add(color); count += 1
#                 logger.info(f"Loaded {count} unique colors.")
#                 if count == 0: logger.warning(f"'{COLORS_CSV.name}' contained no data rows.")
#         else: logger.warning(f"Colors CSV not found at {COLORS_CSV}, color checking unavailable.")
#     # Consolidate error handling (StopIteration and IndexError imply format issues or emptiness handled by warnings)
#     except FileNotFoundError as e: logger.error(f"Config loading failed: {e}"); CONFIG_LOAD_ERROR = str(e)
#     except Exception as e: logger.exception("CRITICAL ERROR loading config CSVs!"); CONFIG_LOAD_ERROR = f"Unexpected error loading config CSVs: {e}"
#
#     logger.debug(f"Final loaded category count: {len(KNOWN_CATEGORIES)}")
#     logger.debug(f"Final loaded style count: {len(KNOWN_STYLES)}")
#     logger.debug(f"Final loaded color count: {len(KNOWN_COLORS)}")
#
# # --- Load config data ---
# load_config_csvs()
#
# # --- Initialize Boto3 Client ---
# secrets_manager = None
# BOTO3_CLIENT_ERROR = None
# try:
#     session = boto3.session.Session()
#     secrets_manager = session.client(service_name='secretsmanager', region_name=AWS_REGION)
# except Exception as e:
#     logger.exception("CRITICAL ERROR initializing Boto3 client!")
#     BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 client: {e}"
#
# # --- API Key Caching ---
# API_KEY_CACHE: Dict[str, Optional[str]] = {}
#
# # --- Helper Function to Get Secrets ---
# def get_secret_value(secret_name: str, key_name: str) -> Optional[str]:
#     """Retrieves a specific key from a secret in AWS Secrets Manager or ENV."""
#     # Check ENV first if IS_LOCAL=true
#     is_local = os.environ.get("IS_LOCAL", "false").lower() == "true"
#     if is_local:
#         direct_key = os.environ.get(key_name)
#         if direct_key:
#             logger.info(f"Using direct env var '{key_name}' (local mode)")
#             return direct_key
#         else: logger.warning(f"Direct env var '{key_name}' not found. Trying Secrets Manager...") # Fall through
#
#     # Check Cache
#     global API_KEY_CACHE
#     cache_key = f"{secret_name}:{key_name}" # Cache per secret+key
#     if cache_key in API_KEY_CACHE:
#         logger.debug(f"Using cached secret key: {cache_key}")
#         return API_KEY_CACHE[cache_key] # Return cached value (even if None)
#
#     # Check Boto3 client
#     if BOTO3_CLIENT_ERROR: logger.error(f"Boto3 client error: {BOTO3_CLIENT_ERROR}"); return None
#     if not secrets_manager: logger.error("Secrets Manager client not initialized."); return None
#
#     # Fetch from Secrets Manager
#     try:
#         logger.info(f"Fetching secret '{secret_name}' to get key '{key_name}'")
#         response = secrets_manager.get_secret_value(SecretId=secret_name)
#         secret_dict = None
#         if 'SecretString' in response:
#             try: secret_dict = json.loads(response['SecretString'])
#             except json.JSONDecodeError as e: logger.error(f"Failed JSON parse: {e}"); return None
#         elif 'SecretBinary' in response:
#              try: secret_dict = json.loads(response['SecretBinary'].decode('utf-8'))
#              except (json.JSONDecodeError, UnicodeDecodeError) as e: logger.error(f"Failed binary decode: {e}"); return None
#         else: logger.error("Secret value not found."); return None
#
#         if not isinstance(secret_dict, dict): logger.error("Parsed secret is not dict."); return None
#
#         key_value = secret_dict.get(key_name)
#         if not key_value or not isinstance(key_value, str):
#             logger.error(f"Key '{key_name}' not found or not string in secret '{secret_name}'.")
#             API_KEY_CACHE[cache_key] = None # Cache the failure for this key
#             return None
#
#         API_KEY_CACHE[cache_key] = key_value # Cache success
#         logger.info(f"Key '{key_name}' successfully retrieved and cached.")
#         return key_value
#
#     except ClientError as e:
#         error_code = e.response.get("Error", {}).get("Code")
#         logger.error(f"AWS ClientError for '{secret_name}': {error_code}")
#         API_KEY_CACHE[cache_key] = None # Cache the failure for this key
#         return None
#     except Exception as e:
#         logger.exception(f"Unexpected error retrieving secret '{secret_name}'.")
#         API_KEY_CACHE[cache_key] = None # Cache the failure for this key
#         return None
#
#
# # --- Main Lambda Handler ---
# def lambda_handler(event, context):
#     """Parses input, calls LLM for interpretation, returns structured result."""
#     # --- Initial Checks ---
#     if CONFIG_LOAD_ERROR:
#         logger.error(f"Config load failure: {CONFIG_LOAD_ERROR}")
#         return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Config Error: {CONFIG_LOAD_ERROR}"})}
#     if BOTO3_CLIENT_ERROR:
#         logger.error(f"Boto3 init failure: {BOTO3_CLIENT_ERROR}")
#         return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": BOTO3_CLIENT_ERROR})}
#     if not GEMINI_SDK_AVAILABLE:
#         return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "Gemini SDK unavailable."})}
#
#     logger.info(f"Received event: {json.dumps(event)}")
#
#     # --- 1. Parse Input ---
#     try:
#         if isinstance(event.get('body'), str): body = json.loads(event['body']); logger.debug("Parsed body from API GW event.")
#         elif isinstance(event, dict) and 'query' in event and 'category' in event and 'country' in event: body = event; logger.debug("Using direct event payload.")
#         else: raise ValueError("Invalid input structure (missing query, category, or country).")
#
#         user_query = body.get('query')
#         category = body.get('category') # Assume received in Title Case or desired format
#         country = body.get('country')   # Assume received in Title Case or desired format
#
#         if not user_query or not category or not country: raise ValueError("Missing required fields: query, category, or country.")
#
#         logger.info(f"Interpreting Query: '{user_query}' for Cat: '{category}', Country: '{country}'")
#
#     except (json.JSONDecodeError, ValueError, TypeError) as e:
#         logger.error(f"Request parsing error: {e}")
#         return {"statusCode": 400, "body": json.dumps({"status": "error", "error_message": f"Invalid input: {e}"})}
#
#     # --- 2. Get Google API Key ---
#     google_api_key = get_secret_value(SECRET_NAME, "GOOGLE_API_KEY")
#     if not google_api_key:
#          return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "API key config error (Google)."})}
#
#     # --- 3. Prepare for LLM Call ---
#     try:
#          genai.configure(api_key=google_api_key)
#          model = genai.GenerativeModel(LLM_MODEL_NAME)
#     except Exception as configure_err:
#          logger.error(f"Gemini SDK config error: {configure_err}", exc_info=True)
#          return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "LLM SDK config error."})}
#
#     # Use globally loaded sets, apply Title Case for prompt context
#     # Note: Storing KNOWN sets in lowercase, converting to TitleCase for prompt
#     known_styles_list = sorted([s.title() for s in KNOWN_STYLES])
#     known_colors_list = sorted([c.title() for c in KNOWN_COLORS])
#
#     # --- Construct the Prompt (with updated Instruction #3 and Output Structure) ---
#     prompt = f"""Analyze the user query strictly within the given fashion context.
#     Context:
#     - Category: "{category}"
#     - Country: "{country}"
#     - List of All Known Styles (global): {json.dumps(known_styles_list) if known_styles_list else "None Provided"}
#     - List of All Known Colors (global): {json.dumps(known_colors_list) if known_colors_list else "None Provided"}
#
#     User Query: "{user_query}"
#
#     Instructions:
#     1.  Identify the primary analysis task based on the User Query's intent (e.g., forecast, trends, recommendations, comparison). Choose ONE from: ['get_trend', 'get_forecast', 'get_recommendation', 'compare_items', 'summarize_category', 'summarize_mega_trends', 'qa_web_only', 'qa_internal_only', 'qa_combined', 'unknown'].
#     2.  Determine the *minimum necessary* data sources required for the task. Choose one or more from: ['internal_trends_category', 'internal_trends_item', 'internal_forecast', 'internal_mega', 'web_search', 'clarify'].
#         - If specific subjects (items/styles/colors) are identified AND the task requires item-level detail (like 'get_forecast', 'get_recommendation' for an item, 'compare_items'), include 'internal_trends_item' and/or 'internal_forecast'.
#         - If the task is broad (like 'summarize_category'), use 'internal_trends_category'.
#         - Use 'internal_mega' if keywords suggest rising/hot trends.
#         - Include 'web_search' if the query explicitly mentions news, sentiment, competitors, asks 'why', or seems unanswerable by internal data.
#         - If the query is too ambiguous, invalid, lacks specifics needed for the task, or falls outside the Category/Country context, use ONLY 'clarify'.
#     3.  Extract key entities mentioned in the User Query:
#         -   `specific_known_subjects`: Create a list of objects. For each item/style/color from the query that *exactly matches* (case-insensitive) ANY entry in the 'All Known Styles' or 'All Known Colors' lists:
#             a. Determine if the matched subject is appropriate for the stated Category context (e.g., "maxi dress" is likely inappropriate for Category "Shirts").
#             b. **If appropriate:** Determine its type ('color' or 'style').
#             c. **If appropriate:** Add an object to the list with keys "subject" (the matched term, formatted in Title Case) and "type" (either "color" or "style"). Example: `{{"subject": "Blue", "type": "color"}}`.
#             d. **If inappropriate for the category:** Do NOT add it to this list; add the term to `unmapped_items` instead.
#         -   `unmapped_items`: List any items/keywords from the query that look like fashion terms but were NOT found in the known lists OR were found but deemed inappropriate for the stated Category context. Use Title Case for items in this list.
#         -   `timeframe_reference`: Any mention of time (e.g., "next 3 months", "last year", "latest"). Return null if none found.
#         -   `attributes`: List any other descriptive attributes mentioned (e.g., "material:linen", "price:high", "fit:baggy"). Return [] if none found.
#     4.  Determine the overall 'status'. It MUST be 'needs_clarification' if 'clarify' is in required_sources OR if step 3 identified potentially relevant but category-inappropriate subjects. Otherwise, it MUST be 'success'.
#     5.  Provide a concise 'clarification_needed' message (string) ONLY if status is 'needs_clarification', otherwise it MUST be null. Include reasons like ambiguity or category mismatch if applicable.
#
#     Output ONLY a valid JSON object following this exact structure:
#     {{
#       "status": "success | needs_clarification",
#       "primary_task": "string | null",
#       "required_sources": ["string", ...],
#       "query_subjects": {{
#         "specific_known": [ {{ "subject": "string (Title Case)", "type": "color | style" }} ],
#         "unmapped_items": ["string (Title Case)", ...]
#       }},
#       "timeframe_reference": "string | null",
#       "attributes": ["string", ...],
#       "clarification_needed": "string | null"
#     }}
#     """
#     logger.debug("Prompt constructed.")
#
#     # --- 4. Call LLM ---
#     logger.info(f"Calling LLM: {LLM_MODEL_NAME}...")
#     try:
#         generation_config = genai.types.GenerationConfig(response_mime_type="application/json")
#         response = model.generate_content(prompt, generation_config=generation_config)
#         logger.info("LLM response received.")
#         logger.debug(f"LLM Raw Response Text:\n{response.text}")
#     except Exception as llm_err:
#          logger.error(f"LLM API call failed: {llm_err}", exc_info=True)
#          return {"statusCode": 502, "body": json.dumps({"status": "error", "error_message": f"LLM API call failed: {llm_err}"})}
#
#     # --- 5. Parse and Validate LLM Response ---
#     try:
#         cleaned_text = response.text.strip()
#         if cleaned_text.startswith("```json"): cleaned_text = cleaned_text[7:]
#         if cleaned_text.endswith("```"): cleaned_text = cleaned_text[:-3]
#         cleaned_text = cleaned_text.strip()
#         if not cleaned_text: raise ValueError("LLM returned empty response after cleaning markdown.")
#
#         llm_output = json.loads(cleaned_text)
#
#         # --- Validation ---
#         required_keys = ["status", "primary_task", "required_sources", "query_subjects", "timeframe_reference", "attributes", "clarification_needed"]
#         missing_keys = [key for key in required_keys if key not in llm_output]
#         if missing_keys: raise ValueError(f"LLM output missing required keys: {', '.join(missing_keys)}.")
#         if not isinstance(llm_output.get("required_sources"), list): raise ValueError("LLM 'required_sources' not list.")
#
#         query_subjects = llm_output.get("query_subjects")
#         if not isinstance(query_subjects, dict): raise ValueError("LLM 'query_subjects' not dict.")
#         if "specific_known" not in query_subjects or "unmapped_items" not in query_subjects: raise ValueError("LLM 'query_subjects' missing keys.")
#         if not isinstance(query_subjects["unmapped_items"], list): raise ValueError("LLM 'unmapped_items' not list.")
#
#         # Validate specific_known structure
#         specific_known = query_subjects["specific_known"]
#         if not isinstance(specific_known, list): raise ValueError("LLM 'specific_known' not list.")
#         for item in specific_known:
#             if not isinstance(item, dict): raise ValueError(f"Item in 'specific_known' not dict: {item}")
#             if "subject" not in item or "type" not in item: raise ValueError(f"Item missing 'subject'/'type': {item}")
#             if item["type"] not in ["color", "style"]: raise ValueError(f"Invalid type '{item['type']}': {item}")
#             if not isinstance(item.get("subject"), str): raise ValueError(f"Subject not string: {item}")
#
#         # Check status consistency
#         if "clarify" in llm_output.get("required_sources", []) and llm_output.get("status") != "needs_clarification":
#             logger.warning("Forcing status to 'needs_clarification' due to 'clarify' source.")
#             llm_output["status"] = "needs_clarification"
#         if llm_output.get("status") == "needs_clarification" and not llm_output.get("clarification_needed"):
#              logger.warning("Adding generic clarification message.")
#              llm_output["clarification_needed"] = "Query requires clarification. Please be more specific or ensure terms are relevant to the category."
#
#         logger.info(f"LLM interpretation successful. Task: {llm_output.get('primary_task')}, Status: {llm_output.get('status')}")
#
#         # Add original context back (using originally passed case, assumed Title)
#         llm_output['original_context'] = {'category': category, 'country': country, 'query': user_query}
#
#         # --- 6. Return Success Response ---
#         return { "statusCode": 200, "body": json.dumps(llm_output) }
#
#     except (json.JSONDecodeError, ValueError, TypeError) as e:
#          logger.error(f"Failed parsing/validating LLM response: {e}", exc_info=True)
#          logger.error(f"LLM Raw Text was: {response.text}") # Log raw on error
#          return { "statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Failed processing LLM response: {e}", "llm_raw_output": response.text}) }
#
#     except Exception as e:
#         logger.exception("Unhandled error during interpretation.")
#         return { "statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Internal server error: {str(e)}"}) }

# import logging
# import os
# import json
# import csv
# from pathlib import Path
# from typing import Dict, List, Any, Optional, Set
# import re
# import boto3
# from botocore.exceptions import ClientError
#
# try:
#     import google.generativeai as genai
#     GEMINI_SDK_AVAILABLE = True
# except ImportError:
#     genai = None
#     GEMINI_SDK_AVAILABLE = False
#     logging.basicConfig(level="ERROR")
#     logging.error("CRITICAL: google-generativeai SDK not found! Install it.")
#
# LAMBDA_ROOT = Path(__file__).resolve().parent
# CONFIG_DIR = LAMBDA_ROOT / "config_data"
# CATEGORIES_CSV = CONFIG_DIR / "categories.csv"
# STYLES_CSV = CONFIG_DIR / "styles.csv"
# COLORS_CSV = CONFIG_DIR / "colors.csv"
#
# SECRET_NAME = os.environ.get("SECRET_NAME", "YourGeminiSecretName")
# LLM_MODEL_NAME = os.environ.get("INTERPRET_LLM_MODEL", "gemini-1.5-flash-latest")
# AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
#
# logger = logging.getLogger()
# log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
# valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
# if log_level_str not in valid_log_levels: log_level_str = "INFO"
# logger.setLevel(log_level_str)
# logger.info(f"Logger initialized with level: {log_level_str}")
#
# KNOWN_CATEGORIES: Set[str] = set()
# KNOWN_STYLES: Set[str] = set()
# KNOWN_COLORS: Set[str] = set()
# CONFIG_LOAD_ERROR: Optional[str] = None
#
# def load_config_csvs():
#     global KNOWN_CATEGORIES, KNOWN_STYLES, KNOWN_COLORS, CONFIG_LOAD_ERROR
#     logger.info(f"Attempting to load config data from: {CONFIG_DIR}")
#     KNOWN_CATEGORIES.clear(); KNOWN_STYLES.clear(); KNOWN_COLORS.clear(); CONFIG_LOAD_ERROR = None
#     try:
#         if not CATEGORIES_CSV.is_file(): raise FileNotFoundError(f"Categories CSV not found at {CATEGORIES_CSV}")
#         with open(CATEGORIES_CSV, mode='r', encoding='utf-8-sig') as infile:
#             reader = csv.reader(infile); header = next(reader); logger.debug(f"Categories CSV header: {header}"); count = 0
#             for row in reader:
#                 if row and row[0].strip(): KNOWN_CATEGORIES.add(row[0].strip().lower()); count += 1
#             logger.info(f"Loaded {count} categories.")
#             if count == 0: logger.warning(f"'{CATEGORIES_CSV.name}' contained no data rows.")
#         if STYLES_CSV.is_file():
#             with open(STYLES_CSV, mode='r', encoding='utf-8-sig') as infile:
#                 reader = csv.reader(infile); header = next(reader); logger.debug(f"Styles CSV header: {header}"); count = 0
#                 for row in reader:
#                     if row and row[0].strip():
#                         style = row[0].strip().lower()
#                         if style not in KNOWN_STYLES: KNOWN_STYLES.add(style); count += 1
#                 logger.info(f"Loaded {count} unique styles.")
#                 if count == 0: logger.warning(f"'{STYLES_CSV.name}' contained no data rows.")
#         else: logger.warning(f"Styles CSV not found at {STYLES_CSV}, style checking unavailable.")
#         if COLORS_CSV.is_file():
#              with open(COLORS_CSV, mode='r', encoding='utf-8-sig') as infile:
#                 reader = csv.reader(infile); header = next(reader); logger.debug(f"Colors CSV header: {header}"); count = 0
#                 for row in reader:
#                      if row and row[0].strip():
#                          color = row[0].strip().lower()
#                          if color not in KNOWN_COLORS: KNOWN_COLORS.add(color); count += 1
#                 logger.info(f"Loaded {count} unique colors.")
#                 if count == 0: logger.warning(f"'{COLORS_CSV.name}' contained no data rows.")
#         else: logger.warning(f"Colors CSV not found at {COLORS_CSV}, color checking unavailable.")
#     except FileNotFoundError as e: logger.error(f"Config loading failed: {e}"); CONFIG_LOAD_ERROR = str(e)
#     except Exception as e: logger.exception("CRITICAL ERROR loading config CSVs!"); CONFIG_LOAD_ERROR = f"Unexpected error loading config CSVs: {e}"
#     logger.debug(f"Final loaded category count: {len(KNOWN_CATEGORIES)}")
#     logger.debug(f"Final loaded style count: {len(KNOWN_STYLES)}")
#     logger.debug(f"Final loaded color count: {len(KNOWN_COLORS)}")
#
# load_config_csvs()
#
# secrets_manager = None
# BOTO3_CLIENT_ERROR = None
# try:
#     session = boto3.session.Session()
#     secrets_manager = session.client(service_name='secretsmanager', region_name=AWS_REGION)
# except Exception as e:
#     logger.exception("CRITICAL ERROR initializing Boto3 client!")
#     BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 client: {e}"
#
# API_KEY_CACHE: Dict[str, Optional[str]] = {}
#
# def get_secret_value(secret_name: str, key_name: str) -> Optional[str]:
#     is_local = os.environ.get("IS_LOCAL", "false").lower() == "true"
#     if is_local:
#         direct_key = os.environ.get(key_name)
#         if direct_key:
#             logger.info(f"Using direct env var '{key_name}' (local mode)")
#             return direct_key
#         else: logger.warning(f"Direct env var '{key_name}' not found. Trying Secrets Manager...")
#
#     global API_KEY_CACHE
#     cache_key = f"{secret_name}:{key_name}"
#     if cache_key in API_KEY_CACHE:
#         logger.debug(f"Using cached secret key: {cache_key}")
#         return API_KEY_CACHE[cache_key]
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
#         elif 'SecretBinary' in response:
#              try: secret_dict = json.loads(response['SecretBinary'].decode('utf-8'))
#              except (json.JSONDecodeError, UnicodeDecodeError) as e: logger.error(f"Failed binary decode: {e}"); return None
#         else: logger.error("Secret value not found."); return None
#
#         if not isinstance(secret_dict, dict): logger.error("Parsed secret is not dict."); return None
#         key_value = secret_dict.get(key_name)
#         if not key_value or not isinstance(key_value, str):
#             logger.error(f"Key '{key_name}' not found or not string in secret '{secret_name}'.")
#             API_KEY_CACHE[cache_key] = None; return None
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
# def lambda_handler(event, context):
#     if CONFIG_LOAD_ERROR:
#         logger.error(f"Config load failure: {CONFIG_LOAD_ERROR}")
#         return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Config Error: {CONFIG_LOAD_ERROR}"})}
#     if BOTO3_CLIENT_ERROR:
#         logger.error(f"Boto3 init failure: {BOTO3_CLIENT_ERROR}")
#         return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": BOTO3_CLIENT_ERROR})}
#     if not GEMINI_SDK_AVAILABLE:
#         return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "Gemini SDK unavailable."})}
#
#     logger.info(f"Received event: {json.dumps(event)}")
#
#     try:
#         if isinstance(event.get('body'), str): body = json.loads(event['body']); logger.debug("Parsed body from API GW event.")
#         elif isinstance(event, dict) and 'query' in event and 'category' in event and 'country' in event: body = event; logger.debug("Using direct event payload.")
#         else: raise ValueError("Invalid input structure (missing query, category, or country).")
#         user_query = body.get('query')
#         category = body.get('category')
#         country = body.get('country')
#         if not user_query or not category or not country: raise ValueError("Missing required fields: query, category, or country.")
#         logger.info(f"Interpreting Query: '{user_query}' for Cat: '{category}', Country: '{country}'")
#     except (json.JSONDecodeError, ValueError, TypeError) as e:
#         logger.error(f"Request parsing error: {e}")
#         return {"statusCode": 400, "body": json.dumps({"status": "error", "error_message": f"Invalid input: {e}"})}
#
#     google_api_key = get_secret_value(SECRET_NAME, "GOOGLE_API_KEY")
#     if not google_api_key:
#          return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "API key config error (Google)."})}
#
#     try:
#          genai.configure(api_key=google_api_key)
#          model = genai.GenerativeModel(LLM_MODEL_NAME)
#     except Exception as configure_err:
#          logger.error(f"Gemini SDK config error: {configure_err}", exc_info=True)
#          return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "LLM SDK config error."})}
#
#     known_styles_list = sorted([s.title() for s in KNOWN_STYLES])
#     known_colors_list = sorted([c.title() for c in KNOWN_COLORS])
#
#     prompt = f"""Analyze the user query strictly within the given fashion context.
#     Context:
#     - Category: "{category}"
#     - Country: "{country}"
#     - List of All Known Styles (global): {json.dumps(known_styles_list) if known_styles_list else "None Provided"}
#     - List of All Known Colors (global): {json.dumps(known_colors_list) if known_colors_list else "None Provided"}
#
#     User Query: "{user_query}"
#
#     Instructions:
#     1.  Identify the primary analysis task based on the User Query's intent (e.g., forecast, trends, recommendations, comparison). Choose ONE from: ['get_trend', 'get_forecast', 'get_recommendation', 'compare_items', 'summarize_category', 'summarize_mega_trends', 'qa_web_only', 'qa_internal_only', 'qa_combined', 'unknown'].
#     2.  Determine the *minimum necessary* data sources required for the task. Choose one or more from: ['internal_trends_category', 'internal_trends_item', 'internal_forecast', 'internal_mega', 'web_search', 'clarify']. You MUST include 'web_search' if the query asks 'why', mentions news, sentiment, competitors, or clearly requires external context for reasoning. If specific subjects (items/styles/colors) are identified AND the task requires item-level detail (like 'get_forecast', 'get_recommendation' for an item, 'compare_items'), include 'internal_trends_item' and/or 'internal_forecast'. If the task is broad (like 'summarize_category'), use 'internal_trends_category'. Use 'internal_mega' if keywords suggest rising/hot trends. If the query is too ambiguous, invalid, lacks specifics needed for the task, or falls outside the Category/Country context, use ONLY 'clarify'.
#     3.  Extract key entities mentioned in the User Query:
#         -   `specific_known_subjects`: Create a list of objects. For each item/style/color from the query that *exactly matches* (case-insensitive) ANY entry in the 'All Known Styles' or 'All Known Colors' lists: Determine if the matched subject is appropriate for the stated Category context (e.g., "maxi dress" is likely inappropriate for Category "Shirts"). If appropriate: Determine its type ('color' or 'style'). If appropriate: Add an object to the list with keys "subject" (the matched term, formatted in Title Case) and "type" (either "color" or "style"). Example: `{{"subject": "Blue", "type": "color"}}`. If inappropriate for the category: Do NOT add it to this list; add the term to `unmapped_items` instead.
#         -   `unmapped_items`: List any items/keywords from the query that look like fashion terms but were NOT found in the known lists OR were found but deemed inappropriate for the stated Category context. Use Title Case for items in this list.
#         -   `timeframe_reference`: Any mention of time (e.g., "next 3 months", "last year", "latest"). Return null if none found.
#         -   `attributes`: List any other descriptive attributes mentioned (e.g., "material:linen", "price:high", "fit:baggy"). Return [] if none found.
#     4.  Determine the overall 'status'. It MUST be 'needs_clarification' if 'clarify' is in required_sources OR if step 3 identified potentially relevant but category-inappropriate subjects. Otherwise (even if 'web_search' is required), it MUST be 'success'.
#     5.  Provide a concise 'clarification_needed' message (string) ONLY if status is 'needs_clarification', otherwise it MUST be null. Include reasons like ambiguity or category mismatch if applicable.
#
#     Output ONLY a valid JSON object following this exact structure:
#     {{
#       "status": "success | needs_clarification",
#       "primary_task": "string | null",
#       "required_sources": ["string", ...],
#       "query_subjects": {{
#         "specific_known": [ {{ "subject": "string (Title Case)", "type": "color | style" }} ],
#         "unmapped_items": ["string (Title Case)", ...]
#       }},
#       "timeframe_reference": "string | null",
#       "attributes": ["string", ...],
#       "clarification_needed": "string | null"
#     }}
#     """
#     logger.debug("Prompt constructed.")
#
#     logger.info(f"Calling LLM: {LLM_MODEL_NAME}...")
#     try:
#         generation_config = genai.types.GenerationConfig(response_mime_type="application/json")
#         response = model.generate_content(prompt, generation_config=generation_config)
#         logger.info("LLM response received.")
#         logger.debug(f"LLM Raw Response Text:\n{response.text}")
#     except Exception as llm_err:
#          logger.error(f"LLM API call failed: {llm_err}", exc_info=True)
#          return {"statusCode": 502, "body": json.dumps({"status": "error", "error_message": f"LLM API call failed: {llm_err}"})}
#
#     try:
#         cleaned_text = response.text.strip()
#         if cleaned_text.startswith("```json"): cleaned_text = cleaned_text[7:]
#         if cleaned_text.endswith("```"): cleaned_text = cleaned_text[:-3]
#         cleaned_text = cleaned_text.strip()
#         if not cleaned_text: raise ValueError("LLM returned empty response after cleaning markdown.")
#         llm_output = json.loads(cleaned_text)
#
#         required_keys = ["status", "primary_task", "required_sources", "query_subjects", "timeframe_reference", "attributes", "clarification_needed"]
#         missing_keys = [key for key in required_keys if key not in llm_output]
#         if missing_keys: raise ValueError(f"LLM output missing required keys: {', '.join(missing_keys)}.")
#         if not isinstance(llm_output.get("required_sources"), list): raise ValueError("LLM 'required_sources' not list.")
#         query_subjects = llm_output.get("query_subjects")
#         if not isinstance(query_subjects, dict): raise ValueError("LLM 'query_subjects' not dict.")
#         if "specific_known" not in query_subjects or "unmapped_items" not in query_subjects: raise ValueError("LLM 'query_subjects' missing keys.")
#         if not isinstance(query_subjects["unmapped_items"], list): raise ValueError("LLM 'unmapped_items' not list.")
#         specific_known = query_subjects["specific_known"]
#         if not isinstance(specific_known, list): raise ValueError("LLM 'specific_known' not list.")
#         for item in specific_known:
#             if not isinstance(item, dict): raise ValueError(f"Item in 'specific_known' not dict: {item}")
#             if "subject" not in item or "type" not in item: raise ValueError(f"Item missing 'subject'/'type': {item}")
#             if item["type"] not in ["color", "style"]: raise ValueError(f"Invalid type '{item['type']}': {item}")
#             if not isinstance(item.get("subject"), str): raise ValueError(f"Subject not string: {item}")
#
#         if "clarify" in llm_output.get("required_sources", []) and llm_output.get("status") != "needs_clarification":
#             logger.warning("Forcing status to 'needs_clarification' due to 'clarify' source.")
#             llm_output["status"] = "needs_clarification"
#         if llm_output.get("status") == "needs_clarification" and not llm_output.get("clarification_needed"):
#              logger.warning("Adding generic clarification message.")
#              llm_output["clarification_needed"] = "Query requires clarification. Please be more specific or ensure terms are relevant to the category."
#
#         logger.info(f"LLM interpretation successful. Task: {llm_output.get('primary_task')}, Status: {llm_output.get('status')}")
#         llm_output['original_context'] = {'category': category, 'country': country, 'query': user_query}
#         return { "statusCode": 200, "body": json.dumps(llm_output) }
#
#     except (json.JSONDecodeError, ValueError, TypeError) as e:
#          logger.error(f"Failed parsing/validating LLM response: {e}", exc_info=True)
#          logger.error(f"LLM Raw Text was: {response.text}")
#          return { "statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Failed processing LLM response: {e}", "llm_raw_output": response.text}) }

    # except Exception as e:
    #     logger.exception("Unhandled error during interpretation.")
    #     return { "statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Internal server error: {str(e)}"}) }

# import logging
# import os
# import json
# import csv
# from pathlib import Path
# from typing import Dict, List, Any, Optional, Set
# import re
# import boto3
# from botocore.exceptions import ClientError
#
# try:
#     import google.generativeai as genai
#     GEMINI_SDK_AVAILABLE = True
# except ImportError:
#     genai = None
#     GEMINI_SDK_AVAILABLE = False
#     logging.basicConfig(level="ERROR")
#     logging.error("CRITICAL: google-generativeai SDK not found! Install it.")
#
# LAMBDA_ROOT = Path(__file__).resolve().parent
# CONFIG_DIR = LAMBDA_ROOT / "config_data"
# CATEGORIES_CSV = CONFIG_DIR / "categories.csv"
# STYLES_CSV = CONFIG_DIR / "styles.csv"
# COLORS_CSV = CONFIG_DIR / "colors.csv"
#
# SECRET_NAME = os.environ.get("SECRET_NAME", "YourGeminiSecretName")
# LLM_MODEL_NAME = os.environ.get("INTERPRET_LLM_MODEL", "gemini-2.0-flash")
# AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
#
# logger = logging.getLogger()
# log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
# valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
# if log_level_str not in valid_log_levels: log_level_str = "INFO"
# logger.setLevel(log_level_str)
# logger.info(f"Logger initialized with level: {log_level_str}")
# logger.info(f"Using Interpreter LLM: {LLM_MODEL_NAME}")
#
# KNOWN_CATEGORIES: Set[str] = set()
# KNOWN_STYLES: Set[str] = set()
# KNOWN_COLORS: Set[str] = set()
# CONFIG_LOAD_ERROR: Optional[str] = None
#
# def load_config_csvs():
#     global KNOWN_CATEGORIES, KNOWN_STYLES, KNOWN_COLORS, CONFIG_LOAD_ERROR
#     logger.info(f"Attempting to load config data from: {CONFIG_DIR}")
#     KNOWN_CATEGORIES.clear(); KNOWN_STYLES.clear(); KNOWN_COLORS.clear(); CONFIG_LOAD_ERROR = None
#     try:
#         if not CATEGORIES_CSV.is_file(): raise FileNotFoundError(f"Categories CSV not found at {CATEGORIES_CSV}")
#         with open(CATEGORIES_CSV, mode='r', encoding='utf-8-sig') as infile:
#             reader = csv.reader(infile); header = next(reader); logger.debug(f"Categories CSV header: {header}"); count = 0
#             for row in reader:
#                 if row and row[0].strip(): KNOWN_CATEGORIES.add(row[0].strip().lower()); count += 1
#             logger.info(f"Loaded {count} categories.")
#             if count == 0: logger.warning(f"'{CATEGORIES_CSV.name}' contained no data rows.")
#         if STYLES_CSV.is_file():
#             with open(STYLES_CSV, mode='r', encoding='utf-8-sig') as infile:
#                 reader = csv.reader(infile); header = next(reader); logger.debug(f"Styles CSV header: {header}"); count = 0
#                 for row in reader:
#                     if row and row[0].strip():
#                         style = row[0].strip().lower()
#                         if style not in KNOWN_STYLES: KNOWN_STYLES.add(style); count += 1
#                 logger.info(f"Loaded {count} unique styles.")
#                 if count == 0: logger.warning(f"'{STYLES_CSV.name}' contained no data rows.")
#         else: logger.warning(f"Styles CSV not found at {STYLES_CSV}, style checking unavailable.")
#         if COLORS_CSV.is_file():
#              with open(COLORS_CSV, mode='r', encoding='utf-8-sig') as infile:
#                 reader = csv.reader(infile); header = next(reader); logger.debug(f"Colors CSV header: {header}"); count = 0
#                 for row in reader:
#                      if row and row[0].strip():
#                          color = row[0].strip().lower()
#                          if color not in KNOWN_COLORS: KNOWN_COLORS.add(color); count += 1
#                 logger.info(f"Loaded {count} unique colors.")
#                 if count == 0: logger.warning(f"'{COLORS_CSV.name}' contained no data rows.")
#         else: logger.warning(f"Colors CSV not found at {COLORS_CSV}, color checking unavailable.")
#     except FileNotFoundError as e: logger.error(f"Config loading failed: {e}"); CONFIG_LOAD_ERROR = str(e)
#     except Exception as e: logger.exception("CRITICAL ERROR loading config CSVs!"); CONFIG_LOAD_ERROR = f"Unexpected error loading config CSVs: {e}"
#     logger.debug(f"Final loaded category count: {len(KNOWN_CATEGORIES)}")
#     logger.debug(f"Final loaded style count: {len(KNOWN_STYLES)}")
#     logger.debug(f"Final loaded color count: {len(KNOWN_COLORS)}")
#
# load_config_csvs()
#
# secrets_manager = None
# BOTO3_CLIENT_ERROR = None
# try:
#     session = boto3.session.Session()
#     secrets_manager = session.client(service_name='secretsmanager', region_name=AWS_REGION)
# except Exception as e:
#     logger.exception("CRITICAL ERROR initializing Boto3 client!")
#     BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 client: {e}"
#
# API_KEY_CACHE: Dict[str, Optional[str]] = {}
#
# def get_secret_value(secret_name: str, key_name: str) -> Optional[str]:
#     is_local = os.environ.get("IS_LOCAL", "false").lower() == "true"
#     if is_local:
#         direct_key = os.environ.get(key_name)
#         if direct_key:
#             logger.info(f"Using direct env var '{key_name}' (local mode)")
#             return direct_key
#         else: logger.warning(f"Direct env var '{key_name}' not found. Trying Secrets Manager...")
#
#     global API_KEY_CACHE
#     cache_key = f"{secret_name}:{key_name}"
#     if cache_key in API_KEY_CACHE:
#         logger.debug(f"Using cached secret key: {cache_key}")
#         return API_KEY_CACHE[cache_key]
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
#         elif 'SecretBinary' in response:
#              try: secret_dict = json.loads(response['SecretBinary'].decode('utf-8'))
#              except (json.JSONDecodeError, UnicodeDecodeError) as e: logger.error(f"Failed binary decode: {e}"); return None
#         else: logger.error("Secret value not found."); return None
#
#         if not isinstance(secret_dict, dict): logger.error("Parsed secret is not dict."); return None
#         key_value = secret_dict.get(key_name)
#         if not key_value or not isinstance(key_value, str):
#             logger.error(f"Key '{key_name}' not found or not string in secret '{secret_name}'.")
#             API_KEY_CACHE[cache_key] = None; return None
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
# def lambda_handler(event, context):
#     if CONFIG_LOAD_ERROR:
#         logger.error(f"Config load failure: {CONFIG_LOAD_ERROR}")
#         return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Config Error: {CONFIG_LOAD_ERROR}"})}
#     if BOTO3_CLIENT_ERROR:
#         logger.error(f"Boto3 init failure: {BOTO3_CLIENT_ERROR}")
#         return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": BOTO3_CLIENT_ERROR})}
#     if not GEMINI_SDK_AVAILABLE:
#         return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "Gemini SDK unavailable."})}
#
#     logger.info(f"Received event: {json.dumps(event)}")
#
#     try:
#         if isinstance(event.get('body'), str): body = json.loads(event['body']); logger.debug("Parsed body from API GW event.")
#         elif isinstance(event, dict) and 'query' in event and 'category' in event and 'country' in event: body = event; logger.debug("Using direct event payload.")
#         else: raise ValueError("Invalid input structure (missing query, category, or country).")
#         user_query = body.get('query')
#         category = body.get('category')
#         country = body.get('country')
#         if not user_query or not category or not country: raise ValueError("Missing required fields: query, category, or country.")
#         logger.info(f"Interpreting Query: '{user_query}' for Cat: '{category}', Country: '{country}'")
#     except (json.JSONDecodeError, ValueError, TypeError) as e:
#         logger.error(f"Request parsing error: {e}")
#         return {"statusCode": 400, "body": json.dumps({"status": "error", "error_message": f"Invalid input: {e}"})}
#
#     google_api_key = get_secret_value(SECRET_NAME, "GOOGLE_API_KEY")
#     if not google_api_key:
#          return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "API key config error (Google)."})}
#
#     try:
#          genai.configure(api_key=google_api_key)
#          model = genai.GenerativeModel(LLM_MODEL_NAME)
#     except Exception as configure_err:
#          actual_error = str(configure_err)
#          logger.error(f"Gemini SDK config error: {actual_error}", exc_info=True)
#          if "model not found" in actual_error.lower() or "invalid api key" in actual_error.lower():
#               return {"statusCode": 400, "body": json.dumps({"status": "error", "error_message": f"LLM config error: {actual_error}"})}
#          else:
#               return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "LLM SDK configuration error."})}
#
#     known_styles_list = sorted([s.title() for s in KNOWN_STYLES])
#     known_colors_list = sorted([c.title() for c in KNOWN_COLORS])
#
#     prompt = f"""Analyze the user query strictly within the given fashion context.
#     Context:
#     - Category: "{category}"
#     - Country: "{country}"
#     - List of All Known Styles (global): {json.dumps(known_styles_list) if known_styles_list else "None Provided"}
#     - List of All Known Colors (global): {json.dumps(known_colors_list) if known_colors_list else "None Provided"}
#
#     User Query: "{user_query}"
#
#     Instructions:
#     1.  Identify the primary analysis task based on the User Query's intent. Choose ONE from: ['get_trend', 'get_forecast', 'get_recommendation', 'compare_items', 'summarize_category', 'summarize_mega_trends', 'qa_web_only', 'qa_internal_only', 'qa_combined', 'unknown'].
#     2.  Determine the necessary data sources required for the identified primary_task. Choose one or more from: ['internal_trends_category', 'internal_trends_item', 'internal_forecast', 'internal_mega', 'web_search', 'clarify']. Follow these rules STRICTLY:
#         -   Web Search Rule: You MUST include 'web_search' if the query explicitly asks 'why', mentions 'news', 'sentiment', 'competitors', 'web', 'web search', 'hot' trends, 'this week', 'global' trends, or clearly requires external context/reasoning.
#         -   Item Detail Rule: If step 3 identifies ANY subjects in `specific_known_subjects` AND the task requires item-level detail (like 'get_forecast', 'get_recommendation' for an item, 'compare_items', 'get_trend' for a specific item), you MUST include 'internal_trends_item' and/or 'internal_forecast'.
#         -   Category Context Rule: If the task is broad ('summarize_category', 'get_trend' for the whole category without specific items), use 'internal_trends_category'. ALSO, if the task is 'get_trend' or 'qa_combined' or 'qa_internal_only' or 'qa_web_only' and step 3 identifies items in `unmapped_items` but NOT in `specific_known_subjects`, you MUST include 'internal_trends_category' to provide context.
#         -   Mega Trends Rule: Use 'internal_mega' ONLY if the primary_task is 'summarize_mega_trends'. You MUST NOT include 'internal_mega' if any other internal source ('internal_trends_category', 'internal_trends_item', 'internal_forecast') is selected OR if step 3 identifies ANY subjects in `specific_known_subjects` OR `unmapped_items`.
#         -   Clarification Rule: If the query is too ambiguous, invalid, lacks specifics needed for the task (e.g., forecast without an item), or falls outside the Category/Country context, use ONLY 'clarify'.
#     3.  Extract key entities mentioned in the User Query. Apply these rules STRICTLY:
#         -   First, identify all potential fashion subjects (styles, colors, items like 'bomber jacket') in the query.
#         -   For EACH potential subject:
#             a. Check for an exact case-insensitive match in the 'All Known Styles' or 'All Known Colors' lists.
#             b. If a match IS found: Check if the matched term is appropriate for the stated Category context (e.g., 'Dresses' style is inappropriate for 'Shirts' Category). If it IS appropriate, determine if it's a 'style' or 'color' and add `{{"subject": "Matched Term Title Case", "type": "style|color"}}` to the `specific_known_subjects` list. If it is NOT appropriate for the category, add the term (Title Case) to `unmapped_items`.
#             c. If NO exact match is found in the known lists: Add the term (Title Case) to the `unmapped_items` list.
#             d. DO NOT guess or find the 'closest' match. Only exact matches are processed for `specific_known_subjects`.
#         -   `specific_known_subjects`: List of objects for matched, category-appropriate subjects. Can be empty.
#         -   `unmapped_items`: List of terms that were not exact matches, were category-inappropriate, or other potential fashion items. Can be empty. Use Title Case.
#         -   `timeframe_reference`: Any mention of time (e.g., "next 6 months", "latest"). Return null if none found.
#         -   `attributes`: Any other descriptors (e.g., "material:linen", "price:high"). Return [] if none found.
#     4.  Determine the overall 'status'. It MUST be 'needs_clarification' ONLY if 'clarify' is in `required_sources` OR if step 3 found category-inappropriate items that prevent analysis. Otherwise (even if 'web_search' is required), it MUST be 'success'.
#     5.  Provide a concise 'clarification_needed' message (string) ONLY if status is 'needs_clarification', otherwise it MUST be null.
#
#     Output ONLY a valid JSON object following this exact structure:
#     {{
#       "status": "success | needs_clarification",
#       "primary_task": "string | null",
#       "required_sources": ["string", ...],
#       "query_subjects": {{
#         "specific_known": [ {{ "subject": "string (Title Case)", "type": "color | style" }} ],
#         "unmapped_items": ["string (Title Case)", ...]
#       }},
#       "timeframe_reference": "string | null",
#       "attributes": ["string", ...],
#       "clarification_needed": "string | null"
#     }}
#     """
#     logger.debug("Prompt constructed.")
#
#     logger.info(f"Calling LLM: {LLM_MODEL_NAME}...")
#     try:
#         generation_config = genai.types.GenerationConfig(response_mime_type="application/json")
#         response = model.generate_content(prompt, generation_config=generation_config)
#         logger.info("LLM response received.")
#         logger.debug(f"LLM Raw Response Text:\n{response.text}")
#     except Exception as llm_err:
#          logger.error(f"LLM API call failed: {llm_err}", exc_info=True)
#          return {"statusCode": 502, "body": json.dumps({"status": "error", "error_message": f"LLM API call failed: {str(llm_err)}"})}
#
#     try:
#         cleaned_text = response.text.strip()
#         if cleaned_text.startswith("```json"): cleaned_text = cleaned_text[7:]
#         if cleaned_text.endswith("```"): cleaned_text = cleaned_text[:-3]
#         cleaned_text = cleaned_text.strip()
#         if not cleaned_text: raise ValueError("LLM returned empty response after cleaning markdown.")
#         llm_output = json.loads(cleaned_text)
#
#         required_keys = ["status", "primary_task", "required_sources", "query_subjects", "timeframe_reference", "attributes", "clarification_needed"]
#         missing_keys = [key for key in required_keys if key not in llm_output]
#         if missing_keys: raise ValueError(f"LLM output missing required keys: {', '.join(missing_keys)}.")
#         if not isinstance(llm_output.get("required_sources"), list): raise ValueError("LLM 'required_sources' not list.")
#         query_subjects = llm_output.get("query_subjects")
#         if not isinstance(query_subjects, dict): raise ValueError("LLM 'query_subjects' not dict.")
#         if "specific_known" not in query_subjects or "unmapped_items" not in query_subjects: raise ValueError("LLM 'query_subjects' missing keys.")
#         if not isinstance(query_subjects["unmapped_items"], list): raise ValueError("LLM 'unmapped_items' not list.")
#         specific_known = query_subjects["specific_known"]
#         if not isinstance(specific_known, list): raise ValueError("LLM 'specific_known' not list.")
#         for item in specific_known:
#             if not isinstance(item, dict): raise ValueError(f"Item in 'specific_known' not dict: {item}")
#             if "subject" not in item or "type" not in item: raise ValueError(f"Item missing 'subject'/'type': {item}")
#             if item["type"] not in ["color", "style"]: raise ValueError(f"Invalid type '{item['type']}': {item}")
#             if not isinstance(item.get("subject"), str): raise ValueError(f"Subject not string: {item}")
#
#         primary_task_llm = llm_output.get("primary_task")
#         required_sources_llm = llm_output.get("required_sources", [])
#         unmapped_items_llm = query_subjects.get("unmapped_items", [])
#
#         # Post-Processing Rule: Add category context for trend/QA if only unmapped items exist
#         if primary_task_llm in ["get_trend", "qa_combined", "qa_internal_only", "qa_web_only"] and \
#            not specific_known and unmapped_items_llm and \
#            "internal_trends_category" not in required_sources_llm:
#             logger.warning("Post-LLM: Adding 'internal_trends_category' based on task type and unmapped items.")
#             required_sources_llm.append("internal_trends_category")
#
#         # Post-Processing Rule 7: If specific OR unmapped items exist, remove internal_mega
#         if specific_known or unmapped_items_llm:
#             if "internal_mega" in required_sources_llm:
#                 logger.warning("Post-LLM: Removing 'internal_mega' source because specific/unmapped subjects were found.")
#                 required_sources_llm = [s for s in required_sources_llm if s != "internal_mega"]
#                 if not required_sources_llm:
#                      if specific_known or unmapped_items_llm:
#                           required_sources_llm.append("internal_trends_category")
#                           logger.warning("Post-LLM: Added 'internal_trends_category' as fallback after removing 'internal_mega'.")
#
#         # Ensure uniqueness and sort for consistency
#         llm_output["required_sources"] = sorted(list(set(required_sources_llm)))
#
#
#         if "clarify" in llm_output.get("required_sources", []) and llm_output.get("status") != "needs_clarification":
#             logger.warning("Forcing status to 'needs_clarification' due to 'clarify' source.")
#             llm_output["status"] = "needs_clarification"
#         if llm_output.get("status") == "needs_clarification" and not llm_output.get("clarification_needed"):
#              logger.warning("Adding generic clarification message.")
#              llm_output["clarification_needed"] = "Query requires clarification. Please be more specific or ensure terms are relevant to the category."
#
#         logger.info(f"LLM interpretation successful (post-processed). Task: {llm_output.get('primary_task')}, Status: {llm_output.get('status')}, Sources: {llm_output.get('required_sources')}")
#         llm_output['original_context'] = {'category': category, 'country': country, 'query': user_query}
#         return { "statusCode": 200, "body": json.dumps(llm_output) }
#
#     except (json.JSONDecodeError, ValueError, TypeError) as e:
#          logger.error(f"Failed parsing/validating LLM response: {e}", exc_info=True)
#          logger.error(f"LLM Raw Text was: {response.text}")
#          return { "statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Failed processing LLM response: {e}", "llm_raw_output": response.text}) }
#
#     except Exception as e:
#         logger.exception("Unhandled error during interpretation.")
#         return { "statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Internal server error: {str(e)}"}) }



import logging
import os
import json
import csv
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
import re
import boto3
from botocore.exceptions import ClientError

try:
    import google.generativeai as genai
    GEMINI_SDK_AVAILABLE = True
except ImportError:
    genai = None
    GEMINI_SDK_AVAILABLE = False
    logging.basicConfig(level="ERROR")
    logging.error("CRITICAL: google-generativeai SDK not found! Install it.")

LAMBDA_ROOT = Path(__file__).resolve().parent
CONFIG_DIR = LAMBDA_ROOT / "config_data"
CATEGORIES_CSV = CONFIG_DIR / "categories.csv"
STYLES_CSV = CONFIG_DIR / "styles.csv"
COLORS_CSV = CONFIG_DIR / "colors.csv"

SECRET_NAME = os.environ.get("SECRET_NAME", "YourGeminiSecretName")
LLM_MODEL_NAME = os.environ.get("INTERPRET_LLM_MODEL", "gemini-2.0-flash")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")

logger = logging.getLogger()
log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
if log_level_str not in valid_log_levels: log_level_str = "INFO"
logger.setLevel(log_level_str)
logger.info(f"Logger initialized with level: {log_level_str}")
logger.info(f"Using Interpreter LLM: {LLM_MODEL_NAME}")

KNOWN_CATEGORIES: Set[str] = set()
KNOWN_STYLES: Set[str] = set()
KNOWN_COLORS: Set[str] = set()
CONFIG_LOAD_ERROR: Optional[str] = None

def load_config_csvs():
    global KNOWN_CATEGORIES, KNOWN_STYLES, KNOWN_COLORS, CONFIG_LOAD_ERROR
    logger.info(f"Attempting to load config data from: {CONFIG_DIR}")
    KNOWN_CATEGORIES.clear(); KNOWN_STYLES.clear(); KNOWN_COLORS.clear(); CONFIG_LOAD_ERROR = None
    try:
        if not CATEGORIES_CSV.is_file(): raise FileNotFoundError(f"Categories CSV not found at {CATEGORIES_CSV}")
        with open(CATEGORIES_CSV, mode='r', encoding='utf-8-sig') as infile:
            reader = csv.reader(infile); header = next(reader); logger.debug(f"Categories CSV header: {header}"); count = 0
            for row in reader:
                if row and row[0].strip(): KNOWN_CATEGORIES.add(row[0].strip().lower()); count += 1
            logger.info(f"Loaded {count} categories.")
            if count == 0: logger.warning(f"'{CATEGORIES_CSV.name}' contained no data rows.")
        if STYLES_CSV.is_file():
            with open(STYLES_CSV, mode='r', encoding='utf-8-sig') as infile:
                reader = csv.reader(infile); header = next(reader); logger.debug(f"Styles CSV header: {header}"); count = 0
                for row in reader:
                    if row and row[0].strip():
                        style = row[0].strip().lower()
                        if style not in KNOWN_STYLES: KNOWN_STYLES.add(style); count += 1
                logger.info(f"Loaded {count} unique styles.")
                if count == 0: logger.warning(f"'{STYLES_CSV.name}' contained no data rows.")
        else: logger.warning(f"Styles CSV not found at {STYLES_CSV}, style checking unavailable.")
        if COLORS_CSV.is_file():
             with open(COLORS_CSV, mode='r', encoding='utf-8-sig') as infile:
                reader = csv.reader(infile); header = next(reader); logger.debug(f"Colors CSV header: {header}"); count = 0
                for row in reader:
                     if row and row[0].strip():
                         color = row[0].strip().lower()
                         if color not in KNOWN_COLORS: KNOWN_COLORS.add(color); count += 1
                logger.info(f"Loaded {count} unique colors.")
                if count == 0: logger.warning(f"'{COLORS_CSV.name}' contained no data rows.")
        else: logger.warning(f"Colors CSV not found at {COLORS_CSV}, color checking unavailable.")
    except FileNotFoundError as e: logger.error(f"Config loading failed: {e}"); CONFIG_LOAD_ERROR = str(e)
    except Exception as e: logger.exception("CRITICAL ERROR loading config CSVs!"); CONFIG_LOAD_ERROR = f"Unexpected error loading config CSVs: {e}"
    logger.debug(f"Final loaded category count: {len(KNOWN_CATEGORIES)}")
    logger.debug(f"Final loaded style count: {len(KNOWN_STYLES)}")
    logger.debug(f"Final loaded color count: {len(KNOWN_COLORS)}")

load_config_csvs()

secrets_manager = None
BOTO3_CLIENT_ERROR = None
try:
    session = boto3.session.Session()
    secrets_manager = session.client(service_name='secretsmanager', region_name=AWS_REGION)
except Exception as e:
    logger.exception("CRITICAL ERROR initializing Boto3 client!")
    BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 client: {e}"

API_KEY_CACHE: Dict[str, Optional[str]] = {}

def get_secret_value(secret_name: str, key_name: str) -> Optional[str]:
    is_local = os.environ.get("IS_LOCAL", "false").lower() == "true"
    if is_local:
        direct_key = os.environ.get(key_name)
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
        elif 'SecretBinary' in response:
             try: secret_dict = json.loads(response['SecretBinary'].decode('utf-8'))
             except (json.JSONDecodeError, UnicodeDecodeError) as e: logger.error(f"Failed binary decode: {e}"); return None
        else: logger.error("Secret value not found."); return None

        if not isinstance(secret_dict, dict): logger.error("Parsed secret is not dict."); return None
        key_value = secret_dict.get(key_name)
        if not key_value or not isinstance(key_value, str):
            logger.error(f"Key '{key_name}' not found or not string in secret '{secret_name}'.")
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

def lambda_handler(event, context):
    if CONFIG_LOAD_ERROR:
        logger.error(f"Config load failure: {CONFIG_LOAD_ERROR}")
        return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Config Error: {CONFIG_LOAD_ERROR}"})}
    if BOTO3_CLIENT_ERROR:
        logger.error(f"Boto3 init failure: {BOTO3_CLIENT_ERROR}")
        return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": BOTO3_CLIENT_ERROR})}
    if not GEMINI_SDK_AVAILABLE:
        return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "Gemini SDK unavailable."})}

    logger.info(f"Received event: {json.dumps(event)}")

    try:
        if isinstance(event.get('body'), str): body = json.loads(event['body']); logger.debug("Parsed body from API GW event.")
        elif isinstance(event, dict) and 'query' in event and 'category' in event and 'country' in event: body = event; logger.debug("Using direct event payload.")
        else: raise ValueError("Invalid input structure (missing query, category, or country).")
        user_query = body.get('query')
        category = body.get('category')
        country = body.get('country')
        if not user_query or not category or not country: raise ValueError("Missing required fields: query, category, or country.")
        logger.info(f"Interpreting Query: '{user_query}' for Cat: '{category}', Country: '{country}'")
    except (json.JSONDecodeError, ValueError, TypeError) as e:
        logger.error(f"Request parsing error: {e}")
        return {"statusCode": 400, "body": json.dumps({"status": "error", "error_message": f"Invalid input: {e}"})}

    google_api_key = get_secret_value(SECRET_NAME, "GOOGLE_API_KEY")
    if not google_api_key:
         return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "API key config error (Google)."})}

    try:
         genai.configure(api_key=google_api_key)
         model = genai.GenerativeModel(LLM_MODEL_NAME)
    except Exception as configure_err:
         actual_error = str(configure_err)
         logger.error(f"Gemini SDK config error: {actual_error}", exc_info=True)
         if "model not found" in actual_error.lower() or "invalid api key" in actual_error.lower():
              return {"statusCode": 400, "body": json.dumps({"status": "error", "error_message": f"LLM config error: {actual_error}"})}
         else:
              return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "LLM SDK configuration error."})}

    known_styles_list = sorted([s.title() for s in KNOWN_STYLES])
    known_colors_list = sorted([c.title() for c in KNOWN_COLORS])

    prompt = f"""Analyze the user query strictly within the given fashion context.
    Context:
    - Category: "{category}"
    - Country: "{country}"
    - List of All Known Styles (global): {json.dumps(known_styles_list) if known_styles_list else "None Provided"}
    - List of All Known Colors (global): {json.dumps(known_colors_list) if known_colors_list else "None Provided"}

    User Query: "{user_query}"

    Instructions:
    1.  Identify the primary analysis task based on the User Query's intent. Choose ONE from: ['get_trend', 'get_forecast', 'get_recommendation', 'compare_items', 'summarize_category', 'summarize_mega_trends', 'qa_web_only', 'qa_internal_only', 'qa_combined', 'unknown']. **Prioritize 'summarize_mega_trends' if the query uses keywords like 'mega', 'hot', 'hottest', or 'rising trends' AND does not mention specific items/styles/colors.** Otherwise, determine intent based on keywords like 'forecast', 'recommend', 'compare', 'summarize category', 'trend', 'why', 'news', etc.
    2.  Determine the necessary data sources required for the identified primary_task. Choose one or more from: ['internal_trends_category', 'internal_trends_item', 'internal_forecast', 'internal_mega', 'web_search', 'clarify']. Follow these rules STRICTLY:
        -   Web Search Rule: You MUST include 'web_search' if the query explicitly asks 'why', mentions 'news', 'sentiment', 'competitors', 'web', 'web search', 'hot' trends, 'this week', 'global' trends, or clearly requires external context/reasoning.
        -   Item Detail Rule: If step 3 identifies ANY subjects in `specific_known_subjects` AND the task requires item-level detail (like 'get_forecast', 'get_recommendation' for an item, 'compare_items', 'get_trend' for a specific item), you MUST include 'internal_trends_item'. You should ALSO include 'internal_forecast' if the primary_task is 'get_forecast'.
        -   Category Context Rule: If the task is broad ('summarize_category', 'get_trend' for the whole category without specific items), use 'internal_trends_category'. ALSO, if the task is 'get_trend' or 'qa_combined' or 'qa_internal_only' or 'qa_web_only' and step 3 identifies items in `unmapped_items` but NOT in `specific_known_subjects`, you MUST include 'internal_trends_category' to provide context.
        -   Mega Trends Rule: Use 'internal_mega' ONLY if the primary_task is 'summarize_mega_trends'. You MUST NOT include 'internal_mega' if any other internal source ('internal_trends_category', 'internal_trends_item', 'internal_forecast') is selected. Also (as checked in step 3), DO NOT use 'internal_mega' if step 3 identifies ANY subjects in `specific_known_subjects` OR `unmapped_items`.
        -   Clarification Rule: If the query is too ambiguous, invalid, lacks specifics needed for the task (e.g., forecast without an item), or falls outside the Category/Country context, use ONLY 'clarify'.
    3.  Extract key entities mentioned in the User Query. Apply these rules STRICTLY:
        -   First, identify all potential fashion subjects (styles, colors, items like 'bomber jacket') in the query.
        -   For EACH potential subject:
            a. Check for an exact case-insensitive match in the 'All Known Styles' or 'All Known Colors' lists.
            b. If a match IS found: Check if the matched term is appropriate for the stated Category context (e.g., 'Dresses' style is inappropriate for 'Shirts' Category). If it IS appropriate, determine if it's a 'style' or 'color' and add `{{"subject": "Matched Term Title Case", "type": "style|color"}}` to the `specific_known_subjects` list. If it is NOT appropriate for the category, add the term (Title Case) to `unmapped_items`.
            c. If NO exact match is found in the known lists: Add the term (Title Case) to the `unmapped_items` list.
            d. DO NOT guess or find the 'closest' match. Only exact matches are processed for `specific_known_subjects`.
        -   `specific_known_subjects`: List of objects for matched, category-appropriate subjects. Can be empty.
        -   `unmapped_items`: List of terms that were not exact matches, were category-inappropriate, or other potential fashion items. Can be empty. Use Title Case.
        -   `timeframe_reference`: Any mention of time (e.g., "next 6 months", "latest"). Return null if none found.
        -   `attributes`: Any other descriptors (e.g., "material:linen", "price:high"). Return [] if none found.
    4.  Determine the overall 'status'. It MUST be 'needs_clarification' ONLY if 'clarify' is in `required_sources` OR if step 3 found category-inappropriate items that prevent analysis. Otherwise (even if 'web_search' is required), it MUST be 'success'.
    5.  Provide a concise 'clarification_needed' message (string) ONLY if status is 'needs_clarification', otherwise it MUST be null.

    Output ONLY a valid JSON object following this exact structure:
    {{
      "status": "success | needs_clarification",
      "primary_task": "string | null",
      "required_sources": ["string", ...],
      "query_subjects": {{
        "specific_known": [ {{ "subject": "string (Title Case)", "type": "color | style" }} ],
        "unmapped_items": ["string (Title Case)", ...]
      }},
      "timeframe_reference": "string | null",
      "attributes": ["string", ...],
      "clarification_needed": "string | null"
    }}
    """
    logger.debug("Prompt constructed.")

    logger.info(f"Calling LLM: {LLM_MODEL_NAME}...")
    try:
        generation_config = genai.types.GenerationConfig(response_mime_type="application/json")
        response = model.generate_content(prompt, generation_config=generation_config)
        logger.info("LLM response received.")
        logger.debug(f"LLM Raw Response Text:\n{response.text}")
    except Exception as llm_err:
         logger.error(f"LLM API call failed: {llm_err}", exc_info=True)
         return {"statusCode": 502, "body": json.dumps({"status": "error", "error_message": f"LLM API call failed: {str(llm_err)}"})}

    try:
        cleaned_text = response.text.strip()
        if cleaned_text.startswith("```json"): cleaned_text = cleaned_text[7:]
        if cleaned_text.endswith("```"): cleaned_text = cleaned_text[:-3]
        cleaned_text = cleaned_text.strip()
        if not cleaned_text: raise ValueError("LLM returned empty response after cleaning markdown.")
        llm_output = json.loads(cleaned_text)

        required_keys = ["status", "primary_task", "required_sources", "query_subjects", "timeframe_reference", "attributes", "clarification_needed"]
        missing_keys = [key for key in required_keys if key not in llm_output]
        if missing_keys: raise ValueError(f"LLM output missing required keys: {', '.join(missing_keys)}.")
        if not isinstance(llm_output.get("required_sources"), list): raise ValueError("LLM 'required_sources' not list.")
        query_subjects = llm_output.get("query_subjects")
        if not isinstance(query_subjects, dict): raise ValueError("LLM 'query_subjects' not dict.")
        if "specific_known" not in query_subjects or "unmapped_items" not in query_subjects: raise ValueError("LLM 'query_subjects' missing keys.")
        if not isinstance(query_subjects["unmapped_items"], list): raise ValueError("LLM 'unmapped_items' not list.")
        specific_known = query_subjects["specific_known"]
        if not isinstance(specific_known, list): raise ValueError("LLM 'specific_known' not list.")
        for item in specific_known:
            if not isinstance(item, dict): raise ValueError(f"Item in 'specific_known' not dict: {item}")
            if "subject" not in item or "type" not in item: raise ValueError(f"Item missing 'subject'/'type': {item}")
            if item["type"] not in ["color", "style"]: raise ValueError(f"Invalid type '{item['type']}': {item}")
            if not isinstance(item.get("subject"), str): raise ValueError(f"Subject not string: {item}")

        primary_task_llm = llm_output.get("primary_task")
        required_sources_llm = llm_output.get("required_sources", [])
        unmapped_items_llm = query_subjects.get("unmapped_items", [])

        if not isinstance(required_sources_llm, list):
             logger.error(f"LLM output 'required_sources' was not a list after initial get: {required_sources_llm}. Setting to empty list.")
             required_sources_llm = []

        if primary_task_llm == "summarize_mega_trends" and "internal_mega" not in required_sources_llm:
            logger.warning("Post-LLM: Adding 'internal_mega' because primary task is summarize_mega_trends.")
            required_sources_llm.append("internal_mega")

        if primary_task_llm in ["get_trend", "qa_combined", "qa_internal_only", "qa_web_only"] and \
           not specific_known and unmapped_items_llm and \
           "internal_trends_category" not in required_sources_llm:
            logger.warning("Post-LLM: Adding 'internal_trends_category' based on task type and unmapped items.")
            required_sources_llm.append("internal_trends_category")

        if specific_known or unmapped_items_llm:
            if "internal_mega" in required_sources_llm:
                logger.warning("Post-LLM: Removing 'internal_mega' source because specific/unmapped subjects were found.")
                required_sources_llm = [s for s in required_sources_llm if s != "internal_mega"]
                if not required_sources_llm:
                     if specific_known or unmapped_items_llm:
                          required_sources_llm.append("internal_trends_category")
                          logger.warning("Post-LLM: Added 'internal_trends_category' as fallback after removing 'internal_mega'.")

        has_mega = "internal_mega" in required_sources_llm
        has_other_internal = any(s in required_sources_llm for s in ["internal_trends_category", "internal_trends_item", "internal_forecast"])

        if has_mega and has_other_internal:
             logger.warning("Post-LLM: Found both 'internal_mega' and other internal sources. Enforcing mutual exclusivity.")
             if primary_task_llm == "summarize_mega_trends":
                  required_sources_llm = [s for s in required_sources_llm if s == "internal_mega" or s not in ["internal_trends_category", "internal_trends_item", "internal_forecast"]]
                  logger.warning("Post-LLM: Prioritized 'internal_mega' for summarize_mega_trends task.")
             else:
                  required_sources_llm = [s for s in required_sources_llm if s != "internal_mega"]
                  logger.warning("Post-LLM: Removed 'internal_mega' because other internal sources were present.")

        llm_output["required_sources"] = sorted(list(set(required_sources_llm)))


        if "clarify" in llm_output.get("required_sources", []) and llm_output.get("status") != "needs_clarification":
            logger.warning("Forcing status to 'needs_clarification' due to 'clarify' source.")
            llm_output["status"] = "needs_clarification"
        if llm_output.get("status") == "needs_clarification" and not llm_output.get("clarification_needed"):
             logger.warning("Adding generic clarification message.")
             llm_output["clarification_needed"] = "Query requires clarification. Please be more specific or ensure terms are relevant to the category."

        logger.info(f"LLM interpretation successful (post-processed). Task: {llm_output.get('primary_task')}, Status: {llm_output.get('status')}, Sources: {llm_output.get('required_sources')}")
        llm_output['original_context'] = {'category': category, 'country': country, 'query': user_query}
        return { "statusCode": 200, "body": json.dumps(llm_output) }

    except (json.JSONDecodeError, ValueError, TypeError) as e:
         logger.error(f"Failed parsing/validating LLM response: {e}", exc_info=True)
         logger.error(f"LLM Raw Text was: {response.text}")
         return { "statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Failed processing LLM response: {e}", "llm_raw_output": response.text}) }

    except Exception as e:
        logger.exception("Unhandled error during interpretation.")
        return { "statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Internal server error: {str(e)}"}) }