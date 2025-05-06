# import logging
# import os
# import json
# import csv
# from pathlib import Path
# from typing import Dict, List, Any, Optional, Set
# import re # Keep existing imports
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
# # --- Configuration (Using User's Code Structure) ---
# LAMBDA_ROOT = Path(__file__).resolve().parent
# CONFIG_DIR = LAMBDA_ROOT / "config_data"
# CATEGORIES_CSV = CONFIG_DIR / "categories.csv"
# STYLES_CSV = CONFIG_DIR / "styles.csv"
# COLORS_CSV = CONFIG_DIR / "colors.csv"
#
# SECRET_NAME = os.environ.get("SECRET_NAME", "YourGeminiSecretName") # From user code
# LLM_MODEL_NAME = os.environ.get("INTERPRET_LLM_MODEL", "gemini-2.5-flash-preview-04-17") # From user code
# AWS_REGION = os.environ.get("AWS_REGION", "us-west-2") # From user code
#
# # --- Initialize Logger (Using User's Code) ---
# logger = logging.getLogger()
# log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
# valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
# if log_level_str not in valid_log_levels: log_level_str = "INFO"
# logger.setLevel(log_level_str)
# logger.info(f"Logger initialized with level: {log_level_str}")
# logger.info(f"Using Interpreter LLM: {LLM_MODEL_NAME}")
#
# # --- Globals for Loaded Config Data (Using User's Code Structure) ---
# KNOWN_CATEGORIES: Set[str] = set()
# KNOWN_STYLES: Set[str] = set()
# KNOWN_COLORS: Set[str] = set()
# CONFIG_LOAD_ERROR: Optional[str] = None
#
# # --- Function to Load Config Data (Using User's Code) ---
# # Assuming this function works correctly with the updated single-column CSVs
# def load_config_csvs():
#     global KNOWN_CATEGORIES, KNOWN_STYLES, KNOWN_COLORS, CONFIG_LOAD_ERROR
#     logger.info(f"Attempting to load config data from: {CONFIG_DIR}")
#     KNOWN_CATEGORIES.clear(); KNOWN_STYLES.clear(); KNOWN_COLORS.clear(); CONFIG_LOAD_ERROR = None
#     try:
#         # Categories
#         if not CATEGORIES_CSV.is_file(): raise FileNotFoundError(f"Categories CSV not found at {CATEGORIES_CSV}")
#         with open(CATEGORIES_CSV, mode='r', encoding='utf-8-sig') as infile:
#             reader = csv.reader(infile); header = next(reader); logger.debug(f"Categories CSV header: {header}"); count = 0
#             for row in reader:
#                 # Reads the first column (index 0)
#                 if row and row[0].strip(): KNOWN_CATEGORIES.add(row[0].strip().lower()); count += 1
#             logger.info(f"Loaded {count} categories.")
#             if count == 0: logger.warning(f"'{CATEGORIES_CSV.name}' contained no data rows.")
#         # Styles
#         if STYLES_CSV.is_file():
#             with open(STYLES_CSV, mode='r', encoding='utf-8-sig') as infile:
#                 reader = csv.reader(infile); header = next(reader); logger.debug(f"Styles CSV header: {header}"); count = 0
#                 for row in reader:
#                     # Reads the first column (index 0)
#                     if row and row[0].strip():
#                         style = row[0].strip().lower()
#                         if style not in KNOWN_STYLES: KNOWN_STYLES.add(style); count += 1
#                 logger.info(f"Loaded {count} unique styles.")
#                 if count == 0: logger.warning(f"'{STYLES_CSV.name}' contained no data rows.")
#         else: logger.warning(f"Styles CSV not found at {STYLES_CSV}, style checking unavailable.")
#         # Colors
#         if COLORS_CSV.is_file():
#              with open(COLORS_CSV, mode='r', encoding='utf-8-sig') as infile:
#                 reader = csv.reader(infile); header = next(reader); logger.debug(f"Colors CSV header: {header}"); count = 0
#                 for row in reader:
#                      # Reads the first column (index 0)
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
# # --- Boto3 Client Initialization (Using User's Code) ---
# secrets_manager = None
# BOTO3_CLIENT_ERROR = None
# try:
#     session = boto3.session.Session()
#     secrets_manager = session.client(service_name='secretsmanager', region_name=AWS_REGION)
# except Exception as e:
#     logger.exception("CRITICAL ERROR initializing Boto3 client!")
#     BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 client: {e}"
#
# # --- API Key Caching (Using User's Code) ---
# API_KEY_CACHE: Dict[str, Optional[str]] = {}
#
# # --- Helper Function to Get Secrets (Using User's Code) ---
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
#         if not isinstance(secret_dict, dict): logger.error("Parsed secret not dict."); return None
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
# # --- Main Lambda Handler ---
# def lambda_handler(event, context):
#     # --- Pre-checks (Using User's Code) ---
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
#     # --- Input Parsing (Using User's Code) ---
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
#     # --- Secret Retrieval (Using User's Code) ---
#     google_api_key = get_secret_value(SECRET_NAME, "GOOGLE_API_KEY")
#     if not google_api_key:
#          return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "API key config error (Google)."})}
#
#     # --- LLM Client Config (Using User's Code) ---
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
#     # --- *** MODIFICATION START: Adjust Prompt and Post-Processing *** ---
#     known_styles_list = sorted([s.title() for s in KNOWN_STYLES])
#     known_colors_list = sorted([c.title() for c in KNOWN_COLORS])
#
#     # --- Prompt Adjustment: Relaxed Mega Trends Exclusivity ---
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
#     1.  Identify the primary analysis task based on the User Query's intent. Choose ONE from: ['get_trend', 'get_forecast', 'get_recommendation', 'compare_items', 'summarize_category', 'summarize_mega_trends', 'qa_web_only', 'qa_internal_only', 'qa_combined', 'unknown']. **Prioritize 'summarize_mega_trends' if the query uses keywords like 'mega', 'hot', 'hottest', or 'rising trends' AND does not mention specific items/styles/colors.** Otherwise, determine intent based on keywords like 'forecast', 'recommend', 'compare', 'summarize category', 'trend', 'why', 'news', etc.
#     2.  Determine the necessary data sources required for the identified primary_task. Choose one or more from: ['internal_trends_category', 'internal_trends_item', 'internal_forecast', 'internal_mega', 'web_search', 'clarify']. Follow these rules STRICTLY:
#         -   Web Search Rule: You MUST include 'web_search' if the query explicitly asks 'why', mentions 'news', 'sentiment', 'competitors', 'web', 'web search', 'hot' trends, 'this week', 'global' trends, or clearly requires external context/reasoning.
#         -   Item Detail Rule: If step 3 identifies ANY subjects in `specific_known_subjects` AND the task requires item-level detail (like 'get_forecast', 'get_recommendation' for an item, 'compare_items', 'get_trend' for a specific item), you MUST include 'internal_trends_item'. You should ALSO include 'internal_forecast' if the primary_task is 'get_forecast'.
#         -   Category Context Rule: If the task is broad ('summarize_category', 'get_trend' for the whole category without specific items), use 'internal_trends_category'. ALSO, if the task is 'get_trend' or 'qa_combined' or 'qa_internal_only' or 'qa_web_only' and step 3 identifies items in `unmapped_items` but NOT in `specific_known_subjects`, you MUST include 'internal_trends_category' to provide context.
#         -   Mega Trends Rule: Use 'internal_mega' if the primary_task is 'summarize_mega_trends'. **(REMOVED EXCLUSIVITY CLAUSE)** Also (as checked in step 3), DO NOT use 'internal_mega' if step 3 identifies ANY subjects in `specific_known_subjects` OR `unmapped_items`.
#         -   Clarification Rule: If the query is too ambiguous, invalid, lacks specifics needed for the task (e.g., forecast without an item), or falls outside the Category/Country context, use ONLY 'clarify'.
#     3.  Extract key entities mentioned in the User Query. Apply these rules STRICTLY:
#         -   First, identify all potential fashion subjects (styles, colors, items like 'bomber jacket') in the query.
#         -   For EACH potential subject:
#             a. Check for an exact case-insensitive match in the 'All Known Styles' or 'All Known Colors' lists.
#             b. If a match IS found: Determine if it's a 'style' or 'color'. **If it's a 'color', add it directly** to the `specific_known_subjects` list as `{{"subject": "Matched Term Title Case", "type": "color"}}`. **If it's a 'style', THEN check if the matched style is appropriate** for the stated Category context (e.g., 'Dresses' style is inappropriate for 'Shirts' Category). If the style IS appropriate, add it to `specific_known_subjects` as `{{"subject": "Matched Term Title Case", "type": "style"}}`. If the style is NOT appropriate for the category, add the term (Title Case) to `unmapped_items`.
#             c. If NO exact match is found in the known lists: Add the term (Title Case) to the `unmapped_items` list.
#             d. DO NOT guess or find the 'closest' match. Only exact matches are processed for `specific_known_subjects`.
#         -   `specific_known_subjects`: List of objects for matched subjects (colors are always added if matched, styles only if matched AND category-appropriate). Can be empty.
#         -   `unmapped_items`: List of terms that were not exact matches, were category-inappropriate styles, or other potential fashion items. Can be empty. Use Title Case.
#         -   `timeframe_reference`: Any mention of time (e.g., "next 6 months", "latest"). Return null if none found.
#         -   `attributes`: Any other descriptors (e.g., "material:linen", "price:high"). Return [] if none found.
#     4.  Determine the overall 'status'. It MUST be 'needs_clarification' ONLY if 'clarify' is in `required_sources` OR if step 3 added items to `unmapped_items` because they were category-inappropriate styles that prevent analysis. Otherwise (even if 'web_search' is required or there are other `unmapped_items`), it MUST be 'success'.
#     5.  Provide a concise 'clarification_needed' message (string) ONLY if status is 'needs_clarification', otherwise it MUST be null. Explain *why* (e.g., "Style 'Dresses' is not applicable to the 'Shirts' category. Please specify a relevant style or remove it.", or "Query is too ambiguous...").
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
#     # --- LLM Call (Using User's Code) ---
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
#     # --- LLM Response Parsing and Validation (Using User's Code) ---
#     try:
#         cleaned_text = response.text.strip()
#         if cleaned_text.startswith("```json"): cleaned_text = cleaned_text[7:]
#         if cleaned_text.endswith("```"): cleaned_text = cleaned_text[:-3]
#         cleaned_text = cleaned_text.strip()
#         if not cleaned_text: raise ValueError("LLM returned empty response after cleaning markdown.")
#         llm_output = json.loads(cleaned_text)
#
#         # Basic structure validation (Keep as before)
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
#         # --- Post-LLM Source Rule Enforcement (MODIFIED) ---
#         primary_task_llm = llm_output.get("primary_task")
#         required_sources_llm = llm_output.get("required_sources", [])
#         unmapped_items_llm = query_subjects.get("unmapped_items", [])
#
#         # Ensure required_sources_llm is a list
#         if not isinstance(required_sources_llm, list):
#              logger.error(f"LLM output 'required_sources' was not a list after initial get: {required_sources_llm}. Setting to empty list.")
#              required_sources_llm = []
#
#         # Convert to set for easier manipulation
#         required_sources_set = set(required_sources_llm)
#
#         # --- NEW: Enforce sources for summarize_mega_trends ---
#         if primary_task_llm == "summarize_mega_trends":
#             logger.warning("Post-LLM: Enforcing sources for summarize_mega_trends task.")
#             required_sources_set.add("internal_mega")
#             required_sources_set.add("internal_trends_category")
#             required_sources_set.add("web_search")
#             # Remove item/forecast sources if they somehow got added by LLM for this task
#             required_sources_set.discard("internal_trends_item")
#             required_sources_set.discard("internal_forecast")
#         # --- END NEW ---
#         else:
#              # Apply previous post-LLM rules ONLY if task is NOT summarize_mega_trends
#              if primary_task_llm == "get_trend" and "internal_mega" not in required_sources_set: # Make sure not to add if mega is enforced
#                 # Rule: Add internal_mega if task is summarize_mega_trends (handled above)
#                 pass # No longer needed here
#
#              # Rule: Add category context if specific task, no known items, but unmapped items exist
#              if primary_task_llm in ["get_trend", "qa_combined", "qa_internal_only", "qa_web_only"] and \
#                 not specific_known and unmapped_items_llm and \
#                 "internal_trends_category" not in required_sources_set:
#                  logger.warning("Post-LLM: Adding 'internal_trends_category' based on task type and unmapped items.")
#                  required_sources_set.add("internal_trends_category")
#
#              # Rule: Remove internal_mega if any subjects found (should not apply if task is mega_trends)
#              if specific_known or unmapped_items_llm:
#                  if "internal_mega" in required_sources_set:
#                      logger.warning("Post-LLM: Removing 'internal_mega' source because specific/unmapped subjects were found.")
#                      required_sources_set.discard("internal_mega")
#                      # Add fallback if removing mega leaves no sources
#                      if not required_sources_set:
#                           required_sources_set.add("internal_trends_category") # Default to category context
#                           logger.warning("Post-LLM: Added 'internal_trends_category' as fallback after removing 'internal_mega'.")
#
#              # Rule: Enforce mutual exclusivity between mega and other internal sources (Only if task is NOT mega_trends)
#              has_mega = "internal_mega" in required_sources_set
#              has_other_internal = any(s in required_sources_set for s in ["internal_trends_category", "internal_trends_item", "internal_forecast"])
#
#              if has_mega and has_other_internal:
#                   logger.warning("Post-LLM: Found both 'internal_mega' and other internal sources. Enforcing mutual exclusivity (non-mega_trends task).")
#                   # Keep other internal, discard mega (most common case if LLM gets confused)
#                   required_sources_set.discard("internal_mega")
#                   logger.warning("Post-LLM: Removed 'internal_mega' because other internal sources were present.")
#
#         # Convert back to sorted list
#         llm_output["required_sources"] = sorted(list(required_sources_set))
#
#         # Status Consistency Check (Keep as before)
#         if "clarify" in llm_output.get("required_sources", []) and llm_output.get("status") != "needs_clarification":
#             logger.warning("Post-LLM: Forcing status to 'needs_clarification' due to 'clarify' source.")
#             llm_output["status"] = "needs_clarification"
#         if llm_output.get("status") == "needs_clarification" and not llm_output.get("clarification_needed"):
#              logger.warning("Post-LLM: Status is 'needs_clarification' but no message provided. Adding generic message.")
#              llm_output["clarification_needed"] = "Query requires clarification. Please be more specific or ensure terms are relevant to the category."
#
#         # --- END MODIFICATION ---
#
#         logger.info(f"LLM interpretation successful (post-processed). Task: {llm_output.get('primary_task')}, Status: {llm_output.get('status')}, Sources: {llm_output.get('required_sources')}")
#         # Add original context back for downstream use
#         llm_output['original_context'] = {'category': category, 'country': country, 'query': user_query}
#         # Ensure final return format matches API GW expectations
#         return { "statusCode": 200, "body": json.dumps(llm_output) }
#
#     # --- Error Handling (Using User's Code Structure) ---
#     except (json.JSONDecodeError, ValueError, TypeError) as e:
#          logger.error(f"Failed parsing/validating LLM response: {e}", exc_info=True)
#          logger.error(f"LLM Raw Text was: {response.text}") # Log raw text on parse error
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
# import re # Keep existing imports
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
# # --- Configuration ---
# LAMBDA_ROOT = Path(__file__).resolve().parent
# CONFIG_DIR = LAMBDA_ROOT / "config_data"
# CATEGORIES_CSV = CONFIG_DIR / "categories.csv"
# STYLES_CSV = CONFIG_DIR / "styles.csv"
# COLORS_CSV = CONFIG_DIR / "colors.csv"
#
# SECRET_NAME = os.environ.get("SECRET_NAME", "YourGeminiSecretName")
# LLM_MODEL_NAME = os.environ.get("INTERPRET_LLM_MODEL", "gemini-2.5-flash-preview-04-17")
# AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
#
# # --- *** MODIFICATION START: Add Constants *** ---
# BRAND_ANALYSIS_CATEGORY = "BRAND_ANALYSIS" # Placeholder category name
# INTERNAL_BRAND_PERFORMANCE_SOURCE = "internal_brand_performance" # New source
# ANALYZE_BRAND_TASK = "analyze_brand_deep_dive" # New task name
# # --- *** MODIFICATION END: Add Constants *** ---
#
# # --- Logger Setup ---
# logger = logging.getLogger()
# log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
# valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
# if log_level_str not in valid_log_levels: log_level_str = "INFO"
# logger.setLevel(log_level_str)
# logger.info(f"Logger initialized with level: {log_level_str}")
# logger.info(f"Using Interpreter LLM: {LLM_MODEL_NAME}")
#
# # --- Globals & Config Loading ---
# KNOWN_CATEGORIES: Set[str] = set()
# KNOWN_STYLES: Set[str] = set()
# KNOWN_COLORS: Set[str] = set()
# CONFIG_LOAD_ERROR: Optional[str] = None
#
# def load_config_csvs():
#     # (Keep existing load_config_csvs function as is)
#     global KNOWN_CATEGORIES, KNOWN_STYLES, KNOWN_COLORS, CONFIG_LOAD_ERROR
#     logger.info(f"Attempting to load config data from: {CONFIG_DIR}")
#     KNOWN_CATEGORIES.clear(); KNOWN_STYLES.clear(); KNOWN_COLORS.clear(); CONFIG_LOAD_ERROR = None
#     try:
#         # Categories
#         if not CATEGORIES_CSV.is_file(): raise FileNotFoundError(f"Categories CSV not found at {CATEGORIES_CSV}")
#         with open(CATEGORIES_CSV, mode='r', encoding='utf-8-sig') as infile:
#             reader = csv.reader(infile); header = next(reader); logger.debug(f"Categories CSV header: {header}"); count = 0
#             for row in reader:
#                 if row and row[0].strip(): KNOWN_CATEGORIES.add(row[0].strip().lower()); count += 1
#             logger.info(f"Loaded {count} categories.")
#             if count == 0: logger.warning(f"'{CATEGORIES_CSV.name}' contained no data rows.")
#         # Styles
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
#         # Colors
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
# # --- Boto3 Client & Secret Handling ---
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
#     # (Keep existing get_secret_value function as is)
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
#         if not isinstance(secret_dict, dict): logger.error("Parsed secret not dict."); return None
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
# # --- *** MODIFICATION START: Helper to Extract Brand *** ---
# def extract_brand_from_query(query: str) -> Optional[str]:
#     """
#     Simple extraction of potential domain names or keywords after 'analyze brand'.
#     Tries to find domain first, then looks for terms after 'analyze brand' or 'brand'.
#     Returns cleaned domain/brand name or None.
#     """
#     if not query or not isinstance(query, str):
#         return None
#
#     query_lower = query.lower()
#
#     # Pattern 1: Look for domain-like strings (e.g., nike.com, brand-x.co.uk)
#     # Allows letters, numbers, hyphens in domain name, requires a dot and 2+ letter TLD
#     domain_match = re.search(r'([\w-]+\.[a-z]{2,})', query_lower)
#     if domain_match:
#         # Basic cleaning (remove http/www) - using the cleaning function below
#         # Use the matched group directly
#         cleaned = clean_domain_for_lookup(domain_match.group(1))
#         if cleaned:
#             logger.info(f"Extracted domain: {cleaned}")
#             return cleaned
#
#     # Pattern 2: Look for phrases like "analyze brand [BRAND NAME]" or "tell me about [BRAND NAME]"
#     # This is less reliable and might need refinement based on typical queries
#     brand_keywords = ["analyze brand", "brand analysis", "competitors for", "tell me about", "analyze", "brand"]
#     target_brand = None
#     for keyword in brand_keywords:
#         if keyword in query_lower:
#             # Take the text immediately following the keyword
#             potential_brand = query_lower.split(keyword, 1)[-1].strip()
#             # Remove punctuation from the start/end
#             potential_brand = re.sub(r"^[^\w]+|[^\w]+$", "", potential_brand)
#             # Simple check: if it's not empty and maybe has reasonable length
#             if potential_brand and len(potential_brand) > 1 and len(potential_brand) < 50:
#                  # Basic cleaning - might need more sophistication
#                  # For now, just take the first few words if it looks like a sentence fragment
#                  target_brand = " ".join(potential_brand.split()[:3]) # Limit length
#                  logger.info(f"Extracted potential brand keyword: {target_brand}")
#                  break # Take the first match
#
#     if target_brand:
#          # Maybe try cleaning common suffixes like '.com' if it wasn't caught by domain regex
#          if target_brand.endswith('.com'): target_brand = target_brand[:-4]
#          return target_brand.strip() # Return extracted name
#
#     logger.warning(f"Could not extract brand/domain from query: {query}")
#     return None
#
# def clean_domain_for_lookup(input_domain: str) -> str:
#     """Removes http(s):// and www. prefixes from a domain for lookup."""
#     if not isinstance(input_domain, str): return ""
#     cleaned = re.sub(r'^https?:\/\/', '', input_domain.strip(), flags=re.IGNORECASE)
#     cleaned = re.sub(r'^www\.', '', cleaned, flags=re.IGNORECASE)
#     # Remove trailing slash if present
#     if cleaned.endswith('/'): cleaned = cleaned[:-1]
#     return cleaned.lower()
# # --- *** MODIFICATION END: Helper to Extract Brand *** ---
#
#
# # --- Main Lambda Handler ---
# def lambda_handler(event, context):
#     # --- Pre-checks ---
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
#     # --- Input Parsing ---
#     try:
#         if isinstance(event.get('body'), str): body = json.loads(event['body']); logger.debug("Parsed body from API GW event.")
#         elif isinstance(event, dict) and 'query' in event and 'category' in event and 'country' in event: body = event; logger.debug("Using direct event payload.")
#         else: raise ValueError("Invalid input structure (missing query, category, or country).")
#         user_query = body.get('query')
#         category = body.get('category') # Might be placeholder
#         country = body.get('country')
#         if not user_query or category is None or not country: # Allow empty category initially
#              raise ValueError("Missing required fields: query, category, or country.")
#         logger.info(f"Input - Query: '{user_query}', Cat: '{category}', Country: '{country}'")
#     except (json.JSONDecodeError, ValueError, TypeError) as e:
#         logger.error(f"Request parsing error: {e}")
#         return {"statusCode": 400, "body": json.dumps({"status": "error", "error_message": f"Invalid input: {e}"})}
#
#     # --- *** MODIFICATION START: Handle Placeholder Category *** ---
#     # Check if category matches the placeholder for Brand Analysis
#     if isinstance(category, str) and category.upper() == BRAND_ANALYSIS_CATEGORY:
#         logger.info(f"Detected '{BRAND_ANALYSIS_CATEGORY}' category. Attempting Brand Analysis flow.")
#         extracted_brand = extract_brand_from_query(user_query)
#
#         if extracted_brand:
#             logger.info(f"Extracted brand '{extracted_brand}' for analysis.")
#             # Construct direct output for brand analysis task
#             output_payload = {
#                 "status": "success",
#                 "primary_task": ANALYZE_BRAND_TASK,
#                 "required_sources": [INTERNAL_BRAND_PERFORMANCE_SOURCE, "web_search"],
#                 "query_subjects": {
#                     "specific_known": [], # Not applicable for brand analysis
#                     "unmapped_items": [], # Not applicable here
#                     "target_brand": extracted_brand # Store extracted brand
#                 },
#                 "timeframe_reference": None,
#                 "attributes": [],
#                 "clarification_needed": None,
#                 "original_context": { # Pass original context along
#                     'category': category, # The placeholder category
#                     'country': country,
#                     'query': user_query,
#                     'target_brand': extracted_brand # Also add here
#                 }
#             }
#             logger.info(f"Bypassing LLM. Returning direct payload for Brand Analysis.")
#             return {"statusCode": 200, "body": json.dumps(output_payload)}
#         else:
#             # Could not extract brand, need clarification
#             logger.warning(f"Category was '{BRAND_ANALYSIS_CATEGORY}' but could not extract brand from query.")
#             output_payload = {
#                 "status": "needs_clarification",
#                 "primary_task": "unknown",
#                 "required_sources": ["clarify"],
#                 "query_subjects": {"specific_known": [], "unmapped_items": []},
#                 "timeframe_reference": None,
#                 "attributes": [],
#                 "clarification_needed": "Please specify the brand name or website you want to analyze (e.g., 'Analyze Nike.com' or 'Tell me about brand Zara').",
#                 "original_context": {'category': category, 'country': country, 'query': user_query}
#             }
#             return {"statusCode": 200, "body": json.dumps(output_payload)}
#     # --- *** MODIFICATION END: Handle Placeholder Category *** ---
#
#     # --- If not Brand Analysis, proceed with standard LLM Interpretation ---
#     logger.info("Category is not placeholder. Proceeding with standard LLM interpretation.")
#
#     # --- Secret Retrieval ---
#     google_api_key = get_secret_value(SECRET_NAME, "GOOGLE_API_KEY")
#     if not google_api_key:
#          return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "API key config error (Google)."})}
#
#     # --- LLM Client Config ---
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
#     # --- Construct LLM Prompt (Keep original prompt logic) ---
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
#     1.  Identify the primary analysis task based on the User Query's intent. Choose ONE from: ['get_trend', 'get_forecast', 'get_recommendation', 'compare_items', 'summarize_category', 'summarize_mega_trends', 'qa_web_only', 'qa_internal_only', 'qa_combined', 'unknown']. **Prioritize 'summarize_mega_trends' if the query uses keywords like 'mega', 'hot', 'hottest', or 'rising trends' AND does not mention specific items/styles/colors.** Otherwise, determine intent based on keywords like 'forecast', 'recommend', 'compare', 'summarize category', 'trend', 'why', 'news', etc.
#     2.  Determine the necessary data sources required for the identified primary_task. Choose one or more from: ['internal_trends_category', 'internal_trends_item', 'internal_forecast', 'internal_mega', 'web_search', 'clarify']. Follow these rules STRICTLY:
#         -   Web Search Rule: You MUST include 'web_search' if the query explicitly asks 'why', mentions 'news', 'sentiment', 'competitors', 'web', 'web search', 'hot' trends, 'this week', 'global' trends, or clearly requires external context/reasoning.
#         -   Item Detail Rule: If step 3 identifies ANY subjects in `specific_known_subjects` AND the task requires item-level detail (like 'get_forecast', 'get_recommendation' for an item, 'compare_items', 'get_trend' for a specific item), you MUST include 'internal_trends_item'. You should ALSO include 'internal_forecast' if the primary_task is 'get_forecast'.
#         -   Category Context Rule: If the task is broad ('summarize_category', 'get_trend' for the whole category without specific items), use 'internal_trends_category'. ALSO, if the task is 'get_trend' or 'qa_combined' or 'qa_internal_only' or 'qa_web_only' and step 3 identifies items in `unmapped_items` but NOT in `specific_known_subjects`, you MUST include 'internal_trends_category' to provide context.
#         -   Mega Trends Rule: Use 'internal_mega' ONLY if the primary_task is 'summarize_mega_trends'. **YOU MUST NOT include 'internal_mega' if any other internal source ('internal_trends_category', 'internal_trends_item', 'internal_forecast') is selected.** Also (as checked in step 3), DO NOT use 'internal_mega' if step 3 identifies ANY subjects in `specific_known_subjects` OR `unmapped_items`.
#         -   Clarification Rule: If the query is too ambiguous, invalid, lacks specifics needed for the task (e.g., forecast without an item), or falls outside the Category/Country context, use ONLY 'clarify'.
#     3.  Extract key entities mentioned in the User Query. Apply these rules STRICTLY:
#         -   First, identify all potential fashion subjects (styles, colors, items like 'bomber jacket') in the query.
#         -   For EACH potential subject:
#             a. Check for an exact case-insensitive match in the 'All Known Styles' or 'All Known Colors' lists.
#             b. If a match IS found: Determine if it's a 'style' or 'color'. **If it's a 'color', add it directly** to the `specific_known_subjects` list as `{{"subject": "Matched Term Title Case", "type": "color"}}`. **If it's a 'style', THEN check if the matched style is appropriate** for the stated Category context (e.g., 'Dresses' style is inappropriate for 'Shirts' Category). If the style IS appropriate, add it to `specific_known_subjects` as `{{"subject": "Matched Term Title Case", "type": "style"}}`. If the style is NOT appropriate for the category, add the term (Title Case) to `unmapped_items`.
#             c. If NO exact match is found in the known lists: Add the term (Title Case) to the `unmapped_items` list.
#             d. DO NOT guess or find the 'closest' match. Only exact matches are processed for `specific_known_subjects`.
#         -   `specific_known_subjects`: List of objects for matched subjects (colors are always added if matched, styles only if matched AND category-appropriate). Can be empty.
#         -   `unmapped_items`: List of terms that were not exact matches, were category-inappropriate styles, or other potential fashion items. Can be empty. Use Title Case.
#         -   `timeframe_reference`: Any mention of time (e.g., "next 6 months", "latest"). Return null if none found.
#         -   `attributes`: Any other descriptors (e.g., "material:linen", "price:high"). Return [] if none found.
#     4.  Determine the overall 'status'. It MUST be 'needs_clarification' ONLY if 'clarify' is in `required_sources` OR if step 3 added items to `unmapped_items` because they were category-inappropriate styles that prevent analysis. Otherwise (even if 'web_search' is required or there are other `unmapped_items`), it MUST be 'success'.
#     5.  Provide a concise 'clarification_needed' message (string) ONLY if status is 'needs_clarification', otherwise it MUST be null. Explain *why* (e.g., "Style 'Dresses' is not applicable to the 'Shirts' category. Please specify a relevant style or remove it.", or "Query is too ambiguous...").
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
#     # --- LLM Call ---
#     logger.info(f"Calling LLM: {LLM_MODEL_NAME} for standard interpretation...")
#     try:
#         generation_config = genai.types.GenerationConfig(response_mime_type="application/json")
#         response = model.generate_content(prompt, generation_config=generation_config)
#         logger.info("LLM response received.")
#         logger.debug(f"LLM Raw Response Text:\n{response.text}")
#     except Exception as llm_err:
#          logger.error(f"LLM API call failed: {llm_err}", exc_info=True)
#          return {"statusCode": 502, "body": json.dumps({"status": "error", "error_message": f"LLM API call failed: {str(llm_err)}"})}
#
#     # --- LLM Response Parsing and Validation ---
#     try:
#         cleaned_text = response.text.strip()
#         if cleaned_text.startswith("```json"): cleaned_text = cleaned_text[7:]
#         if cleaned_text.endswith("```"): cleaned_text = cleaned_text[:-3]
#         cleaned_text = cleaned_text.strip()
#         if not cleaned_text: raise ValueError("LLM returned empty response after cleaning markdown.")
#         llm_output = json.loads(cleaned_text)
#
#         # Basic structure validation
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
#         # --- Post-LLM Source Rule Enforcement (REVERTED to original logic) ---
#         primary_task_llm = llm_output.get("primary_task")
#         required_sources_llm = llm_output.get("required_sources", [])
#         unmapped_items_llm = query_subjects.get("unmapped_items", [])
#
#         if not isinstance(required_sources_llm, list):
#              logger.error(f"LLM output 'required_sources' was not a list after initial get: {required_sources_llm}. Setting to empty list.")
#              required_sources_llm = []
#
#         required_sources_set = set(required_sources_llm)
#
#         # Apply original post-LLM rules
#         if primary_task_llm == "summarize_mega_trends" and "internal_mega" not in required_sources_set:
#             logger.warning("Post-LLM: Adding 'internal_mega' because primary task is summarize_mega_trends.")
#             required_sources_set.add("internal_mega")
#         if primary_task_llm in ["get_trend", "qa_combined", "qa_internal_only", "qa_web_only"] and \
#            not specific_known and unmapped_items_llm and \
#            "internal_trends_category" not in required_sources_set:
#             logger.warning("Post-LLM: Adding 'internal_trends_category' based on task type and unmapped items.")
#             required_sources_set.add("internal_trends_category")
#         if specific_known or unmapped_items_llm:
#             if "internal_mega" in required_sources_set:
#                 logger.warning("Post-LLM: Removing 'internal_mega' source because specific/unmapped subjects were found.")
#                 required_sources_set.discard("internal_mega")
#                 if not required_sources_set:
#                      required_sources_set.add("internal_trends_category")
#                      logger.warning("Post-LLM: Added 'internal_trends_category' as fallback after removing 'internal_mega'.")
#         has_mega = "internal_mega" in required_sources_set
#         has_other_internal = any(s in required_sources_set for s in ["internal_trends_category", "internal_trends_item", "internal_forecast"])
#         if has_mega and has_other_internal:
#              logger.warning("Post-LLM: Found both 'internal_mega' and other internal sources. Enforcing mutual exclusivity.")
#              if primary_task_llm == "summarize_mega_trends":
#                   logger.warning("Post-LLM: Conflict - Mega task with other internal sources. Prioritizing mega.")
#                   required_sources_set.discard("internal_trends_category"); required_sources_set.discard("internal_trends_item"); required_sources_set.discard("internal_forecast")
#              else:
#                   required_sources_set.discard("internal_mega")
#                   logger.warning("Post-LLM: Removed 'internal_mega' because other internal sources were present.")
#
#         llm_output["required_sources"] = sorted(list(required_sources_set))
#
#         # Status Consistency Check
#         if "clarify" in llm_output.get("required_sources", []) and llm_output.get("status") != "needs_clarification":
#             logger.warning("Post-LLM: Forcing status to 'needs_clarification' due to 'clarify' source.")
#             llm_output["status"] = "needs_clarification"
#         if llm_output.get("status") == "needs_clarification" and not llm_output.get("clarification_needed"):
#              logger.warning("Post-LLM: Status is 'needs_clarification' but no message provided. Adding generic message.")
#              llm_output["clarification_needed"] = "Query requires clarification. Please be more specific or ensure terms are relevant to the category."
#
#         logger.info(f"LLM interpretation successful (post-processed). Task: {llm_output.get('primary_task')}, Status: {llm_output.get('status')}, Sources: {llm_output.get('required_sources')}")
#         llm_output['original_context'] = {'category': category, 'country': country, 'query': user_query}
#         return { "statusCode": 200, "body": json.dumps(llm_output) }
#
#     # --- Error Handling ---
#     except (json.JSONDecodeError, ValueError, TypeError) as e:
#          logger.error(f"Failed parsing/validating LLM response: {e}", exc_info=True)
#          logger.error(f"LLM Raw Text was: {response.text}")
#          return { "statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Failed processing LLM response: {e}", "llm_raw_output": response.text}) }
#     except Exception as e:
#         logger.exception("Unhandled error during interpretation.")
#         return { "statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Internal server error: {str(e)}"}) }



import logging
import os
import json
import csv
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
import re # Keep existing imports
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

# --- Configuration ---
LAMBDA_ROOT = Path(__file__).resolve().parent
CONFIG_DIR = LAMBDA_ROOT / "config_data"
CATEGORIES_CSV = CONFIG_DIR / "categories.csv"
STYLES_CSV = CONFIG_DIR / "styles.csv"
COLORS_CSV = CONFIG_DIR / "colors.csv"

SECRET_NAME = os.environ.get("SECRET_NAME", "YourGeminiSecretName")
LLM_MODEL_NAME = os.environ.get("INTERPRET_LLM_MODEL", "gemini-2.5-flash-preview-04-17") # Reverted to this as per your code
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")

# --- Constants for Special Tasks ---
BRAND_ANALYSIS_CATEGORY = "BRAND_ANALYSIS"
INTERNAL_BRAND_PERFORMANCE_SOURCE = "internal_brand_performance"
ANALYZE_BRAND_TASK = "analyze_brand_deep_dive"

AMAZON_RADAR_CATEGORY = "AMAZON_RADAR"
INTERNAL_AMAZON_RADAR_SOURCE = "internal_amazon_radar"
SUMMARIZE_AMAZON_TASK = "summarize_amazon_radar"

# --- Logger Setup ---
logger = logging.getLogger()
log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
if log_level_str not in valid_log_levels: log_level_str = "INFO"
logger.setLevel(log_level_str)
logger.info(f"Logger initialized with level: {log_level_str}")
logger.info(f"Using Interpreter LLM: {LLM_MODEL_NAME}")

# --- Globals & Config Loading ---
KNOWN_CATEGORIES: Set[str] = set()
KNOWN_STYLES: Set[str] = set()
KNOWN_COLORS: Set[str] = set()
CONFIG_LOAD_ERROR: Optional[str] = None

VALID_DEPARTMENTS = ["Men", "Women", "Kids", "Fashion", "Beauty"]
CATEGORIES_BY_DEPARTMENT = {
    "men": {'suits', 'formal shoes', 'cufflinks', 'neckties', 'golf apparel', 'belts', 'wallets', 't-shirts', 'jeans', 'jackets', 'hoodies', 'sneakers', 'socks', 'underwear', 'sweaters', 'shorts', 'pajamas', 'swimwear', 'hats', 'gloves', 'scarves', 'slippers', 'backpacks', 'raincoats', 'sunglasses', 'sportswear', 'sandals', 'boots', 'bags', 'watches', 'necklaces', 'bracelets', 'earrings', 'rings', 'shirts', 'pants', 'blouses', 'beachwear'},
    "women": {'dresses', 'skirts', 'heels', 'jewelry sets', 'handbags', 'bras', 'leggings', 'top tanks', 'jumpsuits', 'bikinis', 't-shirts', 'jeans', 'jackets', 'hoodies', 'sneakers', 'socks', 'underwear', 'sweaters', 'shorts', 'pajamas', 'swimwear', 'hats', 'gloves', 'scarves', 'slippers', 'backpacks', 'raincoats', 'sunglasses', 'sportswear', 'sandals', 'boots', 'bags', 'watches', 'necklaces', 'bracelets', 'earrings', 'rings', 'shirts', 'pants', 'blouses', 'beachwear'},
    "kids": {'t-shirts', 'jeans', 'jackets', 'hoodies', 'sneakers', 'socks', 'underwear', 'shorts', 'pajamas', 'swimwear', 'hats', 'gloves', 'scarves', 'slippers', 'backpacks', 'raincoats', 'sunglasses', 'sportswear', 'sandals', 'boots', 'bags', 'watches', 'necklaces', 'bracelets', 'earrings', 'rings', 'shirts', 'pants', 'blouses', 'beachwear', 'sweaters'},
    "fashion": {'jackets', 'socks', 'pajamas', 'swimwear', 'gloves', 'backpacks', 'sunglasses', 'sportswear', 'boots', 'bags', 'shirts', 'pants', 'blouses', 'beachwear', 't-shirts', 'jeans', 'hoodies', 'sneakers', 'underwear', 'shorts', 'hats', 'slippers', 'raincoats', 'rings', 'scarves', 'sandals', 'bracelets', 'sweaters', 'earrings', 'watches', 'necklaces'},
    "beauty": {'makeup', 'skincare', 'haircare', 'perfumes', 'nail polish', 'lipstick', 'mascara'}
}

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
            logger.info(f"Loaded {count} standard categories.")
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

load_config_csvs()

secrets_manager = None
BOTO3_CLIENT_ERROR = None
try:
    session = boto3.session.Session()
    secrets_manager = session.client(service_name='secretsmanager', region_name=AWS_REGION)
except Exception as e:
    logger.exception("CRITICAL ERROR initializing Boto3 client!"); BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 client: {e}"

API_KEY_CACHE: Dict[str, Optional[str]] = {}
def get_secret_value(secret_name: str, key_name: str) -> Optional[str]:
    is_local = os.environ.get("IS_LOCAL", "false").lower() == "true"
    if is_local:
        direct_key = os.environ.get(key_name)
        if direct_key: logger.info(f"Using direct env var '{key_name}' (local mode)"); return direct_key
        else: logger.warning(f"Direct env var '{key_name}' not found. Trying Secrets Manager...")
    global API_KEY_CACHE; cache_key = f"{secret_name}:{key_name}"
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
        elif 'SecretBinary' in response:
             try: secret_dict = json.loads(response['SecretBinary'].decode('utf-8'))
             except (json.JSONDecodeError, UnicodeDecodeError) as e: logger.error(f"Failed binary decode: {e}"); return None
        else: logger.error("Secret value not found."); return None
        if not isinstance(secret_dict, dict): logger.error("Parsed secret not dict."); return None
        key_value = secret_dict.get(key_name)
        if not key_value or not isinstance(key_value, str):
            logger.error(f"Key '{key_name}' not found or not string in secret '{secret_name}'."); API_KEY_CACHE[cache_key] = None; return None
        API_KEY_CACHE[cache_key] = key_value; logger.info(f"Key '{key_name}' successfully retrieved and cached."); return key_value
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code"); logger.error(f"AWS ClientError for '{secret_name}': {error_code}"); API_KEY_CACHE[cache_key] = None; return None
    except Exception as e:
        logger.exception(f"Unexpected error retrieving secret '{secret_name}'."); API_KEY_CACHE[cache_key] = None; return None

def extract_brand_from_query(query: str) -> Optional[str]:
    if not query or not isinstance(query, str): return None
    query_lower = query.lower()
    domain_match = re.search(r'([\w-]+\.[a-z]{2,}(\.[a-z]{2})?)', query_lower) # Improved to catch .co.uk etc.
    if domain_match:
        cleaned = clean_domain_for_lookup(domain_match.group(1))
        if cleaned: logger.info(f"Extracted domain: {cleaned}"); return cleaned
    brand_keywords = ["analyze brand", "brand analysis of", "competitors for", "tell me about brand", "brand profile for", "brand overview for", "brand insights for", "brand", "analyze"]
    # Try to find the brand name after specific keywords
    for keyword in brand_keywords:
        if keyword in query_lower:
            # Extract text after keyword, clean it up
            potential_brand_segment = query_lower.split(keyword, 1)[-1].strip()
            # Remove common leading/trailing words that are not part of brand
            potential_brand_segment = re.sub(r'^(the\s+)?', '', potential_brand_segment, flags=re.IGNORECASE)
            potential_brand_segment = re.sub(r'\s+in\s+.*$', '', potential_brand_segment, flags=re.IGNORECASE) # Remove " in country"
            potential_brand_segment = re.sub(r'\s+performance$', '', potential_brand_segment, flags=re.IGNORECASE)
            potential_brand_segment = re.sub(r"^[^\w(@.)]+|[^\w(@.)]+$", "", potential_brand_segment) # More permissive punctuation removal
            # Take first few words, assuming brand names are not extremely long
            target_brand_words = potential_brand_segment.split()
            if target_brand_words:
                # Try to reconstruct a plausible brand name (e.g., up to 3 words)
                for i in range(min(3, len(target_brand_words)), 0, -1):
                    brand_candidate = " ".join(target_brand_words[:i])
                    if len(brand_candidate) > 1: # Simple check for some substance
                         logger.info(f"Extracted potential brand keyword: {brand_candidate} from segment '{potential_brand_segment}'")
                         return brand_candidate.strip()
    logger.warning(f"Could not extract brand/domain from query: {query}"); return None

def clean_domain_for_lookup(input_domain: str) -> str:
    if not isinstance(input_domain, str): return ""
    cleaned = re.sub(r'^https?:\/\/', '', input_domain.strip(), flags=re.IGNORECASE)
    cleaned = re.sub(r'^www\.', '', cleaned, flags=re.IGNORECASE)
    if cleaned.endswith('/'): cleaned = cleaned[:-1]
    return cleaned.lower()

def extract_amazon_params(query: str) -> Dict[str, Optional[str]]:
    params = {"department": None, "target_category": None}
    if not query or not isinstance(query, str): return params
    query_lower = query.lower(); original_query = query
    found_dept_orig_case = None
    dept_keywords = [" department", " for men", " for women", " for kids", " fashion", " beauty"]
    dept_map = {"men": "Men", "women": "Women", "kids": "Kids", "fashion": "Fashion", "beauty": "Beauty"}
    for keyword in dept_keywords:
        if keyword in query_lower:
             dept_key = keyword.split(" for ")[-1].strip() if " for " in keyword else keyword.strip()
             if dept_key in dept_map:
                  found_dept_orig_case = dept_map[dept_key]; params["department"] = found_dept_orig_case
                  logger.info(f"Found department '{found_dept_orig_case}' using keyword '{keyword}'"); break
    if not found_dept_orig_case:
        for dept_val in VALID_DEPARTMENTS:
             if re.search(r'\b' + re.escape(dept_val.lower()) + r'\b', query_lower):
                  params["department"] = dept_val; found_dept_orig_case = dept_val
                  logger.info(f"Found department '{found_dept_orig_case}' via direct mention."); break
    if not found_dept_orig_case: logger.warning("Could not determine department for Amazon Radar."); return params
    dept_key_lower = found_dept_orig_case.lower()
    if dept_key_lower in CATEGORIES_BY_DEPARTMENT:
        possible_categories = CATEGORIES_BY_DEPARTMENT[dept_key_lower]
        for cat_lower in possible_categories:
            pattern = re.compile(r'\b' + re.escape(cat_lower.replace('-', r'\-')) + r'\b', re.IGNORECASE) # Escape hyphens
            match = pattern.search(original_query)
            if match:
                # Find original casing from a master list or use title case
                original_case_category = next((c for c_list in CATEGORIES_BY_DEPARTMENT.values() for c in c_list if c.lower() == cat_lower), cat_lower.title())
                # Find the title cased version from the original VALID_DEPARTMENTS lists
                # This is a bit convoluted; ideally, have a direct mapping for original casing if needed elsewhere
                title_cased_cat = cat_lower.title() # Fallback
                for dept_cat_list in CATEGORIES_BY_DEPARTMENT.values():
                    for known_cat_in_list in dept_cat_list: # These are already lowercase
                        if known_cat_in_list == cat_lower: # Found the lowercase
                            # Find the original cased version (requires iterating main Known lists or having original case map)
                            # For simplicity, let's assume title case from the lowercase is acceptable.
                             title_cased_cat = cat_lower.title()
                             break
                    if title_cased_cat != cat_lower.title(): break


                params["target_category"] = title_cased_cat
                logger.info(f"Found category '{params['target_category']}' for department '{found_dept_orig_case}'."); break
    if not params["target_category"]: logger.warning(f"Could not determine category for Amazon Radar (Dept: {found_dept_orig_case}).")
    return params

# --- Main Lambda Handler ---
def lambda_handler(event, context):
    if CONFIG_LOAD_ERROR: return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Config Error: {CONFIG_LOAD_ERROR}"})}
    if BOTO3_CLIENT_ERROR: return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": BOTO3_CLIENT_ERROR})}
    if not GEMINI_SDK_AVAILABLE: return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "Gemini SDK unavailable."})}

    logger.info(f"Received event: {json.dumps(event)}")
    try:
        if isinstance(event.get('body'), str): body = json.loads(event['body']); logger.debug("Parsed body from API GW event.")
        elif isinstance(event, dict) and 'query' in event and 'category' in event and 'country' in event: body = event; logger.debug("Using direct event payload.")
        else: raise ValueError("Invalid input structure")
        user_query = body.get('query'); category = body.get('category'); country = body.get('country')
        if not user_query or category is None or not country: raise ValueError("Missing required fields")
        logger.info(f"Input - Query: '{user_query}', Cat: '{category}', Country: '{country}'")
    except (json.JSONDecodeError, ValueError, TypeError) as e:
        logger.error(f"Request parsing error: {e}"); return {"statusCode": 400, "body": json.dumps({"status": "error", "error_message": f"Invalid input: {e}"})}

    category_upper = category.upper() if isinstance(category, str) else ""
    original_context_payload = {'category': category, 'country': country, 'query': user_query} # Base for all paths

    # 1. Handle BRAND_ANALYSIS
    if category_upper == BRAND_ANALYSIS_CATEGORY:
        logger.info(f"Detected '{BRAND_ANALYSIS_CATEGORY}' category...")
        extracted_brand = extract_brand_from_query(user_query)
        original_context_payload['target_brand'] = extracted_brand # Add to context
        if extracted_brand:
            output_payload = {"status": "success", "primary_task": ANALYZE_BRAND_TASK, "required_sources": [INTERNAL_BRAND_PERFORMANCE_SOURCE, "web_search"], "query_subjects": {"specific_known": [], "unmapped_items": [], "target_brand": extracted_brand}, "timeframe_reference": None, "attributes": [], "clarification_needed": None, "original_context": original_context_payload}
            logger.info("Bypassing LLM. Returning direct payload for Brand Analysis.")
            return {"statusCode": 200, "body": json.dumps(output_payload)}
        else:
            output_payload = {"status": "needs_clarification", "primary_task": "unknown", "required_sources": ["clarify"], "query_subjects": {"specific_known": [], "unmapped_items": []}, "timeframe_reference": None, "attributes": [], "clarification_needed": "Please specify the brand name or website for analysis.", "original_context": original_context_payload}
            return {"statusCode": 200, "body": json.dumps(output_payload)}

    # 2. Handle AMAZON_RADAR
    elif category_upper == AMAZON_RADAR_CATEGORY:
        logger.info(f"Detected '{AMAZON_RADAR_CATEGORY}' category...")
        amazon_params = extract_amazon_params(user_query)
        target_dept = amazon_params.get("department"); target_cat = amazon_params.get("target_category")
        original_context_payload['target_department'] = target_dept; original_context_payload['target_category'] = target_cat
        clarification_msg = None
        if not target_dept: clarification_msg = "Which department for Amazon Radar (e.g., Men, Women, Kids, Fashion, Beauty)?"
        elif not target_cat: clarification_msg = f"Which category within '{target_dept}' for Amazon Radar?"
        else:
            dept_key_lower = target_dept.lower()
            if dept_key_lower not in CATEGORIES_BY_DEPARTMENT or target_cat.lower() not in CATEGORIES_BY_DEPARTMENT[dept_key_lower]:
                 clarification_msg = f"Category '{target_cat}' is not valid for '{target_dept}' on Amazon Radar. Please specify a valid category for {target_dept}."
        if clarification_msg:
             output_payload = {"status": "needs_clarification", "primary_task": "unknown", "required_sources": ["clarify"], "query_subjects": {"specific_known": [], "unmapped_items": []}, "timeframe_reference": None, "attributes": [], "clarification_needed": clarification_msg, "original_context": original_context_payload}
             return {"statusCode": 200, "body": json.dumps(output_payload)}
        else:
             logger.info(f"Valid Amazon params: Dept='{target_dept}', Cat='{target_cat}'")
             output_payload = {"status": "success", "primary_task": SUMMARIZE_AMAZON_TASK, "required_sources": [INTERNAL_AMAZON_RADAR_SOURCE], "query_subjects": {"specific_known": [], "unmapped_items": []}, "timeframe_reference": None, "attributes": [], "clarification_needed": None, "original_context": original_context_payload}
             logger.info("Bypassing LLM. Returning direct payload for Amazon Radar.")
             return {"statusCode": 200, "body": json.dumps(output_payload)}

    # 3. Handle Standard Interpretation
    else:
        logger.info("Category is not a placeholder. Proceeding with standard LLM interpretation.")
        google_api_key = get_secret_value(SECRET_NAME, "GOOGLE_API_KEY")
        if not google_api_key: return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "API key config error (Google)."})}
        try:
             genai.configure(api_key=google_api_key); model = genai.GenerativeModel(LLM_MODEL_NAME)
        except Exception as configure_err:
             actual_error = str(configure_err); logger.error(f"Gemini SDK config error: {actual_error}", exc_info=True)
             if "model not found" in actual_error.lower() or "invalid api key" in actual_error.lower(): return {"statusCode": 400, "body": json.dumps({"status": "error", "error_message": f"LLM config error: {actual_error}"})}
             else: return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "LLM SDK configuration error."})}

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
            -   Mega Trends Rule: Use 'internal_mega' ONLY if the primary_task is 'summarize_mega_trends'. YOU MUST NOT include 'internal_mega' if any other internal source ('internal_trends_category', 'internal_trends_item', 'internal_forecast') is selected. Also (as checked in step 3), DO NOT use 'internal_mega' if step 3 identifies ANY subjects in `specific_known_subjects` OR `unmapped_items`.
            -   Clarification Rule: If the query is too ambiguous, invalid, lacks specifics needed for the task (e.g., forecast without an item), or falls outside the Category/Country context, use ONLY 'clarify'.
        3.  Extract key entities mentioned in the User Query. Apply these rules STRICTLY:
            -   First, identify all potential fashion subjects (styles, colors, items like 'bomber jacket') in the query.
            -   For EACH potential subject:
                a. Check for an exact case-insensitive match in the 'All Known Styles' or 'All Known Colors' lists.
                b. If a match IS found: Determine if it's a 'style' or 'color'. **If it's a 'color', add it directly** to the `specific_known_subjects` list as `{{"subject": "Matched Term Title Case", "type": "color"}}`. **If it's a 'style', THEN check if the matched style is appropriate** for the stated Category context (e.g., 'Dresses' style is inappropriate for 'Shirts' Category). If the style IS appropriate, add it to `specific_known_subjects` as `{{"subject": "Matched Term Title Case", "type": "style"}}`. If the style is NOT appropriate for the category, add the term (Title Case) to `unmapped_items`.
                c. If NO exact match is found in the known lists: Add the term (Title Case) to the `unmapped_items` list.
                d. DO NOT guess or find the 'closest' match. Only exact matches are processed for `specific_known_subjects`.
            -   `specific_known_subjects`: List of objects for matched subjects (colors are always added if matched, styles only if matched AND category-appropriate). Can be empty.
            -   `unmapped_items`: List of terms that were not exact matches, were category-inappropriate styles, or other potential fashion items. Can be empty. Use Title Case.
            -   `timeframe_reference`: Any mention of time (e.g., "next 6 months", "latest"). Return null if none found.
            -   `attributes`: Any other descriptors (e.g., "material:linen", "price:high"). Return [] if none found.
        4.  Determine the overall 'status'. It MUST be 'needs_clarification' ONLY if 'clarify' is in `required_sources` OR if step 3 added items to `unmapped_items` because they were category-inappropriate styles that prevent analysis. Otherwise (even if 'web_search' is required or there are other `unmapped_items`), it MUST be 'success'.
        5.  Provide a concise 'clarification_needed' message (string) ONLY if status is 'needs_clarification', otherwise it MUST be null. Explain *why* (e.g., "Style 'Dresses' is not applicable to the 'Shirts' category. Please specify a relevant style or remove it.", or "Query is too ambiguous...").

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
        logger.info(f"Calling LLM: {LLM_MODEL_NAME} for standard interpretation...")
        try:
            generation_config = genai.types.GenerationConfig(response_mime_type="application/json")
            response = model.generate_content(prompt, generation_config=generation_config)
            logger.info("LLM response received."); logger.debug(f"LLM Raw Response Text:\n{response.text}")
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
            query_subjects_llm = llm_output.get("query_subjects") # Renamed for clarity
            if not isinstance(query_subjects_llm, dict): raise ValueError("LLM 'query_subjects' not dict.")
            if "specific_known" not in query_subjects_llm or "unmapped_items" not in query_subjects_llm: raise ValueError("LLM 'query_subjects' missing keys.")
            if not isinstance(query_subjects_llm["unmapped_items"], list): raise ValueError("LLM 'unmapped_items' not list.")
            specific_known_llm = query_subjects_llm["specific_known"] # Renamed for clarity
            if not isinstance(specific_known_llm, list): raise ValueError("LLM 'specific_known' not list.")
            for item in specific_known_llm:
                if not isinstance(item, dict): raise ValueError(f"Item in 'specific_known' not dict: {item}")
                if "subject" not in item or "type" not in item: raise ValueError(f"Item missing 'subject'/'type': {item}")
                if item["type"] not in ["color", "style"]: raise ValueError(f"Invalid type '{item['type']}': {item}")
                if not isinstance(item.get("subject"), str): raise ValueError(f"Subject not string: {item}")

            # --- Fallback logic if LLM returns None/unknown for primary_task/status for standard queries ---
            current_task = llm_output.get("primary_task")
            current_status = llm_output.get("status")
            current_sources = llm_output.get("required_sources", [])

            if (current_task is None or current_task == "unknown") and \
               (current_status is None or current_status not in ["success", "needs_clarification"]) and \
               not query_subjects_llm.get("specific_known") and not query_subjects_llm.get("unmapped_items"):
                logger.warning("LLM failed to classify a general category query. Applying fallback.")
                llm_output["primary_task"] = "summarize_category"
                llm_output["required_sources"] = ["internal_trends_category"]
                llm_output["status"] = "success"
                llm_output["clarification_needed"] = None
                # Ensure other expected keys are present even if LLM missed them
                if "query_subjects" not in llm_output: llm_output["query_subjects"] = {"specific_known": [], "unmapped_items": []}
                if "timeframe_reference" not in llm_output: llm_output["timeframe_reference"] = None
                if "attributes" not in llm_output: llm_output["attributes"] = []
            # --- End Fallback ---


            # --- Post-LLM Source Rule Enforcement ---
            primary_task_llm = llm_output.get("primary_task")
            required_sources_set = set(llm_output.get("required_sources", [])) # Use the potentially modified sources
            unmapped_items_llm = llm_output.get("query_subjects", {}).get("unmapped_items", [])
            specific_known_llm = llm_output.get("query_subjects", {}).get("specific_known", [])


            if primary_task_llm == "summarize_mega_trends":
                if "internal_mega" not in required_sources_set:
                    logger.warning("Post-LLM: Adding 'internal_mega' for summarize_mega_trends.")
                    required_sources_set.add("internal_mega")
                # Ensure category and web for mega trends (if this is where it should be decided)
                # Based on previous, we decided mega trends also needs category context and web
                # required_sources_set.add("internal_trends_category")
                # required_sources_set.add("web_search")

            if primary_task_llm in ["get_trend", "qa_combined", "qa_internal_only", "qa_web_only"] and \
               not specific_known_llm and unmapped_items_llm and \
               "internal_trends_category" not in required_sources_set:
                logger.warning("Post-LLM: Adding 'internal_trends_category' for context.")
                required_sources_set.add("internal_trends_category")

            if specific_known_llm or unmapped_items_llm:
                if "internal_mega" in required_sources_set and primary_task_llm != "summarize_mega_trends": # Don't remove if it IS mega_trends
                    logger.warning("Post-LLM: Removing 'internal_mega' (non-mega task with subjects).")
                    required_sources_set.discard("internal_mega")
                    if not required_sources_set: required_sources_set.add("internal_trends_category"); logger.warning("Post-LLM: Added fallback 'internal_trends_category'.")

            has_mega = "internal_mega" in required_sources_set
            has_other_internal = any(s in required_sources_set for s in ["internal_trends_category", "internal_trends_item", "internal_forecast"])

            if primary_task_llm != "summarize_mega_trends" and has_mega and has_other_internal : # Only apply if not mega trends task
                 logger.warning("Post-LLM: Enforcing mega exclusivity for non-mega task.")
                 required_sources_set.discard("internal_mega")
                 logger.warning("Post-LLM: Removed 'internal_mega'.")
            elif primary_task_llm == "summarize_mega_trends" and has_other_internal: # If mega_trends, remove other internal
                 logger.warning("Post-LLM: Mega task, removing other conflicting internal sources.")
                 required_sources_set.discard("internal_trends_category")
                 required_sources_set.discard("internal_trends_item")
                 required_sources_set.discard("internal_forecast")
                 # Ensure web_search is present for mega trends
                 if "web_search" not in required_sources_set:
                      logger.warning("Post-LLM: Adding 'web_search' for summarize_mega_trends as it was missing.")
                      required_sources_set.add("web_search")


            llm_output["required_sources"] = sorted(list(required_sources_set))

            if "clarify" in llm_output.get("required_sources", []) and llm_output.get("status") != "needs_clarification":
                logger.warning("Post-LLM: Forcing 'needs_clarification' due to 'clarify' source.")
                llm_output["status"] = "needs_clarification"
            if llm_output.get("status") == "needs_clarification" and not llm_output.get("clarification_needed"):
                 logger.warning("Post-LLM: Adding generic clarification message.")
                 llm_output["clarification_needed"] = "Query requires clarification. Please be more specific."

            logger.info(f"LLM interpretation successful (post-processed). Task: {llm_output.get('primary_task')}, Status: {llm_output.get('status')}, Sources: {llm_output.get('required_sources')}")
            llm_output['original_context'] = original_context_payload # Use the one defined at the start
            return { "statusCode": 200, "body": json.dumps(llm_output) }

        except (json.JSONDecodeError, ValueError, TypeError) as e:
             logger.error(f"Failed parsing/validating LLM response: {e}", exc_info=True); logger.error(f"LLM Raw Text was: {response.text}")
             return { "statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Failed processing LLM response: {e}", "llm_raw_output": response.text}) }
        except Exception as e:
            logger.exception("Unhandled error during interpretation.")
            return { "statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Internal server error: {str(e)}"}) }