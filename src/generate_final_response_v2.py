# # src/generate_final_response_v2.py
#
# import json
# import logging
# import os
# from typing import Dict, Optional, List, Any
#
# import boto3
# from botocore.exceptions import ClientError
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
# SECRET_NAME = os.environ.get("SECRET_NAME", "YourSecretsName") # Secret containing Google API key
# SYNTHESIS_LLM_MODEL = os.environ.get("SYNTHESIS_LLM_MODEL", "gemini-2.0-flash") # Use Gemini Pro for synthesis
# AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
#
# # Define constants for result type indicators
# INDICATOR_TREND_DETAIL = "TREND_DETAIL"
# INDICATOR_MEGA_TREND = "MEGA_TREND"
# INDICATOR_CATEGORY_OVERVIEW = "CATEGORY_OVERVIEW"
# INDICATOR_FORECAST = "FORECAST"
# INDICATOR_COMPARISON = "COMPARISON"
# INDICATOR_RECOMMENDATION = "RECOMMENDATION"
# INDICATOR_QA_WEB = "QA_WEB"
# INDICATOR_QA_INTERNAL = "QA_INTERNAL"
# INDICATOR_QA_COMBINED = "QA_COMBINED"
# INDICATOR_UNKNOWN = "UNKNOWN"
# INDICATOR_ERROR = "ERROR"
#
# # --- Initialize Logger ---
# logger = logging.getLogger()
# log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
# valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
# if log_level_str not in valid_log_levels: log_level_str = "INFO"
# logger.setLevel(log_level_str)
# logger.info(f"Logger initialized with level: {log_level_str}")
# logger.info(f"SYNTHESIS_LLM_MODEL: {SYNTHESIS_LLM_MODEL}")
# logger.info(f"SECRET_NAME: {SECRET_NAME}")
#
# # --- Initialize Boto3 Client for Secrets Manager ---
# secrets_manager = None
# BOTO3_CLIENT_ERROR = None
# try:
#     session = boto3.session.Session()
#     secrets_manager = session.client(service_name='secretsmanager', region_name=AWS_REGION)
# except Exception as e:
#     logger.exception("CRITICAL ERROR initializing Boto3 Secrets Manager client!")
#     BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 client: {e}"
#
# # --- API Key Caching ---
# API_KEY_CACHE: Dict[str, Optional[str]] = {}
#
# # --- Helper Function to Get Google API Key ---
# # (Identical structure to helpers in other lambdas)
# def get_secret_value(secret_name: str, key_name: str) -> Optional[str]:
#     """Retrieves Google API key from Secrets Manager or ENV."""
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
# # --- Placeholder Persona Prompts ---
# # TODO: Define actual persona prompts based on desired tone and output format
# # These should guide the LLM on how to structure its response (e.g., sections, markdown)
# PERSONA_PROMPTS = {
#     INDICATOR_TREND_DETAIL: """
# You are a TrendForecast.io Expert Analyst providing insights on specific fashion items.
# Analyze the provided internal data (category summary, style/color details, specific item details) and external web context (summarized answer, top links).
# Focus on the specific item requested ({specific_item_name}).
# Respond with a concise, actionable analysis covering current trends, potential drivers, and outlook based ONLY on the provided data.
# Structure your response clearly. Use markdown for formatting (e.g., headings, bullet points).
# If forecast data is present, incorporate it into the outlook.
# If web context is available, integrate relevant insights mentioning key sources briefly.
# Keep the tone professional and data-driven.
# DO NOT invent data or trends not present in the input.
# """,
#     INDICATOR_MEGA_TREND: """
# You are a TrendForecast.io Senior Strategist summarizing high-level mega trends.
# Analyze the provided list of trending queries/topics and any relevant web context.
# Identify the top 3-5 overarching mega trends emerging from the data.
# For each mega trend, provide a brief description and mention 1-2 example queries supporting it.
# Structure using markdown headings for each trend.
# Keep the tone insightful and forward-looking.
# Focus ONLY on the provided data.
# """,
#     INDICATOR_CATEGORY_OVERVIEW: """
# You are a TrendForecast.io Market Analyst providing a high-level overview of a fashion category.
# Analyze the provided category summary data, top style/color details, and web context.
# Summarize the current state of the '{category_name}' category in '{country_name}'.
# Highlight key performance indicators (overall growth, volume), mention the top 2-3 performing styles and colors based on the data.
# Briefly incorporate any relevant insights from the web context.
# Use clear, concise language and markdown formatting.
# Focus ONLY on the provided data.
# """,
#     INDICATOR_FORECAST: """
# You are a TrendForecast.io Forecast Specialist providing predictions for a specific item.
# Analyze the provided specific item details, including historical chart data and explicit forecast data (f2, f3, f6, avg2, avg3, avg6). Also consider relevant web context.
# Focus on the specific item requested ({specific_item_name}).
# Provide a forecast summary covering the expected growth trajectory over the next 2, 3, and 6 months based ONLY on the provided forecast numbers.
# Briefly mention potential factors influencing this forecast based on web context or historical trends if available.
# Structure clearly using markdown. Keep the tone objective and data-centric.
# DO NOT make predictions beyond the provided forecast data.
# """,
#     # Add placeholders for other primary_task types:
#     INDICATOR_COMPARISON: """
# You are a TrendForecast.io Comparative Analyst... [TODO: Define prompt]
# """,
#     INDICATOR_RECOMMENDATION: """
# You are a TrendForecast.io Recommendation Engine... [TODO: Define prompt]
# """,
#     INDICATOR_QA_WEB: """
# You are a helpful AI assistant answering a question based primarily on web search results.
# Synthesize the provided web answer and relevant snippets from the top web links to directly answer the original user query: '{user_query}'.
# Cite sources implicitly (e.g., "According to recent reports..." or "Web search suggests...").
# Keep the answer concise and focused on the query. Use markdown.
# """,
#      INDICATOR_QA_INTERNAL: """
# You are a helpful AI assistant answering a question based primarily on internal trend data.
# Synthesize the provided internal data summaries (category, item, mega trends) to directly answer the original user query: '{user_query}'.
# Refer to data points explicitly but concisely (e.g., "The category shows X% growth...", "Style Y has an average volume of Z...").
# Keep the answer concise and focused on the query. Use markdown.
# """,
#      INDICATOR_QA_COMBINED: """
# You are a helpful AI assistant answering a question using both internal data and web search results.
# Synthesize the provided internal data summaries AND the web answer/snippets to provide a comprehensive answer to the original user query: '{user_query}'.
# Integrate insights from both sources where relevant. Cite sources implicitly.
# Keep the answer concise and focused on the query. Use markdown.
# """,
#     INDICATOR_UNKNOWN: """
# You are a helpful AI assistant. The user's request could not be fully categorized.
# Briefly explain that you received the query '{user_query}' for {category_name} in {country_name} but require more specific direction, or indicate you will provide a general summary based on available data.
# [TODO: Decide default behavior - ask for clarification or provide general summary?]
# """,
#     INDICATOR_ERROR: "Error processing request.", # Fallback
# }
#
#
# # --- Helper Function to Map Task to Indicator and Select Prompt ---
# def get_task_details(primary_task: str | None) -> tuple[str, str]:
#     """Maps primary_task to indicator string and retrieves the corresponding persona prompt."""
#     indicator = INDICATOR_UNKNOWN # Default
#     if primary_task == "get_trend":
#         # Need context to know if it's item-level or category-level. Default to detail for now.
#         indicator = INDICATOR_TREND_DETAIL # May need refinement based on input data presence
#     elif primary_task == "get_forecast":
#         indicator = INDICATOR_FORECAST
#     elif primary_task == "summarize_mega_trends":
#         indicator = INDICATOR_MEGA_TREND
#     elif primary_task == "summarize_category":
#          indicator = INDICATOR_CATEGORY_OVERVIEW
#     elif primary_task == "compare_items":
#          indicator = INDICATOR_COMPARISON
#     elif primary_task == "get_recommendation":
#          indicator = INDICATOR_RECOMMENDATION
#     elif primary_task == "qa_web_only":
#          indicator = INDICATOR_QA_WEB
#     elif primary_task == "qa_internal_only":
#          indicator = INDICATOR_QA_INTERNAL
#     elif primary_task == "qa_combined":
#          indicator = INDICATOR_QA_COMBINED
#     # Add mappings for other tasks if defined...
#
#     prompt_template = PERSONA_PROMPTS.get(indicator, PERSONA_PROMPTS[INDICATOR_UNKNOWN])
#     logger.info(f"Mapped primary_task '{primary_task}' to indicator '{indicator}'.")
#     return indicator, prompt_template
#
#
# # --- Helper Function to Format Data for LLM Prompt ---
# def format_data_for_prompt(internal_data: Dict, external_data: Dict) -> str:
#     """Formats the available data into a string for the LLM prompt."""
#     prompt_parts = []
#     interpretation = internal_data.get("interpretation", {})
#     original_context = interpretation.get("original_context", {})
#     query_subjects = interpretation.get("query_subjects", {})
#     specific_known = query_subjects.get("specific_known", [])
#
#     prompt_parts.append("CONTEXT:")
#     prompt_parts.append(f"- User Query: {original_context.get('query', 'N/A')}")
#     prompt_parts.append(f"- Category: {original_context.get('category', 'N/A')}")
#     prompt_parts.append(f"- Country: {original_context.get('country', 'N/A')}")
#     if specific_known:
#         specific_item_name = specific_known[0].get("subject") # Assuming first item focus
#         prompt_parts.append(f"- Specific Focus Item: {specific_item_name}")
#
#     prompt_parts.append("\nAVAILABLE DATA:")
#
#     # Internal Data Summaries
#     trends = internal_data.get("trends_data")
#     if trends:
#         prompt_parts.append("\nInternal Category/Style/Color Trends:")
#         if trends.get("category_summary"):
#             cs = trends["category_summary"]
#             prompt_parts.append(f"- Overall Category ({cs.get('category_name', 'N/A')}): Avg Vol={cs.get('average_volume', 'N/A')}, Growth={cs.get('growth_recent', 'N/A'):.1f}%")
#         # Summarize Top N styles/colors briefly
#         top_styles = sorted(trends.get("style_details", []), key=lambda x: x.get('average_volume', 0), reverse=True)[:3] # Top 3 for prompt
#         top_colors = sorted(trends.get("color_details", []), key=lambda x: x.get('average_volume', 0), reverse=True)[:3] # Top 3 for prompt
#         if top_styles: prompt_parts.append(f"- Top Styles: {', '.join([s.get('style_name', 'N/A') for s in top_styles])}")
#         if top_colors: prompt_parts.append(f"- Top Colors: {', '.join([c.get('color_name', 'N/A') for c in top_colors])}")
#
#     details = internal_data.get("chart_details_data")
#     if details:
#          prompt_parts.append(f"\nInternal Specific Item Details ({details.get('category_subject', 'N/A')}):")
#          prompt_parts.append(f"- Avg Vol={details.get('average_volume', 'N/A')}, Growth={details.get('growth_recent', 'N/A'):.1f}%")
#          # Include forecast if primary task matches
#          if interpretation.get("primary_task") == "get_forecast":
#              prompt_parts.append(f"- Forecast Growth: 2m={details.get('f2', 'N/A')}%, 3m={details.get('f3', 'N/A')}%, 6m={details.get('f6', 'N/A')}%")
#              # Add avg forecast if needed: Avg Forecast Vol: 2m={details.get('avg2', 'N/A')}, ...
#
#     mega = internal_data.get("mega_trends_data")
#     if mega:
#         prompt_parts.append("\nInternal Mega Trends (Top Queries):")
#         # Summarize Top N mega trends
#         top_mega = sorted(mega, key=lambda x: x.get('growth_recent', 0), reverse=True)[:3] # Top 3 for prompt
#         for m in top_mega:
#             prompt_parts.append(f"- Query: '{m.get('query_name', 'N/A')}', Category: {m.get('category_name', 'N/A')}, Growth: {m.get('growth_recent', 'N/A'):.1f}%")
#
#     # External Data Summaries
#     ext_answer = external_data.get("answer")
#     ext_results = external_data.get("results", [])
#     if ext_answer:
#         prompt_parts.append("\nExternal Web Context (Synthesized Answer):")
#         prompt_parts.append(f"- {ext_answer}")
#     elif ext_results:
#         prompt_parts.append("\nExternal Web Context (Top Results):")
#         # Extract snippets from top N results
#         for i, res in enumerate(ext_results[:3]): # Top 3 snippets for prompt
#             title = res.get('title', 'N/A')
#             content_snippet = res.get('content', '')[:150] # First 150 chars
#             prompt_parts.append(f"- [{i+1}] {title}: {content_snippet}...")
#
#     return "\n".join(prompt_parts)
#
#
# # --- Helper Function to Extract Supporting Data for Bubble ---
# def extract_supporting_data(internal_data: Dict, external_data: Dict, task_indicator: str) -> Dict:
#     """Extracts raw chart data and summaries needed by Bubble based on task."""
#     supporting_data = {}
#     trends_data = internal_data.get("trends_data")
#     chart_details_data = internal_data.get("chart_details_data")
#     mega_trends_data = internal_data.get("mega_trends_data")
#     external_results = external_data.get("results", [])
#
#     # 1. Category Summary & Chart (from trends_data)
#     if trends_data and trends_data.get("category_summary"):
#         cs = trends_data["category_summary"]
#         supporting_data["category_summary_metrics"] = {
#             "name": cs.get("category_name"),
#             "growth": cs.get("growth_recent"),
#             "volume": cs.get("average_volume")
#         }
#         supporting_data["category_chart"] = cs.get("chart_data")
#
#     # 2. Top N & All Styles/Colors (from trends_data)
#     if trends_data:
#         all_styles = trends_data.get("style_details", [])
#         all_colors = trends_data.get("color_details", [])
#         if all_styles:
#              top_styles = sorted(all_styles, key=lambda x: x.get('average_volume', 0), reverse=True)[:5] # Top 5
#              supporting_data["top_styles_summary"] = [{"name": s.get("style_name"), "growth": s.get("growth_recent"), "volume": s.get("average_volume")} for s in top_styles]
#              supporting_data["all_styles_details"] = all_styles # Full list for potential drill-down
#         if all_colors:
#             top_colors = sorted(all_colors, key=lambda x: x.get('average_volume', 0), reverse=True)[:5] # Top 5
#             supporting_data["top_colors_summary"] = [{"name": c.get("color_name"), "growth": c.get("growth_recent"), "volume": c.get("average_volume")} for c in top_colors]
#             supporting_data["all_colors_details"] = all_colors # Full list
#
#     # 3. Item Detail & Chart (from chart_details_data)
#     if chart_details_data:
#          item_metrics = {
#              "name": chart_details_data.get("category_subject"),
#              "growth": chart_details_data.get("growth_recent"),
#              "volume": chart_details_data.get("average_volume"),
#              "forecast_growth": None,
#              "forecast_volume": None
#          }
#          # Include forecast only if task matches
#          if task_indicator == INDICATOR_FORECAST:
#              item_metrics["forecast_growth"] = {"f2": chart_details_data.get("f2"), "f3": chart_details_data.get("f3"), "f6": chart_details_data.get("f6")}
#              item_metrics["forecast_volume"] = {"avg2": chart_details_data.get("avg2"), "avg3": chart_details_data.get("avg3"), "avg6": chart_details_data.get("avg6")}
#          supporting_data["item_detail_metrics"] = item_metrics
#          supporting_data["item_chart"] = chart_details_data.get("chart_data")
#
#     # 4. Mega Trends (from mega_trends_data)
#     if mega_trends_data:
#         top_mega = sorted(mega_trends_data, key=lambda x: x.get('growth_recent', 0), reverse=True)[:10] # Top 10
#         supporting_data["top_mega_summary"] = [{"name": m.get("query_name"), "growth": m.get("growth_recent"), "volume": m.get("average_volume"), "category": m.get("category_name")} for m in top_mega]
#         supporting_data["all_mega_details"] = mega_trends_data # Full list
#
#     # 5. Web Links (from external_data)
#     if external_results:
#          supporting_data["web_links"] = [{"title": r.get("title"), "url": r.get("url")} for r in external_results][:5] # Top 5 links
#
#     # Filter out null values before returning
#     return {k: v for k, v in supporting_data.items() if v is not None and v != []}
#
#
# # --- Main Lambda Handler ---
# def lambda_handler(event, context):
#     """
#     Generates the final AI response summary based on internal and external data,
#     and packages supporting data for the Bubble frontend.
#     """
#     logger.info(f"Received combined event: {json.dumps(event)}")
#
#     # --- Initial Checks ---
#     if not GEMINI_SDK_AVAILABLE: return {"statusCode": 500, "body": json.dumps({"ai_summary": "Error: LLM SDK unavailable.", "status": INDICATOR_ERROR})}
#     if BOTO3_CLIENT_ERROR: return {"statusCode": 500, "body": json.dumps({"ai_summary": f"Error: {BOTO3_CLIENT_ERROR}", "status": INDICATOR_ERROR})}
#
#     # --- 1. Parse Combined Input ---
#     # Assuming Step Function passes input as a single JSON object
#     # with keys like "internal_data" and "external_data"
#     internal_data = event.get("internal_data", {})
#     external_data = event.get("external_data", {})
#     interpretation = internal_data.get("interpretation", {})
#     original_context = interpretation.get("original_context", {})
#     primary_task = interpretation.get("primary_task")
#     user_query = original_context.get("query", "the user query") # Fallback text
#
#     # Check for upstream errors
#     upstream_errors = internal_data.get("errors", [])
#     if external_data.get("error"): upstream_errors.append({"source": "FetchExternalContext", "error": external_data["error"]})
#     overall_status = INDICATOR_ERROR if any(err['source'] != 'FetchExternalContext' for err in upstream_errors) else "success" # Default to success if only external failed potentially
#
#
#     # --- 2. Select Persona Prompt & Indicator ---
#     result_type_indicator, prompt_template = get_task_details(primary_task)
#     logger.info(f"Using result indicator: {result_type_indicator}")
#
#     # --- 3. Format Data for LLM Synthesis Prompt ---
#     # Handle potential focus item name injection into prompt template
#     specific_item_name = "N/A"
#     if interpretation.get("query_subjects", {}).get("specific_known"):
#         specific_item_name = interpretation["query_subjects"]["specific_known"][0].get("subject", "N/A")
#
#     try:
#         formatted_data_context = format_data_for_prompt(internal_data, external_data)
#         synthesis_prompt = prompt_template.format(
#             specific_item_name=specific_item_name,
#             category_name=original_context.get('category', 'N/A'),
#             country_name=original_context.get('country', 'N/A'),
#             user_query=user_query
#             # Add other format variables if needed by prompts
#         )
#         synthesis_prompt += "\n\n" + formatted_data_context
#         logger.debug(f"Constructed Synthesis Prompt:\n{synthesis_prompt}")
#
#     except Exception as e:
#         logger.error(f"Error formatting data for synthesis prompt: {e}", exc_info=True)
#         # Decide how to handle - return error or try fallback?
#         return {"statusCode": 500, "body": json.dumps({"ai_summary": "Error: Could not prepare data for AI synthesis.", "result_type_indicator": INDICATOR_ERROR, "status": INDICATOR_ERROR, "error_message": str(e)})}
#
#
#     # --- 4. Call Synthesis LLM (Gemini Pro) ---
#     google_api_key = get_secret_value(SECRET_NAME, "GOOGLE_API_KEY")
#     if not google_api_key:
#          # Error already logged by helper
#          return {"statusCode": 500, "body": json.dumps({"ai_summary": "Error: API key configuration error (Google).", "result_type_indicator": INDICATOR_ERROR, "status": INDICATOR_ERROR, "error_message": "API key config error"})}
#
#     ai_summary = "Error: AI synthesis failed." # Default error message
#     llm_error = None
#     try:
#         logger.info(f"Calling Synthesis LLM: {SYNTHESIS_LLM_MODEL}...")
#         genai.configure(api_key=google_api_key)
#         model = genai.GenerativeModel(SYNTHESIS_LLM_MODEL)
#         # Consider adding safety settings, json mode if desired output is structured JSON from LLM
#         response = model.generate_content(synthesis_prompt)
#         logger.info("Synthesis LLM response received.")
#         ai_summary = response.text # Extract text response
#         logger.debug(f"Synthesis LLM Raw Response Text:\n{ai_summary}")
#
#     except Exception as e:
#         logger.error(f"Synthesis LLM call failed: {e}", exc_info=True)
#         llm_error = f"Synthesis LLM call failed: {str(e)}"
#         # Continue to package supporting data, but flag error
#
#
#     # --- 5. Extract Supporting Data for Bubble ---
#     supporting_data = {}
#     try:
#          supporting_data = extract_supporting_data(internal_data, external_data, result_type_indicator)
#          logger.info("Successfully extracted supporting data for Bubble.")
#          logger.debug(f"Supporting Data: {json.dumps(supporting_data)}")
#     except Exception as e:
#          logger.error(f"Error extracting supporting data: {e}", exc_info=True)
#          # Decide if this error should prevent returning data or just log
#          if not llm_error: # Prioritize LLM error message if it exists
#              llm_error = f"Error packaging supporting data: {str(e)}"
#
#
#     # --- 6. Construct Final Output Payload ---
#     final_status = INDICATOR_ERROR if llm_error else "success" # Default to success if LLM call worked
#     user_error_message = None
#
#     # Refine status based on upstream errors
#     if final_status == "success" and upstream_errors:
#         # Check if only non-critical errors occurred (e.g., web fetch failed but internal data is good)
#         if any(err['source'] != 'FetchExternalContext' for err in upstream_errors):
#              final_status = "partial_data_success" # Mark partial if internal data had issues but synthesis worked
#              logger.warning("Upstream errors detected in internal data sources.")
#         else:
#             final_status = "partial_data_success" # Mark partial if only web fetch failed
#             logger.warning("Upstream errors detected in external data fetch.")
#
#
#     if final_status == INDICATOR_ERROR:
#         user_error_message = llm_error or "An unexpected error occurred during analysis."
#
#     output_payload = {
#       "ai_summary": ai_summary,
#       "result_type_indicator": result_type_indicator if final_status != INDICATOR_ERROR else INDICATOR_ERROR,
#       "supporting_data": supporting_data if final_status != INDICATOR_ERROR else {}, # Don't send supporting data on error? Or send partial?
#       "status": final_status,
#       "error_message": user_error_message
#     }
#
#     logger.info(f"Final payload status: {final_status}")
#     return {
#         "statusCode": 200, # Lambda executed successfully, even if analysis had errors passed in payload
#         "body": json.dumps(output_payload)
#     }

import json
import logging
import os
from typing import Dict, Optional, List, Any

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

SECRET_NAME = os.environ.get("SECRET_NAME", "YourSecretsName")
SYNTHESIS_LLM_MODEL = os.environ.get("SYNTHESIS_LLM_MODEL", "gemini-1.5-flash-latest")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")

INDICATOR_TREND_DETAIL = "TREND_DETAIL"
INDICATOR_MEGA_TREND = "MEGA_TREND"
INDICATOR_CATEGORY_OVERVIEW = "CATEGORY_OVERVIEW"
INDICATOR_FORECAST = "FORECAST"
INDICATOR_COMPARISON = "COMPARISON"
INDICATOR_RECOMMENDATION = "RECOMMENDATION"
INDICATOR_QA_WEB = "QA_WEB"
INDICATOR_QA_INTERNAL = "QA_INTERNAL"
INDICATOR_QA_COMBINED = "QA_COMBINED"
INDICATOR_UNKNOWN = "UNKNOWN"
INDICATOR_ERROR = "ERROR"
INDICATOR_CLARIFICATION = "CLARIFICATION_NEEDED"

logger = logging.getLogger()
log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
if log_level_str not in valid_log_levels: log_level_str = "INFO"
logger.setLevel(log_level_str)
logger.info(f"Logger initialized with level: {log_level_str}")
logger.info(f"SYNTHESIS_LLM_MODEL: {SYNTHESIS_LLM_MODEL}")
logger.info(f"SECRET_NAME: {SECRET_NAME}")

secrets_manager = None
BOTO3_CLIENT_ERROR = None
try:
    session = boto3.session.Session()
    secrets_manager = session.client(service_name='secretsmanager', region_name=AWS_REGION)
except Exception as e:
    logger.exception("CRITICAL ERROR initializing Boto3 Secrets Manager client!")
    BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 client: {e}"

API_KEY_CACHE: Dict[str, Optional[str]] = {}

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

PERSONA_PROMPTS = {
    INDICATOR_TREND_DETAIL: """You are a TrendForecast.io Expert Analyst providing insights on specific fashion items. Analyze the provided internal data (category summary, style/color details, specific item details) and external web context (summarized answer, top links). Focus on the specific item requested ({specific_item_name}). Respond with a concise, actionable analysis covering current trends, potential drivers, and outlook based ONLY on the provided data. Structure your response clearly. Use markdown for formatting (e.g., headings, bullet points). If forecast data is present, incorporate it into the outlook. If web context is available, integrate relevant insights mentioning key sources briefly. Keep the tone professional and data-driven. DO NOT invent data or trends not present in the input.""",
    INDICATOR_MEGA_TREND: """You are a TrendForecast.io Senior Strategist summarizing high-level mega trends. Analyze the provided list of trending queries/topics and any relevant web context. Identify the top 3-5 overarching mega trends emerging from the data. For each mega trend, provide a brief description and mention 1-2 example queries supporting it. Structure using markdown headings for each trend. Keep the tone insightful and forward-looking. Focus ONLY on the provided data.""",
    INDICATOR_CATEGORY_OVERVIEW: """You are a TrendForecast.io Market Analyst providing a high-level overview of a fashion category. Analyze the provided category summary data, top style/color details, and web context. Summarize the current state of the '{category_name}' category in '{country_name}'. Highlight key performance indicators (overall growth, volume), mention the top 2-3 performing styles and colors based on the data. Briefly incorporate any relevant insights from the web context. Use clear, concise language and markdown formatting. Focus ONLY on the provided data.""",
    INDICATOR_FORECAST: """You are a TrendForecast.io Forecast Specialist providing predictions for a specific item. Analyze the provided specific item details, including historical chart data and explicit forecast data (f2, f3, f6, avg2, avg3, avg6). Also consider relevant web context. Focus on the specific item requested ({specific_item_name}). Provide a forecast summary covering the expected growth trajectory over the next 2, 3, and 6 months based ONLY on the provided forecast numbers. Briefly mention potential factors influencing this forecast based on web context or historical trends if available. Structure clearly using markdown. Keep the tone objective and data-centric. DO NOT make predictions beyond the provided forecast data.""",
    INDICATOR_COMPARISON: """You are a TrendForecast.io Comparative Analyst... [TODO: Define prompt]""",
    INDICATOR_RECOMMENDATION: """You are a TrendForecast.io Recommendation Engine... [TODO: Define prompt]""",
    INDICATOR_QA_WEB: """You are a helpful AI assistant answering a question based primarily on web search results. Synthesize the provided web answer and relevant snippets from the top web links to directly answer the original user query: '{user_query}'. Cite sources implicitly (e.g., "According to recent reports..." or "Web search suggests..."). Keep the answer concise and focused on the query. Use markdown.""",
    INDICATOR_QA_INTERNAL: """You are a helpful AI assistant answering a question based primarily on internal trend data. Synthesize the provided internal data summaries (category, item, mega trends) to directly answer the original user query: '{user_query}'. Refer to data points explicitly but concisely (e.g., "The category shows X% growth...", "Style Y has an average volume of Z..."). Keep the answer concise and focused on the query. Use markdown.""",
    INDICATOR_QA_COMBINED: """You are a helpful AI assistant answering a question using both internal data and web search results. Synthesize the provided internal data summaries AND the web answer/snippets to provide a comprehensive answer to the original user query: '{user_query}'. Integrate insights from both sources where relevant. Cite sources implicitly. Keep the answer concise and focused on the query. Use markdown.""",
    INDICATOR_UNKNOWN: """You are a helpful AI assistant. The user's request ('{user_query}' for {category_name} in {country_name}) could not be fully processed due to missing or invalid internal/external data needed for a categorized response. Please state that you cannot provide a specific analysis without the necessary underlying data.""",
    INDICATOR_ERROR: "Error processing request.",
}

def get_task_details(primary_task: str | None) -> tuple[str, str]:
    indicator = INDICATOR_UNKNOWN
    if primary_task == "get_trend": indicator = INDICATOR_TREND_DETAIL
    elif primary_task == "get_forecast": indicator = INDICATOR_FORECAST
    elif primary_task == "summarize_mega_trends": indicator = INDICATOR_MEGA_TREND
    elif primary_task == "summarize_category": indicator = INDICATOR_CATEGORY_OVERVIEW
    elif primary_task == "compare_items": indicator = INDICATOR_COMPARISON
    elif primary_task == "get_recommendation": indicator = INDICATOR_RECOMMENDATION
    elif primary_task == "qa_web_only": indicator = INDICATOR_QA_WEB
    elif primary_task == "qa_internal_only": indicator = INDICATOR_QA_INTERNAL
    elif primary_task == "qa_combined": indicator = INDICATOR_QA_COMBINED
    prompt_template = PERSONA_PROMPTS.get(indicator, PERSONA_PROMPTS[INDICATOR_UNKNOWN])
    logger.info(f"Mapped primary_task '{primary_task}' to indicator '{indicator}'.")
    return indicator, prompt_template

def format_data_for_prompt(internal_data: Dict, external_data: Dict) -> str:
    prompt_parts = []
    interpretation = internal_data.get("interpretation", {})
    if not isinstance(interpretation, dict): interpretation = {}
    original_context = interpretation.get("original_context", {})
    if not isinstance(original_context, dict): original_context = {}
    query_subjects = interpretation.get("query_subjects", {})
    if not isinstance(query_subjects, dict): query_subjects = {}
    specific_known = query_subjects.get("specific_known", [])
    if not isinstance(specific_known, list): specific_known = []

    prompt_parts.append("CONTEXT:")
    prompt_parts.append(f"- User Query: {original_context.get('query', 'N/A')}")
    prompt_parts.append(f"- Category: {original_context.get('category', 'N/A')}")
    prompt_parts.append(f"- Country: {original_context.get('country', 'N/A')}")
    if specific_known and isinstance(specific_known[0], dict):
        specific_item_name = specific_known[0].get("subject", "N/A")
        prompt_parts.append(f"- Specific Focus Item: {specific_item_name}")

    prompt_parts.append("\nAVAILABLE DATA:")
    data_found = False

    trends = internal_data.get("trends_data")
    if isinstance(trends, dict):
        data_found = True
        prompt_parts.append("\nInternal Category/Style/Color Trends:")
        cs = trends.get("category_summary")
        if isinstance(cs, dict):
            prompt_parts.append(f"- Overall Category ({cs.get('category_name', 'N/A')}): Avg Vol={cs.get('average_volume', 'N/A')}, Growth={cs.get('growth_recent', 'N/A'):.1f}%")
        style_details = trends.get("style_details", [])
        color_details = trends.get("color_details", [])
        if isinstance(style_details, list):
            top_styles = sorted([s for s in style_details if isinstance(s,dict)], key=lambda x: x.get('average_volume', 0), reverse=True)[:3]
            if top_styles: prompt_parts.append(f"- Top Styles: {', '.join([s.get('style_name', 'N/A') for s in top_styles])}")
        if isinstance(color_details, list):
            top_colors = sorted([c for c in color_details if isinstance(c,dict)], key=lambda x: x.get('average_volume', 0), reverse=True)[:3]
            if top_colors: prompt_parts.append(f"- Top Colors: {', '.join([c.get('color_name', 'N/A') for c in top_colors])}")
    else:
        logger.debug("trends_data is missing or not a dict in format_data_for_prompt.")

    details = internal_data.get("chart_details_data")
    if isinstance(details, dict):
         data_found = True
         prompt_parts.append(f"\nInternal Specific Item Details ({details.get('category_subject', 'N/A')}):")
         prompt_parts.append(f"- Avg Vol={details.get('average_volume', 'N/A')}, Growth={details.get('growth_recent', 'N/A'):.1f}%")
         if interpretation.get("primary_task") == "get_forecast":
             prompt_parts.append(f"- Forecast Growth: 2m={details.get('f2', 'N/A')}%, 3m={details.get('f3', 'N/A')}%, 6m={details.get('f6', 'N/A')}%")
    else:
         logger.debug("chart_details_data is missing or not a dict in format_data_for_prompt.")

    mega = internal_data.get("mega_trends_data")
    if isinstance(mega, list) and mega:
        data_found = True
        prompt_parts.append("\nInternal Mega Trends (Top Queries):")
        top_mega = sorted([m for m in mega if isinstance(m,dict)], key=lambda x: x.get('growth_recent', 0), reverse=True)[:3]
        if top_mega:
            for m in top_mega:
                prompt_parts.append(f"- Query: '{m.get('query_name', 'N/A')}', Category: {m.get('category_name', 'N/A')}, Growth: {m.get('growth_recent', 'N/A'):.1f}%")
    else:
        logger.debug("mega_trends_data is missing or not a list in format_data_for_prompt.")

    ext_answer = external_data.get("answer")
    ext_results = external_data.get("results", [])
    if not isinstance(ext_results, list): ext_results = []

    if ext_answer:
        data_found = True
        prompt_parts.append("\nExternal Web Context (Synthesized Answer):")
        prompt_parts.append(f"- {ext_answer}")
    elif ext_results:
        data_found = True
        prompt_parts.append("\nExternal Web Context (Top Results):")
        for i, res in enumerate(ext_results[:3]):
             if isinstance(res, dict):
                title = res.get('title', 'N/A')
                content_snippet = res.get('content', '')[:150]
                prompt_parts.append(f"- [{i+1}] {title}: {content_snippet}...")
    else:
         logger.debug("No external web data (answer or results) available for prompt.")

    if not data_found:
        logger.warning("No significant internal or external data found to format for prompt.")
        return "No specific data available to analyze for this request."

    return "\n".join(prompt_parts)

def build_final_payload_for_bubble(ai_summary: str, internal_data: Dict, external_data: Dict, task_indicator: str, final_status: str, error_message: Optional[str]) -> Dict:
    logger.debug("Building final payload object with top-level lists for Bubble...")
    payload = {
        "ai_summary": ai_summary,
        "result_type_indicator": task_indicator if final_status != INDICATOR_ERROR else INDICATOR_ERROR,
        "status": final_status,
        "error_message": error_message,
        "category_trend": None,
        "top_styles": None,
        "top_colors": None,
        "item_trend": None,
        "item_metrics": None,
        "mega_trends_top": None,
        "web_links": None,
        "web_answer": None
    }

    internal_data = internal_data or {}
    trends_data = internal_data.get("trends_data") if isinstance(internal_data.get("trends_data"), dict) else None
    chart_details_data = internal_data.get("chart_details_data") if isinstance(internal_data.get("chart_details_data"), dict) else None
    mega_trends_data = internal_data.get("mega_trends_data") if isinstance(internal_data.get("mega_trends_data"), list) else None

    external_data = external_data or {}
    external_results = external_data.get("results", []) if isinstance(external_data.get("results", []), list) else []
    external_answer = external_data.get("answer") if isinstance(external_data.get("answer"), str) else None

    if trends_data:
        cs = trends_data.get("category_summary")
        if isinstance(cs, dict):
             chart = cs.get("chart_data")
             if isinstance(chart, list) and chart:
                 payload["category_trend"] = chart
                 logger.debug("Added category_trend list.")

        all_styles = trends_data.get("style_details", [])
        if isinstance(all_styles, list):
             valid_styles = [s for s in all_styles if isinstance(s,dict)]
             if valid_styles:
                 top_styles_list = sorted(valid_styles, key=lambda x: x.get('average_volume', 0), reverse=True)[:5]
                 payload["top_styles"] = [{"name": s.get("style_name"), "growth": s.get("growth_recent"), "volume": s.get("average_volume")} for s in top_styles_list]
                 logger.debug("Added top_styles list.")

        all_colors = trends_data.get("color_details", [])
        if isinstance(all_colors, list):
            valid_colors = [c for c in all_colors if isinstance(c,dict)]
            if valid_colors:
                top_colors_list = sorted(valid_colors, key=lambda x: x.get('average_volume', 0), reverse=True)[:5]
                payload["top_colors"] = [{"name": c.get("color_name"), "growth": c.get("growth_recent"), "volume": c.get("average_volume")} for c in top_colors_list]
                logger.debug("Added top_colors list.")

    if chart_details_data:
         item_metrics_obj = {
             "name": chart_details_data.get("category_subject"),
             "growth": chart_details_data.get("growth_recent"),
             "volume": chart_details_data.get("average_volume"),
             "forecast_growth": None,
             "forecast_volume": None
         }
         if task_indicator == INDICATOR_FORECAST:
             item_metrics_obj["forecast_growth"] = {"f2": chart_details_data.get("f2"), "f3": chart_details_data.get("f3"), "f6": chart_details_data.get("f6")}
             item_metrics_obj["forecast_volume"] = {"avg2": chart_details_data.get("avg2"), "avg3": chart_details_data.get("avg3"), "avg6": chart_details_data.get("avg6")}
         # Store metrics as a list containing one object for consistency? Might be easier for Bubble.
         payload["item_metrics"] = [item_metrics_obj]
         logger.debug("Added item_metrics list.")

         item_chart = chart_details_data.get("chart_data")
         if isinstance(item_chart, list) and item_chart:
             payload["item_trend"] = item_chart
             logger.debug("Added item_trend list.")

    if mega_trends_data:
        valid_mega = [m for m in mega_trends_data if isinstance(m,dict)]
        if valid_mega:
            top_mega_list = sorted(valid_mega, key=lambda x: x.get('growth_recent', 0), reverse=True)[:10]
            payload["mega_trends_top"] = [{"name": m.get("query_name"), "growth": m.get("growth_recent"), "volume": m.get("average_volume"), "category": m.get("category_name")} for m in top_mega_list]
            logger.debug("Added mega_trends_top list.")

    if external_results:
         web_links_list = [{"title": r.get("title"), "url": r.get("url")} for r in external_results if isinstance(r, dict) and r.get("title") and r.get("url")][:5]
         if web_links_list:
             payload["web_links"] = web_links_list
             logger.debug("Added web_links list.")

    if external_answer:
         payload["web_answer"] = external_answer
         logger.debug("Added web_answer string.")

    final_payload_cleaned = {k: v for k, v in payload.items() if v is not None}
    logger.info(f"Built final bubble payload object with keys: {list(final_payload_cleaned.keys())}")
    return final_payload_cleaned

def lambda_handler(event, context):
    logger.info(f"Received combined event: {json.dumps(event)}")

    if not GEMINI_SDK_AVAILABLE:
        error_payload = build_final_payload_for_bubble("Error: LLM SDK unavailable.", {}, {}, INDICATOR_ERROR, INDICATOR_ERROR, "LLM SDK unavailable.")
        return {"statusCode": 500, "body": json.dumps(error_payload)}
    if BOTO3_CLIENT_ERROR:
        error_payload = build_final_payload_for_bubble(f"Error: {BOTO3_CLIENT_ERROR}", {}, {}, INDICATOR_ERROR, INDICATOR_ERROR, BOTO3_CLIENT_ERROR)
        return {"statusCode": 500, "body": json.dumps(error_payload)}

    internal_data = event.get("internal_data", {})
    external_data = event.get("external_data", {})
    interpretation = internal_data.get("interpretation", {}) if isinstance(internal_data, dict) else {}
    original_context = interpretation.get("original_context", {}) if isinstance(interpretation, dict) else {}
    primary_task = interpretation.get("primary_task")
    user_query = original_context.get("query", "the user query")

    upstream_errors = []
    if isinstance(internal_data, dict) and internal_data.get("errors"):
        upstream_errors.extend(internal_data["errors"])
    if isinstance(internal_data, dict) and internal_data.get("errorType"):
        upstream_errors.append({"source": "FetchInternalDataRouter", "error": internal_data.get("errorType"), "details": internal_data.get("cause", internal_data.get("errorMessage"))})
    if isinstance(external_data, dict) and external_data.get("error"):
        upstream_errors.append({"source": "FetchExternalContext", "error": external_data["error"]})
    if isinstance(external_data, dict) and external_data.get("errorType"):
        upstream_errors.append({"source": "FetchExternalContext", "error": external_data.get("errorType"), "details": external_data.get("cause", external_data.get("errorMessage"))})

    result_type_indicator, prompt_template = get_task_details(primary_task)
    logger.info(f"Using result indicator: {result_type_indicator}")

    specific_item_name = "N/A"
    if isinstance(interpretation.get("query_subjects"), dict):
         specific_known = interpretation["query_subjects"].get("specific_known", [])
         if specific_known and isinstance(specific_known[0], dict):
             specific_item_name = specific_known[0].get("subject", "N/A")

    formatted_data_context = ""
    try:
        formatted_data_context = format_data_for_prompt(internal_data, external_data)
        synthesis_prompt = prompt_template.format(
            specific_item_name=specific_item_name,
            category_name=original_context.get('category', 'N/A'),
            country_name=original_context.get('country', 'N/A'),
            user_query=user_query
        )
        synthesis_prompt += "\n\n" + formatted_data_context
        logger.debug(f"Constructed Synthesis Prompt:\n{synthesis_prompt}")
    except Exception as e:
        logger.error(f"Error formatting data for synthesis prompt: {e}", exc_info=True)
        error_payload = build_final_payload_for_bubble("Error: Could not prepare data for AI synthesis.", internal_data, external_data, INDICATOR_ERROR, INDICATOR_ERROR, str(e))
        return {"statusCode": 500, "body": json.dumps(error_payload)}

    google_api_key = get_secret_value(SECRET_NAME, "GOOGLE_API_KEY")
    if not google_api_key:
        error_payload = build_final_payload_for_bubble("Error: API key configuration error (Google).", internal_data, external_data, INDICATOR_ERROR, INDICATOR_ERROR, "API key config error")
        return {"statusCode": 500, "body": json.dumps(error_payload)}

    ai_summary = "Error: AI synthesis failed."
    llm_error = None
    try:
        if not formatted_data_context or formatted_data_context == "No specific data available to analyze for this request.":
             logger.warning("Skipping LLM call as no significant data was formatted for prompt.")
             ai_summary = PERSONA_PROMPTS[INDICATOR_UNKNOWN].format(
                 user_query=user_query,
                 category_name=original_context.get('category', 'N/A'),
                 country_name=original_context.get('country', 'N/A')
             )
             result_type_indicator = INDICATOR_UNKNOWN
        else:
             logger.info(f"Calling Synthesis LLM: {SYNTHESIS_LLM_MODEL}...")
             genai.configure(api_key=google_api_key)
             model = genai.GenerativeModel(SYNTHESIS_LLM_MODEL)
             response = model.generate_content(synthesis_prompt)
             logger.info("Synthesis LLM response received.")
             ai_summary = response.text
             logger.debug(f"Synthesis LLM Raw Response Text:\n{ai_summary}")

    except Exception as e:
        logger.error(f"Synthesis LLM call failed: {e}", exc_info=True)
        llm_error = f"Synthesis LLM call failed: {str(e)}"

    final_bubble_payload = {}
    try:
        final_status = "success"
        user_error_message = None

        if llm_error:
            final_status = INDICATOR_ERROR
            user_error_message = llm_error
            result_type_indicator = INDICATOR_ERROR
            ai_summary = "An error occurred during the analysis process."
        elif upstream_errors:
            final_status = "partial_data_success"
            logger.warning(f"Upstream errors detected: {upstream_errors}")

        final_bubble_payload = build_final_payload_for_bubble(
            ai_summary=ai_summary,
            internal_data=internal_data,
            external_data=external_data,
            task_indicator=result_type_indicator,
            final_status=final_status,
            error_message=user_error_message
        )
        logger.info(f"Final payload status: {final_status}")

    except Exception as e:
         logger.error(f"Error building final bubble payload: {e}", exc_info=True)
         final_bubble_payload = {
             "ai_summary": "An critical error occurred preparing the final response.",
             "result_type_indicator": INDICATOR_ERROR,
             "status": INDICATOR_ERROR,
             "error_message": f"Payload construction error: {str(e)}",
             "category_trend": None,
             "top_styles": None,
             "top_colors": None,
             "item_trend": None,
             "item_metrics": None,
             "mega_trends_top": None,
             "web_links": None,
             "web_answer": None
         }
         final_bubble_payload = {k: v for k, v in final_bubble_payload.items() if v is not None}


    return {
        "statusCode": 200,
        "body": json.dumps(final_bubble_payload)
    }