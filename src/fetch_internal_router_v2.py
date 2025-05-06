
# import json
# import logging
# import os
# import boto3
# from botocore.exceptions import ClientError
# import re
# from typing import Dict, Optional, List, Any
# import concurrent.futures
# import time
#
# # --- Lambda Names from Environment Variables ---
# TREND_MAIN_LAMBDA_NAME = os.environ.get("TREND_MAIN_LAMBDA_NAME", "trend_analysis_main_page_placeholder")
# MEGA_TRENDS_LAMBDA_NAME = os.environ.get("MEGA_TRENDS_LAMBDA_NAME", "dev_mega_trends_placeholder")
# CHART_DETAILS_LAMBDA_NAME = os.environ.get("CHART_DETAILS_LAMBDA_NAME", "chart_details_lambda_placeholder")
# BRAND_INSIGHT_LAMBDA_NAME = os.environ.get("BRAND_INSIGHT_LAMBDA_NAME", "brand_insight_placeholder")
# AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
#
# # --- Logger Setup ---
# logger = logging.getLogger()
# log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
# valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
# if log_level_str not in valid_log_levels: log_level_str = "INFO"
# logger.setLevel(log_level_str)
# logger.info(f"Logger initialized with level: {log_level_str}")
# logger.info(f"Target Lambdas - TrendMain: {TREND_MAIN_LAMBDA_NAME}, MegaTrends: {MEGA_TRENDS_LAMBDA_NAME}, ChartDetails: {CHART_DETAILS_LAMBDA_NAME}, BrandInsight: {BRAND_INSIGHT_LAMBDA_NAME}")
#
# # --- Boto3 Client ---
# lambda_client = None
# BOTO3_CLIENT_ERROR = None
# try:
#    session = boto3.session.Session()
#    boto_config = boto3.session.Config(max_pool_connections=50)
#    lambda_client = session.client(service_name='lambda', region_name=AWS_REGION, config=boto_config)
# except Exception as e:
#    logger.exception("CRITICAL ERROR initializing Boto3 Lambda client!")
#    BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 client: {e}"
#
# # --- Helper Functions ---
# def map_timeframe_reference(timeframe_ref_str: str | None) -> str:
#     default_value = "12"
#     logger.debug(f"Mapping timeframe '{timeframe_ref_str}'...")
#     if not timeframe_ref_str:
#         logger.debug("Null/empty -> '3'")
#         return "3"
#     tf = timeframe_ref_str.lower()
#     if tf in ["latest", "recent", "now"]: return "3"
#     if tf in ["this year", "a year", "last year", "1 year", "12 months"]: return "12"
#     if tf in ["historical", "deep historical", "all time", "long term"]: return "48"
#     match = re.search(r'(\d+)\s+month', tf)
#     if match:
#         months = match.group(1)
#         if months in ["3", "12", "48"]: return months
#         else: logger.warning(f"Invalid month count '{months}', defaulting."); return default_value
#     logger.warning(f"Unmapped timeframe '{timeframe_ref_str}', defaulting."); return default_value
#
# def safe_title_case(input_string: str) -> str:
#     return input_string.title() if isinstance(input_string, str) else ""
#
# # --- Helper for parallel invocation ---
# def invoke_lambda_task(lambda_name: str, payload: Dict, task_id: str, subject_name: Optional[str] = None) -> Dict:
#     logger.info(f"Starting task '{task_id}' to invoke {lambda_name}...")
#     start_time = time.time()
#     response_body_outer = None
#     response_body_inner = None
#     result = {"task_id": task_id, "data": None, "error_info": None}
#     try:
#         response = lambda_client.invoke(FunctionName=lambda_name, InvocationType='RequestResponse', Payload=json.dumps(payload))
#         invocation_duration = time.time() - start_time
#         logger.info(f"Raw invoke for {lambda_name} (task '{task_id}') took {invocation_duration:.3f}s")
#         response_payload = response['Payload'].read()
#         if response.get('FunctionError'):
#             error_payload_str = response_payload.decode('utf-8'); logger.error(f"FunctionError from {lambda_name} (task '{task_id}'): {error_payload_str[:500]}...")
#             try: error_details = json.loads(error_payload_str)
#             except json.JSONDecodeError: error_details = {"raw_error": error_payload_str}
#             error_msg = f"FunctionError in {lambda_name}: {error_details.get('errorMessage', 'Unknown')}"
#             result["error_info"] = {"source": lambda_name, "error": error_msg, "details": error_details}
#             if subject_name: result["error_info"]["subject"] = subject_name
#         else:
#             response_body_outer = response_payload.decode('utf-8')
#             outer_payload = json.loads(response_body_outer)
#             status_code = outer_payload.get("statusCode", 200)
#             if status_code >= 300:
#                  error_msg = f"Downstream lambda {lambda_name} returned error status code: {status_code}"; logger.error(error_msg + f" (task '{task_id}')")
#                  result["error_info"] = {"source": lambda_name, "error": error_msg, "details": outer_payload.get("body", outer_payload)}
#                  if subject_name: result["error_info"]["subject"] = subject_name
#             elif isinstance(outer_payload.get("body"), str):
#                  response_body_inner = outer_payload["body"]
#                  inner_result_payload = json.loads(response_body_inner)
#                  result["data"] = inner_result_payload; logger.info(f"Successfully parsed response from {lambda_name} (task '{task_id}').")
#             elif isinstance(outer_payload.get("body"), dict):
#                  result["data"] = outer_payload["body"]; logger.info(f"Successfully used direct body dict from {lambda_name} (task '{task_id}').")
#             elif outer_payload:
#                  result["data"] = outer_payload; logger.warning(f"Using outer payload as data from {lambda_name} (task '{task_id}') as body was missing/invalid.")
#             else:
#                  error_msg = f"Response from {lambda_name} missing or invalid 'body' field, and outer payload is empty/invalid."; logger.error(error_msg + f" (task '{task_id}')")
#                  result["error_info"] = {"source": lambda_name, "error": error_msg, "details": outer_payload}
#                  if subject_name: result["error_info"]["subject"] = subject_name
#     except ClientError as e:
#         error_msg = f"Boto3 ClientError invoking {lambda_name}: {e.response['Error']['Code']}"; logger.error(error_msg + f" (task '{task_id}')", exc_info=True)
#         result["error_info"] = {"source": lambda_name, "error": error_msg};
#         if subject_name: result["error_info"]["subject"] = subject_name
#     except json.JSONDecodeError as e:
#          error_msg = f"Failed to parse JSON response from {lambda_name}: {e}"; raw_payload_to_log = response_body_inner if response_body_inner else (response_body_outer if response_body_outer else 'N/A')
#          logger.error(error_msg + f" (task '{task_id}')", exc_info=True); result["error_info"] = {"source": lambda_name, "error": error_msg, "raw_payload": raw_payload_to_log}
#          if subject_name: result["error_info"]["subject"] = subject_name
#     except Exception as e:
#          error_msg = f"Unexpected error during {lambda_name} processing (task '{task_id}'): {str(e)}"; logger.exception(error_msg)
#          result["error_info"] = {"source": lambda_name, "error": error_msg}
#          if subject_name: result["error_info"]["subject"] = subject_name
#     end_time = time.time()
#     logger.info(f"Finished task '{task_id}' for {lambda_name} in {end_time - start_time:.3f}s. Success: {result['error_info'] is None}")
#     return result
#
#
# def lambda_handler(event, context):
#     overall_start_time = time.time()
#     try: logger.info(f"ROUTER RECEIVED EVENT: {json.dumps(event)}")
#     except Exception as log_e: logger.error(f"Could not dump incoming event: {log_e}"); logger.info(f"ROUTER RECEIVED EVENT type: {type(event)}")
#
#     if BOTO3_CLIENT_ERROR:
#         logger.error(f"Boto3 init failure: {BOTO3_CLIENT_ERROR}"); raise Exception(f"Configuration Error: {BOTO3_CLIENT_ERROR}")
#
#     # --- Input Parsing ---
#     try:
#         interpretation_result = event
#         if not isinstance(interpretation_result, dict): raise ValueError(f"Input event is not dict: {type(interpretation_result)}")
#         required_sources = interpretation_result.get("required_sources"); original_context = interpretation_result.get("original_context")
#         query_subjects = interpretation_result.get("query_subjects", {}); primary_task = interpretation_result.get("primary_task")
#         timeframe_reference = interpretation_result.get("timeframe_reference")
#         if interpretation_result is None: raise ValueError("Input interpretation_result is null.")
#         if required_sources is None: raise ValueError("Missing 'required_sources'")
#         if not isinstance(required_sources, list): raise ValueError("'required_sources' is not list.")
#         if original_context is None: raise ValueError("Missing 'original_context'")
#         if not isinstance(original_context, dict): raise ValueError("'original_context' is not dict.")
#         country_name = original_context.get("country"); category_name = original_context.get("category")
#         if not country_name: raise ValueError("Missing 'original_context.country'")
#         if not category_name: raise ValueError("Missing 'original_context.category'")
#         specific_known_subjects = query_subjects.get("specific_known", [])
#         if not isinstance(specific_known_subjects, list): raise ValueError("'query_subjects.specific_known' not list.")
#         logger.info(f"Parsed Input: Required={required_sources}, Subjects={specific_known_subjects}, Context={country_name}/{category_name}, Task={primary_task}, TimeframeRef={timeframe_reference}")
#     except (TypeError, ValueError, AttributeError, KeyError) as e:
#         try: logger.error(f"RAW EVENT AT PARSE FAILURE: {json.dumps(event)}")
#         except: logger.error(f"RAW EVENT AT PARSE FAILURE type: {type(event)}")
#         logger.error(f"Failed to parse input event: {e}", exc_info=True); raise Exception(f"Invalid input structure: {e}")
#
#     # --- Initialize Results ---
#     aggregated_results = {
#         "status": "partial", "trends_data": None, "mega_trends_data": None,
#         "chart_details_data": None, "brand_performance_data": None, "errors": []
#     }
#
#     # --- Determine which Lambdas to call ---
#     invoke_trend_main = "internal_trends_item" in required_sources or "internal_trends_category" in required_sources
#     invoke_mega_trends = "internal_mega" in required_sources
#     invoke_charts = bool(specific_known_subjects)
#     invoke_brand_performance = "internal_brand_performance" in required_sources
#
#     # --- Prepare tasks for parallel execution ---
#     tasks_to_submit = []
#     time_frame_trends = map_timeframe_reference(timeframe_reference)
#
#     if invoke_trend_main:
#         payload = {"queryStringParameters": {"country": country_name, "category": category_name, "time_frame": time_frame_trends}}
#         tasks_to_submit.append(("trends", TREND_MAIN_LAMBDA_NAME, payload, None))
#
#     if invoke_mega_trends:
#         payload = {"queryStringParameters": {"country": country_name, "category": category_name, "time_frame": time_frame_trends, "next_batch": "Previous", "cat_mode": "True"}}
#         tasks_to_submit.append(("mega", MEGA_TRENDS_LAMBDA_NAME, payload, None))
#
#     if invoke_charts:
#         first_subject_info = specific_known_subjects[0]
#         subject_name = first_subject_info.get("subject")
#         subject_type = first_subject_info.get("type")
#         if subject_name and subject_type:
#             time_frame_charts = map_timeframe_reference(timeframe_reference)
#             if time_frame_charts not in ["12", "48"]: time_frame_charts = "48"; logger.debug(f"Overriding chart timeframe to '48'")
#
#             # --- *** MODIFICATION START: Re-insert category_subject construction *** ---
#             subject_name_clean = safe_title_case(subject_name)
#             category_name_clean = safe_title_case(category_name)
#             # Construct the key, assuming downstream expects "Subject Category" format
#             if subject_name_clean != category_name_clean:
#                 category_subject_key = f"{subject_name_clean} {category_name_clean}" # e.g., "Red Sweatshirts"
#             else:
#                 category_subject_key = category_name_clean # Just use category if subject was the same
#             logger.debug(f"Constructed category_subject for chart details: {category_subject_key}")
#             # --- *** MODIFICATION END *** ---
#
#             forecast_value = "True" if primary_task == "get_forecast" else "False"
#             payload = {"queryStringParameters": {"country": country_name, "category_subject": category_subject_key, "category": category_name, "time_frame": time_frame_charts, "mode": subject_type, "forecast": forecast_value}}
#             tasks_to_submit.append(("charts", CHART_DETAILS_LAMBDA_NAME, payload, subject_name))
#         else:
#             logger.warning(f"Skipping chart details invocation: subject/type missing in {first_subject_info}")
#             aggregated_results["errors"].append({"source": CHART_DETAILS_LAMBDA_NAME, "error": "Missing subject/type for chart lookup", "details": first_subject_info})
#
#     if invoke_brand_performance:
#         target_brand = query_subjects.get("target_brand")
#         if target_brand and isinstance(target_brand, str):
#              payload = {"body": json.dumps({"competitor_domain": target_brand})}
#              tasks_to_submit.append(("brand_perf", BRAND_INSIGHT_LAMBDA_NAME, payload, target_brand))
#         else:
#              logger.error(f"Cannot invoke brand performance: target_brand missing/invalid in {query_subjects}")
#              aggregated_results["errors"].append({"source": "Router", "error": "Cannot invoke brand performance lambda", "details": "Target brand domain not found"})
#
#     # --- Execute tasks in parallel ---
#     if tasks_to_submit:
#         futures = []
#         with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
#             for task_id, lambda_name, payload, subject_name in tasks_to_submit:
#                 logger.info(f"Submitting task '{task_id}' for {lambda_name}")
#                 futures.append(executor.submit(invoke_lambda_task, lambda_name, payload, task_id, subject_name))
#
#             for future in concurrent.futures.as_completed(futures):
#                 try:
#                     task_result = future.result()
#                     task_id = task_result.get("task_id"); error_info = task_result.get("error_info"); data = task_result.get("data")
#                     if error_info:
#                         logger.warning(f"Task '{task_id}' completed with error: {error_info.get('error')}")
#                         aggregated_results["errors"].append(error_info)
#                     elif data is not None:
#                         logger.info(f"Task '{task_id}' completed successfully.")
#                         if task_id == "trends":
#                             aggregated_results["trends_data"] = {"category_summary": data.get("country_category"), "style_details": data.get("country_category_style", []), "color_details": data.get("country_color_category", [])}
#                         elif task_id == "mega": aggregated_results["mega_trends_data"] = data.get("query_category", [])
#                         elif task_id == "charts": aggregated_results["chart_details_data"] = data
#                         elif task_id == "brand_perf":
#                             if isinstance(data.get("performance_data"), list): aggregated_results["brand_performance_data"] = data["performance_data"]; logger.info(f"Stored brand performance data for {data.get('brand_domain')}")
#                             else: logger.error(f"Task 'brand_perf' returned unexpected data format: {data}"); aggregated_results["errors"].append({"source": BRAND_INSIGHT_LAMBDA_NAME, "error": "Invalid data format received", "details": "Expected 'performance_data' list key."})
#                     else: logger.error(f"Task '{task_id}' returned no data and no error."); aggregated_results["errors"].append({"source": task_id, "error": "Task returned unexpected empty result"})
#                 except Exception as e:
#                     logger.exception(f"Error retrieving result from future: {e}"); aggregated_results["errors"].append({"source": "ThreadPoolExecutor", "error": f"Failed to get task result: {str(e)}"})
#     else: logger.info("No downstream Lambdas needed based on required_sources.")
#
#     # --- Determine final status ---
#     attempted_fetch = bool(tasks_to_submit)
#     if not aggregated_results["errors"]:
#         if attempted_fetch: aggregated_results["status"] = "success"; logger.info("Internal data fetch step completed successfully.")
#         else: aggregated_results["status"] = "success_noop"; logger.info("No internal data fetch required.")
#     else: logger.warning(f"Internal data fetch step completed with {len(aggregated_results['errors'])} error(s). Status: partial.")
#
#     # --- Add interpretation result back & Log ---
#     aggregated_results["interpretation"] = interpretation_result
#     overall_end_time = time.time(); total_duration = overall_end_time - overall_start_time
#     logger.info(f"Router Lambda finished in {total_duration:.3f}s. Status: {aggregated_results['status']}.")
#     data_presence = {k: aggregated_results.get(k) is not None for k in ["trends_data", "mega_trends_data", "chart_details_data", "brand_performance_data"]}
#     logger.debug(f"Final Data Presence={data_presence}, Errors={len(aggregated_results['errors'])}")
#
#     return aggregated_results

import json
import logging
import os
import boto3
from botocore.exceptions import ClientError
import re
from typing import Dict, Optional, List, Any
import concurrent.futures
import time

# --- Lambda Names from Environment Variables ---
TREND_MAIN_LAMBDA_NAME = os.environ.get("TREND_MAIN_LAMBDA_NAME", "trend_analysis_main_page_placeholder")
MEGA_TRENDS_LAMBDA_NAME = os.environ.get("MEGA_TRENDS_LAMBDA_NAME", "dev_mega_trends_placeholder")
CHART_DETAILS_LAMBDA_NAME = os.environ.get("CHART_DETAILS_LAMBDA_NAME", "chart_details_lambda_placeholder")
BRAND_INSIGHT_LAMBDA_NAME = os.environ.get("BRAND_INSIGHT_LAMBDA_NAME", "brand_insight_placeholder")
# --- *** MODIFICATION START: Add Amazon Radar Lambda Name *** ---
AMAZON_RADAR_LAMBDA_NAME = os.environ.get("AMAZON_RADAR_LAMBDA_NAME", "dev_amazon_recommend_placeholder") # Default to dev_amazon_recommend
# --- *** MODIFICATION END *** ---
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")

# --- Logger Setup ---
logger = logging.getLogger()
log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
if log_level_str not in valid_log_levels: log_level_str = "INFO"
logger.setLevel(log_level_str)
logger.info(f"Logger initialized with level: {log_level_str}")
# --- *** MODIFICATION START: Update Logging for Amazon Radar *** ---
logger.info(
    f"Target Lambdas - TrendMain: {TREND_MAIN_LAMBDA_NAME}, MegaTrends: {MEGA_TRENDS_LAMBDA_NAME}, "
    f"ChartDetails: {CHART_DETAILS_LAMBDA_NAME}, BrandInsight: {BRAND_INSIGHT_LAMBDA_NAME}, "
    f"AmazonRadar: {AMAZON_RADAR_LAMBDA_NAME}"
)
# --- *** MODIFICATION END *** ---

# --- Boto3 Client ---
lambda_client = None
BOTO3_CLIENT_ERROR = None
try:
   session = boto3.session.Session()
   boto_config = boto3.session.Config(max_pool_connections=50) # Increased pool for more concurrency
   lambda_client = session.client(service_name='lambda', region_name=AWS_REGION, config=boto_config)
except Exception as e:
   logger.exception("CRITICAL ERROR initializing Boto3 Lambda client!")
   BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 client: {e}"

# --- Helper Functions (Keep as is) ---
def map_timeframe_reference(timeframe_ref_str: str | None) -> str:
    default_value = "12"
    logger.debug(f"Mapping timeframe '{timeframe_ref_str}'...")
    if not timeframe_ref_str: logger.debug("Null/empty -> '3'"); return "3"
    tf = timeframe_ref_str.lower()
    if tf in ["latest", "recent", "now"]: return "3"
    if tf in ["this year", "a year", "last year", "1 year", "12 months"]: return "12"
    if tf in ["historical", "deep historical", "all time", "long term"]: return "48"
    match = re.search(r'(\d+)\s+month', tf)
    if match:
        months = match.group(1)
        if months in ["3", "12", "48"]: return months
        else: logger.warning(f"Invalid month count '{months}', defaulting."); return default_value
    logger.warning(f"Unmapped timeframe '{timeframe_ref_str}', defaulting."); return default_value

def safe_title_case(input_string: str) -> str:
    return input_string.title() if isinstance(input_string, str) else ""

# --- Helper for parallel invocation (Keep as is) ---
def invoke_lambda_task(lambda_name: str, payload: Dict, task_id: str, subject_name: Optional[str] = None) -> Dict:
    logger.info(f"Starting task '{task_id}' to invoke {lambda_name}...")
    start_time = time.time()
    response_body_outer = None
    response_body_inner = None
    result = {"task_id": task_id, "data": None, "error_info": None}
    try:
        response = lambda_client.invoke(FunctionName=lambda_name, InvocationType='RequestResponse', Payload=json.dumps(payload))
        invocation_duration = time.time() - start_time
        logger.info(f"Raw invoke for {lambda_name} (task '{task_id}') took {invocation_duration:.3f}s")
        response_payload = response['Payload'].read()
        if response.get('FunctionError'):
            error_payload_str = response_payload.decode('utf-8'); logger.error(f"FunctionError from {lambda_name} (task '{task_id}'): {error_payload_str[:500]}...")
            try: error_details = json.loads(error_payload_str)
            except json.JSONDecodeError: error_details = {"raw_error": error_payload_str}
            error_msg = f"FunctionError in {lambda_name}: {error_details.get('errorMessage', 'Unknown')}"
            result["error_info"] = {"source": lambda_name, "error": error_msg, "details": error_details}
            if subject_name: result["error_info"]["subject"] = subject_name
        else:
            response_body_outer = response_payload.decode('utf-8')
            outer_payload = json.loads(response_body_outer)
            status_code = outer_payload.get("statusCode", 200)
            if status_code >= 300:
                 error_msg = f"Downstream lambda {lambda_name} returned error status code: {status_code}"; logger.error(error_msg + f" (task '{task_id}')")
                 result["error_info"] = {"source": lambda_name, "error": error_msg, "details": outer_payload.get("body", outer_payload)}
                 if subject_name: result["error_info"]["subject"] = subject_name
            elif isinstance(outer_payload.get("body"), str):
                 response_body_inner = outer_payload["body"]
                 inner_result_payload = json.loads(response_body_inner)
                 result["data"] = inner_result_payload; logger.info(f"Successfully parsed response from {lambda_name} (task '{task_id}').")
            elif isinstance(outer_payload.get("body"), dict):
                 result["data"] = outer_payload["body"]; logger.info(f"Successfully used direct body dict from {lambda_name} (task '{task_id}').")
            elif outer_payload:
                 result["data"] = outer_payload; logger.warning(f"Using outer payload as data from {lambda_name} (task '{task_id}') as body was missing/invalid.")
            else:
                 error_msg = f"Response from {lambda_name} missing or invalid 'body' field, and outer payload is empty/invalid."; logger.error(error_msg + f" (task '{task_id}')")
                 result["error_info"] = {"source": lambda_name, "error": error_msg, "details": outer_payload}
                 if subject_name: result["error_info"]["subject"] = subject_name
    except ClientError as e:
        error_msg = f"Boto3 ClientError invoking {lambda_name}: {e.response['Error']['Code']}"; logger.error(error_msg + f" (task '{task_id}')", exc_info=True)
        result["error_info"] = {"source": lambda_name, "error": error_msg};
        if subject_name: result["error_info"]["subject"] = subject_name
    except json.JSONDecodeError as e:
         error_msg = f"Failed to parse JSON response from {lambda_name}: {e}"; raw_payload_to_log = response_body_inner if response_body_inner else (response_body_outer if response_body_outer else 'N/A')
         logger.error(error_msg + f" (task '{task_id}')", exc_info=True); result["error_info"] = {"source": lambda_name, "error": error_msg, "raw_payload": raw_payload_to_log}
         if subject_name: result["error_info"]["subject"] = subject_name
    except Exception as e:
         error_msg = f"Unexpected error during {lambda_name} processing (task '{task_id}'): {str(e)}"; logger.exception(error_msg)
         result["error_info"] = {"source": lambda_name, "error": error_msg}
         if subject_name: result["error_info"]["subject"] = subject_name
    end_time = time.time()
    logger.info(f"Finished task '{task_id}' for {lambda_name} in {end_time - start_time:.3f}s. Success: {result['error_info'] is None}")
    return result


def lambda_handler(event, context):
    overall_start_time = time.time()
    try: logger.info(f"ROUTER RECEIVED EVENT: {json.dumps(event)}")
    except Exception as log_e: logger.error(f"Could not dump incoming event: {log_e}"); logger.info(f"ROUTER RECEIVED EVENT type: {type(event)}")

    if BOTO3_CLIENT_ERROR:
        logger.error(f"Boto3 init failure: {BOTO3_CLIENT_ERROR}"); raise Exception(f"Configuration Error: {BOTO3_CLIENT_ERROR}")

    # --- Input Parsing ---
    try:
        interpretation_result = event
        if not isinstance(interpretation_result, dict): raise ValueError(f"Input event is not dict: {type(interpretation_result)}")
        required_sources = interpretation_result.get("required_sources"); original_context = interpretation_result.get("original_context")
        query_subjects = interpretation_result.get("query_subjects", {}); primary_task = interpretation_result.get("primary_task")
        timeframe_reference = interpretation_result.get("timeframe_reference")
        if interpretation_result is None: raise ValueError("Input interpretation_result is null.")
        if required_sources is None: raise ValueError("Missing 'required_sources'")
        if not isinstance(required_sources, list): raise ValueError("'required_sources' is not list.")
        if original_context is None: raise ValueError("Missing 'original_context'")
        if not isinstance(original_context, dict): raise ValueError("'original_context' is not dict.")
        country_name = original_context.get("country"); # category can be placeholder
        category_name_from_context = original_context.get("category") # This is the one from input, might be placeholder
        if not country_name: raise ValueError("Missing 'original_context.country'")
        if not category_name_from_context: raise ValueError("Missing 'original_context.category'") # From Interpreter, should always be there
        specific_known_subjects = query_subjects.get("specific_known", [])
        if not isinstance(specific_known_subjects, list): raise ValueError("'query_subjects.specific_known' not list.")
        logger.info(f"Parsed Input: Required={required_sources}, Subjects={specific_known_subjects}, ContextCountry={country_name}, ContextCatFromInput={category_name_from_context}, Task={primary_task}, TimeframeRef={timeframe_reference}")
    except (TypeError, ValueError, AttributeError, KeyError) as e:
        try: logger.error(f"RAW EVENT AT PARSE FAILURE: {json.dumps(event)}")
        except: logger.error(f"RAW EVENT AT PARSE FAILURE type: {type(event)}")
        logger.error(f"Failed to parse input event: {e}", exc_info=True); raise Exception(f"Invalid input structure: {e}")

    # --- Initialize Results ---
    # --- *** MODIFICATION START: Add amazon_radar_data key *** ---
    aggregated_results = {
        "status": "partial", "trends_data": None, "mega_trends_data": None,
        "chart_details_data": None, "brand_performance_data": None,
        "amazon_radar_data": None, # New key
        "errors": []
    }
    # --- *** MODIFICATION END *** ---

    # --- Determine which Lambdas to call ---
    invoke_trend_main = "internal_trends_item" in required_sources or "internal_trends_category" in required_sources
    invoke_mega_trends = "internal_mega" in required_sources
    invoke_charts = bool(specific_known_subjects)
    invoke_brand_performance = "internal_brand_performance" in required_sources
    # --- *** MODIFICATION START: Add Amazon Radar Invocation Flag *** ---
    invoke_amazon_radar = "internal_amazon_radar" in required_sources
    # --- *** MODIFICATION END *** ---

    # --- Prepare tasks for parallel execution ---
    tasks_to_submit = []
    time_frame_trends = map_timeframe_reference(timeframe_reference)

    # Use category_name_from_context for trends and mega trends calls
    # For charts, it will use the one in original_context if not overridden
    # For Amazon Radar, it will use target_category from original_context

    if invoke_trend_main:
        payload = {"queryStringParameters": {"country": country_name, "category": category_name_from_context, "time_frame": time_frame_trends}}
        tasks_to_submit.append(("trends", TREND_MAIN_LAMBDA_NAME, payload, None))

    if invoke_mega_trends:
        payload = {"queryStringParameters": {"country": country_name, "category": category_name_from_context, "time_frame": time_frame_trends, "next_batch": "Previous", "cat_mode": "True"}}
        tasks_to_submit.append(("mega", MEGA_TRENDS_LAMBDA_NAME, payload, None))

    if invoke_charts:
        first_subject_info = specific_known_subjects[0]
        subject_name = first_subject_info.get("subject")
        subject_type = first_subject_info.get("type")
        if subject_name and subject_type:
            time_frame_charts = map_timeframe_reference(timeframe_reference)
            if time_frame_charts not in ["12", "48"]: time_frame_charts = "48"; logger.debug(f"Overriding chart timeframe to '48'")
            subject_name_clean = safe_title_case(subject_name)
            category_name_for_charts = safe_title_case(category_name_from_context) # Use the category from context
            if subject_name_clean != category_name_for_charts:
                category_subject_key = f"{subject_name_clean} {category_name_for_charts}"
            else:
                category_subject_key = category_name_for_charts
            logger.debug(f"Constructed category_subject for chart details: {category_subject_key}")
            forecast_value = "True" if primary_task == "get_forecast" else "False"
            payload = {"queryStringParameters": {"country": country_name, "category_subject": category_subject_key, "category": category_name_from_context, "time_frame": time_frame_charts, "mode": subject_type, "forecast": forecast_value}}
            tasks_to_submit.append(("charts", CHART_DETAILS_LAMBDA_NAME, payload, subject_name))
        else:
            logger.warning(f"Skipping chart details invocation: subject/type missing in {first_subject_info}")
            aggregated_results["errors"].append({"source": CHART_DETAILS_LAMBDA_NAME, "error": "Missing subject/type for chart lookup", "details": first_subject_info})

    if invoke_brand_performance:
        target_brand = query_subjects.get("target_brand")
        if target_brand and isinstance(target_brand, str):
             payload = {"body": json.dumps({"competitor_domain": target_brand})}
             tasks_to_submit.append(("brand_perf", BRAND_INSIGHT_LAMBDA_NAME, payload, target_brand))
        else:
             logger.error(f"Cannot invoke brand performance: target_brand missing/invalid in {query_subjects}")
             aggregated_results["errors"].append({"source": "Router", "error": "Cannot invoke brand performance lambda", "details": "Target brand domain not found"})

    # --- *** MODIFICATION START: Add Amazon Radar Task Submission *** ---
    if invoke_amazon_radar:
        # Extract department and actual category from original_context (set by Interpreter)
        target_department = original_context.get("target_department")
        target_category_for_amazon = original_context.get("target_category") # This is the actual category, not placeholder

        if target_department and target_category_for_amazon:
            payload = {
                "queryStringParameters": {
                    "country": country_name,
                    "category": target_category_for_amazon,
                    "department": target_department,
                    "sort_type": "revenue_descending" # Static as per requirement
                }
            }
            tasks_to_submit.append(("amazon", AMAZON_RADAR_LAMBDA_NAME, payload, None))
        else:
            logger.error(f"Cannot invoke Amazon Radar: target_department or target_category missing in original_context: {original_context}")
            aggregated_results["errors"].append({
                "source": "Router",
                "error": "Cannot invoke Amazon Radar lambda",
                "details": "Target department or category not found in interpretation result"
            })
    # --- *** MODIFICATION END *** ---

    # --- Execute tasks in parallel ---
    if tasks_to_submit:
        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            for task_id, lambda_name, payload, subject_name in tasks_to_submit:
                logger.info(f"Submitting task '{task_id}' for {lambda_name}")
                futures.append(executor.submit(invoke_lambda_task, lambda_name, payload, task_id, subject_name))

            for future in concurrent.futures.as_completed(futures):
                try:
                    task_result = future.result()
                    task_id = task_result.get("task_id"); error_info = task_result.get("error_info"); data = task_result.get("data")
                    if error_info:
                        logger.warning(f"Task '{task_id}' completed with error: {error_info.get('error')}")
                        aggregated_results["errors"].append(error_info)
                    elif data is not None:
                        logger.info(f"Task '{task_id}' completed successfully.")
                        if task_id == "trends":
                            aggregated_results["trends_data"] = {"category_summary": data.get("country_category"), "style_details": data.get("country_category_style", []), "color_details": data.get("country_color_category", [])}
                        elif task_id == "mega": aggregated_results["mega_trends_data"] = data.get("query_category", [])
                        elif task_id == "charts": aggregated_results["chart_details_data"] = data
                        elif task_id == "brand_perf":
                            if isinstance(data.get("performance_data"), list): aggregated_results["brand_performance_data"] = data["performance_data"]; logger.info(f"Stored brand performance data for {data.get('brand_domain')}")
                            else: logger.error(f"Task 'brand_perf' returned unexpected data format: {data}"); aggregated_results["errors"].append({"source": BRAND_INSIGHT_LAMBDA_NAME, "error": "Invalid data format received", "details": "Expected 'performance_data' list key."})
                        # --- *** MODIFICATION START: Handle Amazon Radar Result *** ---
                        elif task_id == "amazon":
                            # Assuming 'data' is the parsed inner body of dev_amazon_recommend
                            # which contains 'country_department_category' and 'category_dep_market_size'
                            aggregated_results["amazon_radar_data"] = data
                            logger.info(f"Stored Amazon Radar data. Keys: {list(data.keys()) if isinstance(data,dict) else 'Not a dict'}")
                        # --- *** MODIFICATION END ---
                    else: logger.error(f"Task '{task_id}' returned no data and no error."); aggregated_results["errors"].append({"source": task_id, "error": "Task returned unexpected empty result"})
                except Exception as e:
                    logger.exception(f"Error retrieving result from future: {e}"); aggregated_results["errors"].append({"source": "ThreadPoolExecutor", "error": f"Failed to get task result: {str(e)}"})
    else: logger.info("No downstream Lambdas needed based on required_sources.")

    # --- Determine final status ---
    attempted_fetch = bool(tasks_to_submit)
    if not aggregated_results["errors"]:
        if attempted_fetch: aggregated_results["status"] = "success"; logger.info("Internal data fetch step completed successfully.")
        else: aggregated_results["status"] = "success_noop"; logger.info("No internal data fetch required.")
    else: logger.warning(f"Internal data fetch step completed with {len(aggregated_results['errors'])} error(s). Status: partial.")

    # --- Add interpretation result back & Log ---
    aggregated_results["interpretation"] = interpretation_result
    overall_end_time = time.time(); total_duration = overall_end_time - overall_start_time
    logger.info(f"Router Lambda finished in {total_duration:.3f}s. Status: {aggregated_results['status']}.")
    # --- *** MODIFICATION START: Update Data Presence Logging *** ---
    data_presence = {
        k: aggregated_results.get(k) is not None
        for k in ["trends_data", "mega_trends_data", "chart_details_data", "brand_performance_data", "amazon_radar_data"]
    }
    # --- *** MODIFICATION END --- ---
    logger.debug(f"Final Data Presence={data_presence}, Errors={len(aggregated_results['errors'])}")

    return aggregated_results