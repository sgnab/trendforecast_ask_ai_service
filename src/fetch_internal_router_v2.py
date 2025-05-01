#
#
# import json
# import logging
# import os
# import boto3
# from botocore.exceptions import ClientError
# import re
# from typing import Dict, Optional, List, Any
#
# TREND_MAIN_LAMBDA_NAME = os.environ.get("TREND_MAIN_LAMBDA_NAME", "trend_analysis_main_page_placeholder")
# MEGA_TRENDS_LAMBDA_NAME = os.environ.get("MEGA_TRENDS_LAMBDA_NAME", "dev_mega_trends_placeholder")
# CHART_DETAILS_LAMBDA_NAME = os.environ.get("CHART_DETAILS_LAMBDA_NAME", "chart_details_lambda_placeholder")
# AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
#
# logger = logging.getLogger()
# log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
# valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
# if log_level_str not in valid_log_levels: log_level_str = "INFO"
# logger.setLevel(log_level_str)
# logger.info(f"Logger initialized with level: {log_level_str}")
# logger.info(f"Target Lambdas - TrendMain: {TREND_MAIN_LAMBDA_NAME}, MegaTrends: {MEGA_TRENDS_LAMBDA_NAME}, ChartDetails: {CHART_DETAILS_LAMBDA_NAME}")
#
# lambda_client = None
# BOTO3_CLIENT_ERROR = None
# try:
#    session = boto3.session.Session()
#    lambda_client = session.client(service_name='lambda', region_name=AWS_REGION)
# except Exception as e:
#    logger.exception("CRITICAL ERROR initializing Boto3 Lambda client!")
#    BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 client: {e}"
#
# def map_timeframe_reference(timeframe_ref_str: str | None) -> str:
#     default_value = "12"
#     logger.debug(f"Mapping timeframe '{timeframe_ref_str}'...")
#     if not timeframe_ref_str:
#         logger.debug("Null/empty -> '3'")
#         return "3"
#     tf = timeframe_ref_str.lower()
#     if tf in ["latest", "recent", "now"]:
#         logger.debug("Latest/recent -> '3'")
#         return "3"
#     if tf in ["this year", "a year", "last year", "1 year", "12 months"]:
#         logger.debug("Year -> '12'")
#         return "12"
#     if tf in ["historical", "deep historical", "all time", "long term"]:
#         logger.debug("Historical -> '48'")
#         return "48"
#     match = re.search(r'(\d+)\s+month', tf)
#     if match:
#         months = match.group(1)
#         logger.debug(f"Regex match -> '{months}'")
#         if months in ["3", "12", "48"]:
#              return months
#         else:
#              logger.warning(f"Regex extracted invalid month count '{months}', defaulting to '{default_value}'")
#              return default_value
#     logger.warning(f"Unmapped timeframe '{timeframe_ref_str}', defaulting to '{default_value}'")
#     return default_value
#
# def safe_title_case(input_string: str) -> str:
#     return input_string.title() if isinstance(input_string, str) else ""
#
# def lambda_handler(event, context):
#     try:
#         logger.info(f"ROUTER RECEIVED EVENT: {json.dumps(event)}")
#     except Exception as log_e:
#         logger.error(f"Could not dump incoming event for logging: {log_e}")
#         logger.info(f"ROUTER RECEIVED EVENT (raw type): {type(event)}")
#
#     if BOTO3_CLIENT_ERROR:
#         logger.error(f"Boto3 init failure: {BOTO3_CLIENT_ERROR}")
#         raise Exception(f"Configuration Error: {BOTO3_CLIENT_ERROR}")
#
#     try:
#         interpretation_result = event
#         if not isinstance(interpretation_result, dict):
#             raise ValueError(f"Input event is not a dictionary, type received: {type(interpretation_result)}")
#
#         required_sources = interpretation_result.get("required_sources")
#         original_context = interpretation_result.get("original_context")
#         query_subjects = interpretation_result.get("query_subjects", {})
#         primary_task = interpretation_result.get("primary_task")
#         timeframe_reference = interpretation_result.get("timeframe_reference")
#
#         if required_sources is None: raise ValueError("Input missing required field: 'required_sources'")
#         if not isinstance(required_sources, list): raise ValueError("'required_sources' is not a list.")
#         if original_context is None: raise ValueError("Input missing required field: 'original_context'")
#         if not isinstance(original_context, dict): raise ValueError("'original_context' is not a dictionary.")
#
#         country_name = original_context.get("country")
#         category_name = original_context.get("category")
#
#         if not country_name: raise ValueError("Input missing required field: 'original_context.country'")
#         if not category_name: raise ValueError("Input missing required field: 'original_context.category'")
#
#         specific_known_subjects = query_subjects.get("specific_known", [])
#         if not isinstance(specific_known_subjects, list): raise ValueError("'query_subjects.specific_known' is not a list.")
#
#         logger.info(f"Successfully Parsed Input: Required={required_sources}, Subjects={specific_known_subjects}, Context={country_name}/{category_name}, Task={primary_task}, TimeframeRef={timeframe_reference}")
#
#     except (TypeError, ValueError, AttributeError, KeyError) as e:
#         try: logger.error(f"RAW EVENT AT PARSE FAILURE: {json.dumps(event)}")
#         except: logger.error(f"RAW EVENT AT PARSE FAILURE (type): {type(event)}")
#         logger.error(f"Failed to parse input event: {e}", exc_info=True)
#         raise Exception(f"Invalid input structure or missing key: {e}")
#
#     aggregated_results = {
#         "status": "partial",
#         "trends_data": None,
#         "mega_trends_data": None,
#         "chart_details_data": None,
#         "errors": []
#     }
#
#     invoke_trend_main = "internal_trends_item" in required_sources or "internal_trends_category" in required_sources
#     invoke_mega_trends = "internal_mega" in required_sources
#     invoke_charts = bool(specific_known_subjects)
#
#     if invoke_trend_main:
#         lambda_name = TREND_MAIN_LAMBDA_NAME
#         logger.info(f"Invoking {lambda_name}...")
#         response_body_outer = None
#         response_body_inner = None
#         try:
#             time_frame = map_timeframe_reference(timeframe_reference)
#             payload = {"queryStringParameters": {"country": country_name, "category": category_name, "time_frame": time_frame}}
#             logger.debug(f"Payload for {lambda_name}: {json.dumps(payload)}")
#             response = lambda_client.invoke(FunctionName=lambda_name, InvocationType='RequestResponse', Payload=json.dumps(payload))
#             logger.info(f"Response received from {lambda_name}")
#
#             response_payload = response['Payload'].read()
#             if response.get('FunctionError'):
#                 error_payload_str = response_payload.decode('utf-8')
#                 logger.error(f"FunctionError received from {lambda_name}: {error_payload_str[:1000]}...")
#                 try: error_details = json.loads(error_payload_str)
#                 except json.JSONDecodeError: error_details = {"raw_error": error_payload_str}
#                 error_msg = f"FunctionError in {lambda_name}: {error_details.get('errorMessage', 'Unknown')}"
#                 aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "details": error_details})
#             else:
#                 response_body_outer = response_payload.decode('utf-8')
#                 logger.debug(f"Raw OUTER response body from {lambda_name}: {response_body_outer[:1000]}...")
#                 outer_payload = json.loads(response_body_outer)
#
#                 if isinstance(outer_payload.get("body"), str):
#                     response_body_inner = outer_payload["body"]
#                     logger.debug(f"INNER body string snippet from {lambda_name}: {response_body_inner[:1000]}...")
#                     inner_result_payload = json.loads(response_body_inner)
#                     parsed_trends = {
#                         "category_summary": inner_result_payload.get("country_category"),
#                         "style_details": inner_result_payload.get("country_category_style", []),
#                         "color_details": inner_result_payload.get("country_color_category", [])
#                     }
#                     aggregated_results["trends_data"] = parsed_trends
#                     logger.info(f"Successfully parsed INNER response from {lambda_name}.")
#                     logger.debug(f"Parsed trends_data snippet: {json.dumps(parsed_trends)[:1000]}...")
#                 else:
#                     error_msg = f"Response from {lambda_name} missing or invalid 'body' field."
#                     logger.error(error_msg)
#                     logger.debug(f"Outer payload received was: {outer_payload}")
#                     aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "details": outer_payload})
#
#         except ClientError as e:
#             error_msg = f"Boto3 ClientError invoking {lambda_name}: {e.response['Error']['Code']}"
#             logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg})
#         except json.JSONDecodeError as e:
#              error_msg = f"Failed to parse JSON response from {lambda_name}: {e}"
#              raw_payload_to_log = response_body_inner if response_body_inner else (response_body_outer if response_body_outer else 'N/A')
#              logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "raw_payload": raw_payload_to_log})
#         except Exception as e:
#              error_msg = f"Unexpected error during {lambda_name} processing: {str(e)}"
#              logger.exception(error_msg); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg})
#
#     if invoke_mega_trends:
#         lambda_name = MEGA_TRENDS_LAMBDA_NAME
#         logger.info(f"Invoking {lambda_name}...")
#         response_body_outer = None
#         response_body_inner = None
#         try:
#             time_frame = map_timeframe_reference(timeframe_reference)
#             payload = {"queryStringParameters": {"country": country_name, "category": category_name, "time_frame": time_frame, "next_batch": "Previous", "cat_mode": "False"}}
#             logger.debug(f"Payload for {lambda_name}: {json.dumps(payload)}")
#             response = lambda_client.invoke(FunctionName=lambda_name, InvocationType='RequestResponse', Payload=json.dumps(payload))
#             logger.info(f"Response received from {lambda_name}")
#
#             response_payload = response['Payload'].read()
#             if response.get('FunctionError'):
#                 error_payload_str = response_payload.decode('utf-8')
#                 logger.error(f"FunctionError received from {lambda_name}: {error_payload_str[:1000]}...")
#                 try: error_details = json.loads(error_payload_str)
#                 except json.JSONDecodeError: error_details = {"raw_error": error_payload_str}
#                 error_msg = f"FunctionError in {lambda_name}: {error_details.get('errorMessage', 'Unknown')}"
#                 aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "details": error_details})
#             else:
#                 response_body_outer = response_payload.decode('utf-8')
#                 logger.debug(f"Raw OUTER response body from {lambda_name}: {response_body_outer[:1000]}...")
#                 outer_payload = json.loads(response_body_outer)
#                 if isinstance(outer_payload.get("body"), str):
#                     response_body_inner = outer_payload["body"]
#                     logger.debug(f"INNER body string snippet from {lambda_name}: {response_body_inner[:1000]}...")
#                     inner_result_payload = json.loads(response_body_inner)
#                     mega_trends_list = inner_result_payload.get("query_category", [])
#                     aggregated_results["mega_trends_data"] = mega_trends_list
#                     logger.info(f"Successfully parsed INNER response from {lambda_name}. Found {len(mega_trends_list)} mega trend items.")
#                     logger.debug(f"Parsed mega_trends_data snippet: {json.dumps(mega_trends_list)[:1000]}...")
#                 else:
#                      error_msg = f"Response from {lambda_name} missing or invalid 'body' field."
#                      logger.error(error_msg); logger.debug(f"Outer payload: {outer_payload}")
#                      aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "details": outer_payload})
#
#         except ClientError as e:
#             error_msg = f"Boto3 ClientError invoking {lambda_name}: {e.response['Error']['Code']}"
#             logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg})
#         except json.JSONDecodeError as e:
#              error_msg = f"Failed to parse JSON response from {lambda_name}: {e}"
#              raw_payload_to_log = response_body_inner if response_body_inner else (response_body_outer if response_body_outer else 'N/A')
#              logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "raw_payload": raw_payload_to_log})
#         except Exception as e:
#              error_msg = f"Unexpected error during {lambda_name} processing: {str(e)}"
#              logger.exception(error_msg); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg})
#
#     if invoke_charts:
#         lambda_name = CHART_DETAILS_LAMBDA_NAME
#         first_subject_info = specific_known_subjects[0]
#         subject_name = first_subject_info.get("subject")
#         subject_type = first_subject_info.get("type")
#
#         if subject_name and subject_type:
#             logger.info(f"Invoking {lambda_name} for '{subject_name}' ({subject_type})...")
#             response_body_outer = None
#             response_body_inner = None
#             try:
#                 time_frame = map_timeframe_reference(timeframe_reference)
#                 if time_frame not in ["12", "48"]: time_frame = "48"; logger.debug(f"Overriding chart timeframe to '48'")
#                 formatted_subject_name = safe_title_case(subject_name).replace(' ', '_')
#                 category_subject_key = f"{safe_title_case(category_name)}_{formatted_subject_name}"
#                 forecast_value = "True" if primary_task == "get_forecast" else "False"
#                 payload = {"queryStringParameters": {"country": country_name, "category_subject": category_subject_key, "category": category_name, "time_frame": time_frame, "mode": subject_type, "forecast": forecast_value}}
#                 logger.debug(f"Payload for {lambda_name}: {json.dumps(payload)}")
#                 response = lambda_client.invoke(FunctionName=lambda_name, InvocationType='RequestResponse', Payload=json.dumps(payload))
#                 logger.info(f"Response received from {lambda_name}")
#
#                 response_payload = response['Payload'].read()
#                 if response.get('FunctionError'):
#                     error_payload_str = response_payload.decode('utf-8')
#                     logger.error(f"FunctionError received from {lambda_name}: {error_payload_str[:1000]}...")
#                     try: error_details = json.loads(error_payload_str)
#                     except json.JSONDecodeError: error_details = {"raw_error": error_payload_str}
#                     error_msg = f"FunctionError in {lambda_name}: {error_details.get('errorMessage', 'Unknown')}"
#                     aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "details": error_details, "subject": subject_name})
#                 else:
#                     response_body_outer = response_payload.decode('utf-8')
#                     logger.debug(f"Raw OUTER response body from {lambda_name}: {response_body_outer[:1000]}...")
#                     outer_payload = json.loads(response_body_outer)
#                     if isinstance(outer_payload.get("body"), str):
#                          response_body_inner = outer_payload["body"]
#                          logger.debug(f"INNER body string snippet from {lambda_name}: {response_body_inner[:1000]}...")
#                          inner_result_payload = json.loads(response_body_inner)
#                          aggregated_results["chart_details_data"] = inner_result_payload
#                          logger.info(f"Successfully parsed INNER response from {lambda_name} for subject '{subject_name}'.")
#                          logger.debug(f"Parsed chart_details_data snippet: {json.dumps(inner_result_payload)[:1000]}...")
#                     else:
#                          error_msg = f"Response from {lambda_name} missing or invalid 'body' field."
#                          logger.error(error_msg); logger.debug(f"Outer payload: {outer_payload}")
#                          aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "details": outer_payload, "subject": subject_name})
#
#             except ClientError as e:
#                  error_msg = f"Boto3 ClientError invoking {lambda_name}: {e.response['Error']['Code']}"
#                  logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "subject": subject_name})
#             except json.JSONDecodeError as e:
#                  error_msg = f"Failed to parse JSON response from {lambda_name}: {e}"
#                  raw_payload_to_log = response_body_inner if response_body_inner else (response_body_outer if response_body_outer else 'N/A')
#                  logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "raw_payload": raw_payload_to_log, "subject": subject_name})
#             except Exception as e:
#                  error_msg = f"Unexpected error during {lambda_name} processing for '{subject_name}': {str(e)}"
#                  logger.exception(error_msg); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "subject": subject_name})
#         else:
#             logger.warning(f"Skipping {lambda_name}: subject/type missing in {first_subject_info}")
#             aggregated_results["errors"].append({"source": lambda_name, "error": "Missing subject/type", "details": first_subject_info})
#
#     if not aggregated_results["errors"]:
#         attempted_fetch = invoke_trend_main or invoke_mega_trends or invoke_charts
#         if attempted_fetch:
#              aggregated_results["status"] = "success"
#              logger.info("Internal data fetch step completed successfully.")
#         else:
#              aggregated_results["status"] = "success_noop"
#              logger.info("No internal data fetch required by interpretation.")
#     else:
#         logger.warning(f"Internal data fetch step completed with {len(aggregated_results['errors'])} error(s). Status: partial.")
#
#     aggregated_results["interpretation"] = interpretation_result
#     logger.info(f"Returning aggregated results (Status: {aggregated_results['status']}).")
#     final_keys = list(aggregated_results.keys())
#     data_presence = {
#         "trends_data_present": aggregated_results["trends_data"] is not None,
#         "mega_trends_data_present": aggregated_results["mega_trends_data"] is not None,
#         "chart_details_data_present": aggregated_results["chart_details_data"] is not None
#     }
#     logger.debug(f"Final aggregated_results structure: Keys={final_keys}, DataPresence={data_presence}, Errors={len(aggregated_results['errors'])}")
#
#     return aggregated_results


import json
import logging
import os
import boto3
from botocore.exceptions import ClientError
import re
from typing import Dict, Optional, List, Any

TREND_MAIN_LAMBDA_NAME = os.environ.get("TREND_MAIN_LAMBDA_NAME", "trend_analysis_main_page_placeholder")
MEGA_TRENDS_LAMBDA_NAME = os.environ.get("MEGA_TRENDS_LAMBDA_NAME", "dev_mega_trends_placeholder")
CHART_DETAILS_LAMBDA_NAME = os.environ.get("CHART_DETAILS_LAMBDA_NAME", "chart_details_lambda_placeholder")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")

logger = logging.getLogger()
log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
if log_level_str not in valid_log_levels: log_level_str = "INFO"
logger.setLevel(log_level_str)
logger.info(f"Logger initialized with level: {log_level_str}")
logger.info(f"Target Lambdas - TrendMain: {TREND_MAIN_LAMBDA_NAME}, MegaTrends: {MEGA_TRENDS_LAMBDA_NAME}, ChartDetails: {CHART_DETAILS_LAMBDA_NAME}")

lambda_client = None
BOTO3_CLIENT_ERROR = None
try:
   session = boto3.session.Session()
   lambda_client = session.client(service_name='lambda', region_name=AWS_REGION)
except Exception as e:
   logger.exception("CRITICAL ERROR initializing Boto3 Lambda client!")
   BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 client: {e}"

def map_timeframe_reference(timeframe_ref_str: str | None) -> str:
    default_value = "12"
    logger.debug(f"Mapping timeframe '{timeframe_ref_str}'...")
    if not timeframe_ref_str:
        logger.debug("Null/empty -> '3'")
        return "3"
    tf = timeframe_ref_str.lower()
    if tf in ["latest", "recent", "now"]:
        logger.debug("Latest/recent -> '3'")
        return "3"
    if tf in ["this year", "a year", "last year", "1 year", "12 months"]:
        logger.debug("Year -> '12'")
        return "12"
    if tf in ["historical", "deep historical", "all time", "long term"]:
        logger.debug("Historical -> '48'")
        return "48"
    match = re.search(r'(\d+)\s+month', tf)
    if match:
        months = match.group(1)
        logger.debug(f"Regex match -> '{months}'")
        if months in ["3", "12", "48"]:
             return months
        else:
             logger.warning(f"Regex extracted invalid month count '{months}', defaulting to '{default_value}'")
             return default_value
    logger.warning(f"Unmapped timeframe '{timeframe_ref_str}', defaulting to '{default_value}'")
    return default_value

def safe_title_case(input_string: str) -> str:
    return input_string.title() if isinstance(input_string, str) else ""

def lambda_handler(event, context):
    try:
        logger.info(f"ROUTER RECEIVED EVENT: {json.dumps(event)}")
    except Exception as log_e:
        logger.error(f"Could not dump incoming event for logging: {log_e}")
        logger.info(f"ROUTER RECEIVED EVENT (raw type): {type(event)}")

    if BOTO3_CLIENT_ERROR:
        logger.error(f"Boto3 init failure: {BOTO3_CLIENT_ERROR}")
        raise Exception(f"Configuration Error: {BOTO3_CLIENT_ERROR}")

    try:
        interpretation_result = event
        if not isinstance(interpretation_result, dict):
            raise ValueError(f"Input event is not a dictionary, type received: {type(interpretation_result)}")

        required_sources = interpretation_result.get("required_sources")
        original_context = interpretation_result.get("original_context")
        query_subjects = interpretation_result.get("query_subjects", {})
        primary_task = interpretation_result.get("primary_task")
        timeframe_reference = interpretation_result.get("timeframe_reference")

        if required_sources is None: raise ValueError("Input missing required field: 'required_sources'")
        if not isinstance(required_sources, list): raise ValueError("'required_sources' is not a list.")
        if original_context is None: raise ValueError("Input missing required field: 'original_context'")
        if not isinstance(original_context, dict): raise ValueError("'original_context' is not a dictionary.")

        country_name = original_context.get("country")
        category_name = original_context.get("category")

        if not country_name: raise ValueError("Input missing required field: 'original_context.country'")
        if not category_name: raise ValueError("Input missing required field: 'original_context.category'")

        specific_known_subjects = query_subjects.get("specific_known", [])
        if not isinstance(specific_known_subjects, list): raise ValueError("'query_subjects.specific_known' is not a list.")

        logger.info(f"Successfully Parsed Input: Required={required_sources}, Subjects={specific_known_subjects}, Context={country_name}/{category_name}, Task={primary_task}, TimeframeRef={timeframe_reference}")

    except (TypeError, ValueError, AttributeError, KeyError) as e:
        try: logger.error(f"RAW EVENT AT PARSE FAILURE: {json.dumps(event)}")
        except: logger.error(f"RAW EVENT AT PARSE FAILURE (type): {type(event)}")
        logger.error(f"Failed to parse input event: {e}", exc_info=True)
        raise Exception(f"Invalid input structure or missing key: {e}")

    aggregated_results = {
        "status": "partial",
        "trends_data": None,
        "mega_trends_data": None,
        "chart_details_data": None,
        "errors": []
    }

    invoke_trend_main = "internal_trends_item" in required_sources or "internal_trends_category" in required_sources
    invoke_mega_trends = "internal_mega" in required_sources
    invoke_charts = bool(specific_known_subjects)

    if invoke_trend_main:
        lambda_name = TREND_MAIN_LAMBDA_NAME
        logger.info(f"Invoking {lambda_name}...")
        response_body_outer = None
        response_body_inner = None
        try:
            time_frame = map_timeframe_reference(timeframe_reference)
            payload = {"queryStringParameters": {"country": country_name, "category": category_name, "time_frame": time_frame}}
            logger.debug(f"Payload for {lambda_name}: {json.dumps(payload)}")
            response = lambda_client.invoke(FunctionName=lambda_name, InvocationType='RequestResponse', Payload=json.dumps(payload))
            logger.info(f"Response received from {lambda_name}")

            response_payload = response['Payload'].read()
            if response.get('FunctionError'):
                error_payload_str = response_payload.decode('utf-8')
                logger.error(f"FunctionError received from {lambda_name}: {error_payload_str[:1000]}...")
                try: error_details = json.loads(error_payload_str)
                except json.JSONDecodeError: error_details = {"raw_error": error_payload_str}
                error_msg = f"FunctionError in {lambda_name}: {error_details.get('errorMessage', 'Unknown')}"
                aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "details": error_details})
            else:
                response_body_outer = response_payload.decode('utf-8')
                logger.debug(f"Raw OUTER response body from {lambda_name}: {response_body_outer[:1000]}...")
                outer_payload = json.loads(response_body_outer)

                if isinstance(outer_payload.get("body"), str):
                    response_body_inner = outer_payload["body"]
                    logger.debug(f"INNER body string snippet from {lambda_name}: {response_body_inner[:1000]}...")
                    inner_result_payload = json.loads(response_body_inner)
                    parsed_trends = {
                        "category_summary": inner_result_payload.get("country_category"),
                        "style_details": inner_result_payload.get("country_category_style", []),
                        "color_details": inner_result_payload.get("country_color_category", [])
                    }
                    aggregated_results["trends_data"] = parsed_trends
                    logger.info(f"Successfully parsed INNER response from {lambda_name}.")
                    logger.debug(f"Parsed trends_data snippet: {json.dumps(parsed_trends)[:1000]}...")
                else:
                    error_msg = f"Response from {lambda_name} missing or invalid 'body' field."
                    logger.error(error_msg)
                    logger.debug(f"Outer payload received was: {outer_payload}")
                    aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "details": outer_payload})

        except ClientError as e:
            error_msg = f"Boto3 ClientError invoking {lambda_name}: {e.response['Error']['Code']}"
            logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg})
        except json.JSONDecodeError as e:
             error_msg = f"Failed to parse JSON response from {lambda_name}: {e}"
             raw_payload_to_log = response_body_inner if response_body_inner else (response_body_outer if response_body_outer else 'N/A')
             logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "raw_payload": raw_payload_to_log})
        except Exception as e:
             error_msg = f"Unexpected error during {lambda_name} processing: {str(e)}"
             logger.exception(error_msg); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg})

    if invoke_mega_trends:
        lambda_name = MEGA_TRENDS_LAMBDA_NAME
        logger.info(f"Invoking {lambda_name}...")
        response_body_outer = None
        response_body_inner = None
        try:
            time_frame = map_timeframe_reference(timeframe_reference)
            payload = {"queryStringParameters": {"country": country_name, "category": category_name, "time_frame": time_frame, "next_batch": "Previous", "cat_mode": "True"}}
            logger.debug(f"Payload for {lambda_name}: {json.dumps(payload)}")
            response = lambda_client.invoke(FunctionName=lambda_name, InvocationType='RequestResponse', Payload=json.dumps(payload))
            logger.info(f"Response received from {lambda_name}")

            response_payload = response['Payload'].read()
            if response.get('FunctionError'):
                error_payload_str = response_payload.decode('utf-8')
                logger.error(f"FunctionError received from {lambda_name}: {error_payload_str[:1000]}...")
                try: error_details = json.loads(error_payload_str)
                except json.JSONDecodeError: error_details = {"raw_error": error_payload_str}
                error_msg = f"FunctionError in {lambda_name}: {error_details.get('errorMessage', 'Unknown')}"
                aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "details": error_details})
            else:
                response_body_outer = response_payload.decode('utf-8')
                logger.debug(f"Raw OUTER response body from {lambda_name}: {response_body_outer[:1000]}...")
                outer_payload = json.loads(response_body_outer)
                if isinstance(outer_payload.get("body"), str):
                    response_body_inner = outer_payload["body"]
                    logger.debug(f"INNER body string snippet from {lambda_name}: {response_body_inner[:1000]}...")
                    inner_result_payload = json.loads(response_body_inner)
                    mega_trends_list = inner_result_payload.get("query_category", [])
                    aggregated_results["mega_trends_data"] = mega_trends_list
                    logger.info(f"Successfully parsed INNER response from {lambda_name}. Found {len(mega_trends_list)} mega trend items.")
                    logger.debug(f"Parsed mega_trends_data snippet: {json.dumps(mega_trends_list)[:1000]}...")
                else:
                     error_msg = f"Response from {lambda_name} missing or invalid 'body' field."
                     logger.error(error_msg); logger.debug(f"Outer payload: {outer_payload}")
                     aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "details": outer_payload})

        except ClientError as e:
            error_msg = f"Boto3 ClientError invoking {lambda_name}: {e.response['Error']['Code']}"
            logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg})
        except json.JSONDecodeError as e:
             error_msg = f"Failed to parse JSON response from {lambda_name}: {e}"
             raw_payload_to_log = response_body_inner if response_body_inner else (response_body_outer if response_body_outer else 'N/A')
             logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "raw_payload": raw_payload_to_log})
        except Exception as e:
             error_msg = f"Unexpected error during {lambda_name} processing: {str(e)}"
             logger.exception(error_msg); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg})

    if invoke_charts:
        lambda_name = CHART_DETAILS_LAMBDA_NAME
        first_subject_info = specific_known_subjects[0]
        subject_name = first_subject_info.get("subject")
        subject_type = first_subject_info.get("type")

        if subject_name and subject_type:
            logger.info(f"Invoking {lambda_name} for '{subject_name}' ({subject_type})...")
            response_body_outer = None
            response_body_inner = None
            try:
                time_frame = map_timeframe_reference(timeframe_reference)
                if time_frame not in ["12", "48"]: time_frame = "48"; logger.debug(f"Overriding chart timeframe to '48'")

                # --- CORRECTED category_subject FORMATTING ---
                # Use the subject name directly (which might be "Blue" or "Polo Shirts")
                # Apply title case as requested by downstream Lambda format "Color Category" / "Style Category"
                category_subject_key = safe_title_case(subject_name)
                # If the subject *doesn't* already contain the category name (e.g. it's just "Blue"), append it.
                # We need a reliable way to know if the category name is already part of the subject_name.
                # Simplest approach for now: Assume interpreter gives "Blue" or "Polo Shirts". Append category if needed.
                # A more robust way might be needed if interpreter output varies.
                # Let's assume for now interpreter gives "Blue" or "Polo Shirts" and downstream needs "Blue Shirts" or "Polo Shirts".
                # This check is imperfect but a starting point:
                if category_name.lower() not in subject_name.lower():
                    category_subject_key = f"{safe_title_case(subject_name)} {safe_title_case(category_name)}"
                # --- END CORRECTION ---

                logger.debug(f"Constructed category_subject for chart details: {category_subject_key}")

                forecast_value = "True" if primary_task == "get_forecast" else "False"
                payload = {"queryStringParameters": {"country": country_name, "category_subject": category_subject_key, "category": category_name, "time_frame": time_frame, "mode": subject_type, "forecast": forecast_value}}
                logger.debug(f"Payload for {lambda_name}: {json.dumps(payload)}")
                response = lambda_client.invoke(FunctionName=lambda_name, InvocationType='RequestResponse', Payload=json.dumps(payload))
                logger.info(f"Response received from {lambda_name}")

                response_payload = response['Payload'].read()
                if response.get('FunctionError'):
                    error_payload_str = response_payload.decode('utf-8')
                    logger.error(f"FunctionError received from {lambda_name}: {error_payload_str[:1000]}...")
                    try: error_details = json.loads(error_payload_str)
                    except json.JSONDecodeError: error_details = {"raw_error": error_payload_str}
                    error_msg = f"FunctionError in {lambda_name}: {error_details.get('errorMessage', 'Unknown')}"
                    aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "details": error_details, "subject": subject_name})
                else:
                    response_body_outer = response_payload.decode('utf-8')
                    logger.debug(f"Raw OUTER response body from {lambda_name}: {response_body_outer[:1000]}...")
                    outer_payload = json.loads(response_body_outer)
                    if isinstance(outer_payload.get("body"), str):
                         response_body_inner = outer_payload["body"]
                         logger.debug(f"INNER body string snippet from {lambda_name}: {response_body_inner[:1000]}...")
                         inner_result_payload = json.loads(response_body_inner)
                         aggregated_results["chart_details_data"] = inner_result_payload
                         logger.info(f"Successfully parsed INNER response from {lambda_name} for subject '{subject_name}'.")
                         logger.debug(f"Parsed chart_details_data snippet: {json.dumps(inner_result_payload)[:1000]}...")
                    else:
                         error_msg = f"Response from {lambda_name} missing or invalid 'body' field."
                         logger.error(error_msg); logger.debug(f"Outer payload: {outer_payload}")
                         aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "details": outer_payload, "subject": subject_name})

            except ClientError as e:
                 error_msg = f"Boto3 ClientError invoking {lambda_name}: {e.response['Error']['Code']}"
                 logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "subject": subject_name})
            except json.JSONDecodeError as e:
                 error_msg = f"Failed to parse JSON response from {lambda_name}: {e}"
                 raw_payload_to_log = response_body_inner if response_body_inner else (response_body_outer if response_body_outer else 'N/A')
                 logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "raw_payload": raw_payload_to_log, "subject": subject_name})
            except Exception as e:
                 error_msg = f"Unexpected error during {lambda_name} processing for '{subject_name}': {str(e)}"
                 logger.exception(error_msg); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "subject": subject_name})
        else:
            logger.warning(f"Skipping {lambda_name}: subject/type missing in {first_subject_info}")
            aggregated_results["errors"].append({"source": lambda_name, "error": "Missing subject/type", "details": first_subject_info})

    if not aggregated_results["errors"]:
        attempted_fetch = invoke_trend_main or invoke_mega_trends or invoke_charts
        if attempted_fetch:
             aggregated_results["status"] = "success"
             logger.info("Internal data fetch step completed successfully.")
        else:
             aggregated_results["status"] = "success_noop"
             logger.info("No internal data fetch required by interpretation.")
    else:
        logger.warning(f"Internal data fetch step completed with {len(aggregated_results['errors'])} error(s). Status: partial.")

    aggregated_results["interpretation"] = interpretation_result
    logger.info(f"Returning aggregated results (Status: {aggregated_results['status']}).")
    final_keys = list(aggregated_results.keys())
    data_presence = {
        "trends_data_present": aggregated_results["trends_data"] is not None,
        "mega_trends_data_present": aggregated_results["mega_trends_data"] is not None,
        "chart_details_data_present": aggregated_results["chart_details_data"] is not None
    }
    logger.debug(f"Final aggregated_results structure: Keys={final_keys}, DataPresence={data_presence}, Errors={len(aggregated_results['errors'])}")

    return aggregated_results