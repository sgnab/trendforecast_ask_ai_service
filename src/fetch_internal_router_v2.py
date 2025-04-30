# src/fetch_internal_router_v2.py

import json
import logging
import os
import boto3
from botocore.exceptions import ClientError
import re
from typing import Dict, Optional, List, Any # Added List, Any

# --- Configuration ---
TREND_MAIN_LAMBDA_NAME = os.environ.get("TREND_MAIN_LAMBDA_NAME", "trend_analysis_main_page_placeholder")
MEGA_TRENDS_LAMBDA_NAME = os.environ.get("MEGA_TRENDS_LAMBDA_NAME", "dev_mega_trends_placeholder")
CHART_DETAILS_LAMBDA_NAME = os.environ.get("CHART_DETAILS_LAMBDA_NAME", "chart_details_lambda_placeholder")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")

# --- Initialize Logger ---
logger = logging.getLogger()
log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
if log_level_str not in valid_log_levels: log_level_str = "INFO"
logger.setLevel(log_level_str)
logger.info(f"Logger initialized with level: {log_level_str}")
logger.info(f"Target Lambdas - TrendMain: {TREND_MAIN_LAMBDA_NAME}, MegaTrends: {MEGA_TRENDS_LAMBDA_NAME}, ChartDetails: {CHART_DETAILS_LAMBDA_NAME}")

# --- Initialize Boto3 Lambda Client ---
lambda_client = None
BOTO3_CLIENT_ERROR = None
try:
   session = boto3.session.Session()
   lambda_client = session.client(service_name='lambda', region_name=AWS_REGION)
except Exception as e:
   logger.exception("CRITICAL ERROR initializing Boto3 Lambda client!")
   BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 client: {e}"

# --- Helper Functions ---
def map_timeframe_reference(timeframe_ref_str: str | None) -> str:
    """Maps the textual timeframe reference from interpreter to numerical string."""
    default_value = "12"; logger.debug(f"Mapping timeframe '{timeframe_ref_str}'...")
    if not timeframe_ref_str: logger.debug("Null/empty -> '3'"); return "3"
    tf = timeframe_ref_str.lower()
    if tf in ["latest", "recent", "now"]: logger.debug("Latest/recent -> '3'"); return "3"
    if tf in ["this year", "a year", "last year", "1 year", "12 months"]: logger.debug("Year -> '12'"); return "12"
    if tf in ["historical", "deep historical", "all time", "long term"]: logger.debug("Historical -> '48'"); return "48"
    match = re.search(r'(\d+)\s+month', tf);
    if match: months = match.group(1); logger.debug(f"Regex match -> '{months}'"); return months
    logger.warning(f"Unmapped timeframe '{timeframe_ref_str}', defaulting to '{default_value}'"); return default_value

def safe_title_case(input_string: str) -> str:
    """Converts string to Title Case safely."""
    return input_string.title() if isinstance(input_string, str) else ""

# --- Main Lambda Handler ---
def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    if BOTO3_CLIENT_ERROR: raise Exception(f"Config Error: {BOTO3_CLIENT_ERROR}")

    # 1. Parse Input Event
    try:
        interpretation_result = event
        required_sources = interpretation_result.get("required_sources", [])
        query_subjects = interpretation_result.get("query_subjects", {})
        original_context = interpretation_result.get("original_context", {})
        primary_task = interpretation_result.get("primary_task")
        timeframe_reference = interpretation_result.get("timeframe_reference")
        specific_known_subjects = query_subjects.get("specific_known", []) # List of {"subject": "...", "type": "..."}
        country_name = original_context.get("country")
        category_name = original_context.get("category")
        if not required_sources or not country_name or not category_name:
            raise ValueError("Input missing required fields: 'required_sources', 'original_context.country', or 'original_context.category'")
        logger.info(f"Required: {required_sources}, Subjects: {specific_known_subjects}, Context: {country_name}/{category_name}, Task: {primary_task}, Timeframe: {timeframe_reference}")
    except (TypeError, ValueError, AttributeError, KeyError) as e: # Added KeyError
        logger.error(f"Failed to parse input event: {e}", exc_info=True)
        raise ValueError(f"Invalid input structure: {e}")

    # 2. Initialize results structure
    aggregated_results = {
        "status": "partial",
        "trends_data": None,       # Will hold parsed trend_analysis_main output
        "mega_trends_data": None,  # Will hold parsed dev_mega_trends output (list)
        "chart_details_data": None,# Will hold parsed chart_details output (object or list if parallel later)
        "errors": []
    }

    # 3. Determine which Lambdas to invoke
    invoke_trend_main = "internal_trends_item" in required_sources or "internal_trends_category" in required_sources
    invoke_mega_trends = "internal_mega" in required_sources
    invoke_charts = bool(specific_known_subjects)

    # --- Invoke Trend Analysis Lambda ---
    if invoke_trend_main:
        lambda_name = TREND_MAIN_LAMBDA_NAME
        logger.info(f"Invoking {lambda_name}...")
        try:
            time_frame = map_timeframe_reference(timeframe_reference)
            payload = {"queryStringParameters": {"country": country_name, "category": category_name, "time_frame": time_frame}}
            logger.debug(f"Payload for {lambda_name}: {json.dumps(payload)}")
            response = lambda_client.invoke(FunctionName=lambda_name, InvocationType='RequestResponse', Payload=json.dumps(payload))
            logger.info(f"Response received from {lambda_name}")

            if response.get('FunctionError'):
                error_payload = json.loads(response['Payload'].read().decode('utf-8'))
                error_msg = f"FunctionError in {lambda_name}: {error_payload.get('errorMessage', 'Unknown')}"
                logger.error(error_msg)
                aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "details": error_payload})
            else:
                # --- PARSE Trend Analysis Response ---
                response_body = response['Payload'].read().decode('utf-8')
                logger.debug(f"Raw response body from {lambda_name}: {response_body[:500]}...") # Log snippet
                result_payload = json.loads(response_body)

                # Extract key data - adjust based on exact needs for final synthesis
                parsed_trends = {
                    "category_summary": result_payload.get("country_category"), # Keep full summary obj
                    "style_details": result_payload.get("country_category_style", []), # Keep list of styles
                    "color_details": result_payload.get("country_color_category", [])  # Keep list of colors
                }
                # Example: Filtering/simplifying (optional)
                # parsed_trends["style_details"] = [
                #     {"style_name": s.get("style_name"), "avg_vol": s.get("average_volume"), "growth": s.get("growth_recent")}
                #     for s in result_payload.get("country_category_style", [])
                # ][:10] # Keep top 10 styles summary
                # parsed_trends["color_details"] = [
                #     {"color_name": c.get("color_name"), "avg_vol": c.get("average_volume"), "growth": c.get("growth_recent")}
                #     for c in result_payload.get("country_color_category", [])
                # ][:10] # Keep top 10 colors summary

                aggregated_results["trends_data"] = parsed_trends
                logger.info(f"Successfully parsed response from {lambda_name}.")
                logger.debug(f"Parsed trends_data: {json.dumps(parsed_trends)}")

        except ClientError as e:
            error_msg = f"Boto3 ClientError invoking {lambda_name}: {e.response['Error']['Code']}"
            logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg})
        except json.JSONDecodeError as e:
             error_msg = f"Failed to parse JSON response from {lambda_name}: {e}"
             logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "raw_payload": response_body if 'response_body' in locals() else 'N/A'})
        except Exception as e:
             error_msg = f"Unexpected error during {lambda_name} processing: {str(e)}"
             logger.exception(error_msg); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg})

    # --- Invoke Mega Trends Lambda ---
    if invoke_mega_trends:
        lambda_name = MEGA_TRENDS_LAMBDA_NAME
        logger.info(f"Invoking {lambda_name}...")
        try:
            time_frame = map_timeframe_reference(timeframe_reference)
            payload = {"queryStringParameters": {"country": country_name, "category": category_name, "time_frame": time_frame, "next_batch": "Previous", "cat_mode": "False"}}
            logger.debug(f"Payload for {lambda_name}: {json.dumps(payload)}")
            response = lambda_client.invoke(FunctionName=lambda_name, InvocationType='RequestResponse', Payload=json.dumps(payload))
            logger.info(f"Response received from {lambda_name}")

            if response.get('FunctionError'):
                error_payload = json.loads(response['Payload'].read().decode('utf-8'))
                error_msg = f"FunctionError in {lambda_name}: {error_payload.get('errorMessage', 'Unknown')}"
                logger.error(error_msg); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "details": error_payload})
            else:
                # --- PARSE Mega Trends Response ---
                response_body = response['Payload'].read().decode('utf-8')
                logger.debug(f"Raw response body from {lambda_name}: {response_body[:500]}...")
                result_payload = json.loads(response_body)
                # Extract the list of trending queries
                mega_trends_list = result_payload.get("query_category", [])
                aggregated_results["mega_trends_data"] = mega_trends_list # Store the list directly
                logger.info(f"Successfully parsed response from {lambda_name}. Found {len(mega_trends_list)} mega trend items.")
                logger.debug(f"Parsed mega_trends_data: {json.dumps(mega_trends_list)}")

        except ClientError as e:
            error_msg = f"Boto3 ClientError invoking {lambda_name}: {e.response['Error']['Code']}"
            logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg})
        except json.JSONDecodeError as e:
             error_msg = f"Failed to parse JSON response from {lambda_name}: {e}"
             logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "raw_payload": response_body if 'response_body' in locals() else 'N/A'})
        except Exception as e:
             error_msg = f"Unexpected error during {lambda_name} processing: {str(e)}"
             logger.exception(error_msg); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg})

    # --- Invoke Chart Details Lambda ---
    # Note: Still only processes the *first* specific subject.
    if invoke_charts:
        lambda_name = CHART_DETAILS_LAMBDA_NAME
        first_subject_info = specific_known_subjects[0]
        subject_name = first_subject_info.get("subject")
        subject_type = first_subject_info.get("type")

        if subject_name and subject_type:
            logger.info(f"Invoking {lambda_name} for '{subject_name}' ({subject_type})...")
            try:
                time_frame = map_timeframe_reference(timeframe_reference)
                if time_frame not in ["12", "48"]: time_frame = "48"; logger.debug(f"Overriding chart timeframe to '48'")
                formatted_subject_name = safe_title_case(subject_name).replace(' ', '_')
                category_subject_key = f"{safe_title_case(category_name)}_{formatted_subject_name}"
                forecast_value = "True" if primary_task == "get_forecast" else "False"
                payload = {"queryStringParameters": {"country": country_name, "category_subject": category_subject_key, "category": category_name, "time_frame": time_frame, "mode": subject_type, "forecast": forecast_value}}
                logger.debug(f"Payload for {lambda_name}: {json.dumps(payload)}")
                response = lambda_client.invoke(FunctionName=lambda_name, InvocationType='RequestResponse', Payload=json.dumps(payload))
                logger.info(f"Response received from {lambda_name}")

                if response.get('FunctionError'):
                    error_payload = json.loads(response['Payload'].read().decode('utf-8'))
                    error_msg = f"FunctionError in {lambda_name}: {error_payload.get('errorMessage', 'Unknown')}"
                    logger.error(error_msg); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "details": error_payload, "subject": subject_name})
                else:
                    # --- PARSE Chart Details Response ---
                    response_body = response['Payload'].read().decode('utf-8')
                    logger.debug(f"Raw response body from {lambda_name}: {response_body[:500]}...")
                    result_payload = json.loads(response_body)
                    # Store the entire result object for this specific item
                    aggregated_results["chart_details_data"] = result_payload
                    logger.info(f"Successfully parsed response from {lambda_name} for subject '{subject_name}'.")
                    logger.debug(f"Parsed chart_details_data: {json.dumps(result_payload)}")

            except ClientError as e:
                 error_msg = f"Boto3 ClientError invoking {lambda_name}: {e.response['Error']['Code']}"
                 logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "subject": subject_name})
            except json.JSONDecodeError as e:
                 error_msg = f"Failed to parse JSON response from {lambda_name}: {e}"
                 logger.error(error_msg, exc_info=True); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "raw_payload": response_body if 'response_body' in locals() else 'N/A', "subject": subject_name})
            except Exception as e:
                 error_msg = f"Unexpected error during {lambda_name} processing for '{subject_name}': {str(e)}"
                 logger.exception(error_msg); aggregated_results["errors"].append({"source": lambda_name, "error": error_msg, "subject": subject_name})
        else:
            logger.warning(f"Skipping {lambda_name}: subject/type missing in {first_subject_info}")
            aggregated_results["errors"].append({"source": lambda_name, "error": "Missing subject/type", "details": first_subject_info})

    # 4. Finalize Status & Return Results
    if not aggregated_results["errors"]:
        # Check if at least one data fetch was attempted and successful (modify as needed)
        attempted_fetch = invoke_trend_main or invoke_mega_trends or invoke_charts
        if attempted_fetch: # Only success if something was actually invoked without error
             aggregated_results["status"] = "success"
             logger.info("Internal data fetch step completed successfully.")
        else: # Nothing needed to be invoked
             aggregated_results["status"] = "success_noop" # Indicate nothing needed fetching
             logger.info("No internal data fetch required by interpretation.")
    else:
        logger.warning(f"Internal data fetch step completed with {len(aggregated_results['errors'])} error(s). Status: partial.")
        # Status remains "partial"

    aggregated_results["interpretation"] = interpretation_result
    logger.info(f"Returning aggregated results (Status: {aggregated_results['status']}).")
    logger.debug(f"Final payload: {json.dumps(aggregated_results)}")
    return aggregated_results