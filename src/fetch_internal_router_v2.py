import json
import logging
import os
import boto3
from botocore.exceptions import ClientError
import re # Needed for time frame parsing

# --- Configuration ---
TREND_MAIN_LAMBDA_NAME = os.environ.get("TREND_MAIN_LAMBDA_NAME", "trend_analysis_main_page_placeholder")
MEGA_TRENDS_LAMBDA_NAME = os.environ.get("MEGA_TRENDS_LAMBDA_NAME", "dev_mega_trends_placeholder")
CHART_DETAILS_LAMBDA_NAME = os.environ.get("CHART_DETAILS_LAMBDA_NAME", "chart_details_lambda_placeholder")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")

# --- Initialize Logger ---
logger = logging.getLogger()
log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
if log_level_str not in valid_log_levels:
   log_level_str = "INFO"
logger.setLevel(log_level_str)
logger.info(f"Logger initialized with level: {log_level_str}")
# Log lambda names during init for easier debugging deployment issues
logger.info(f"Target Lambdas - TrendMain: {TREND_MAIN_LAMBDA_NAME}, MegaTrends: {MEGA_TRENDS_LAMBDA_NAME}, ChartDetails: {CHART_DETAILS_LAMBDA_NAME}")


# --- Initialize Boto3 Lambda Client ---
lambda_client = None
BOTO3_CLIENT_ERROR = None
try:
   session = boto3.session.Session()
   lambda_client = session.client(
       service_name='lambda',
       region_name=AWS_REGION
   )
except Exception as e:
   logger.exception("CRITICAL ERROR initializing Boto3 Lambda client!")
   BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 client: {e}"

# --- Helper Functions ---
def map_timeframe_reference(timeframe_ref_str: str | None) -> str:
    """Maps the textual timeframe reference from interpreter to numerical string."""
    default_value = "12" # Default to 12 months if unclear

    if not timeframe_ref_str:
        logger.debug("Timeframe reference is null/empty, defaulting to '3'")
        return "3" # Use 3 for null/empty, often implies 'latest'

    timeframe_lower = timeframe_ref_str.lower()

    # Simple keyword matching
    if timeframe_lower in ["latest", "recent", "now"]:
        logger.debug(f"Mapping timeframe '{timeframe_ref_str}' to '3'")
        return "3"
    elif timeframe_lower in ["this year", "a year", "last year", "1 year", "12 months"]:
         logger.debug(f"Mapping timeframe '{timeframe_ref_str}' to '12'")
         return "12"
    elif timeframe_lower in ["historical", "deep historical", "all time", "long term"]:
         logger.debug(f"Mapping timeframe '{timeframe_ref_str}' to '48'")
         return "48"

    # Optional: More complex parsing (e.g., "last 6 months")
    match = re.search(r'(\d+)\s+month', timeframe_lower)
    if match:
        months = match.group(1)
        logger.debug(f"Mapping timeframe '{timeframe_ref_str}' to '{months}' based on regex")
        return months # Return the extracted number of months

    # Fallback if no clear mapping
    logger.warning(f"Could not map timeframe reference '{timeframe_ref_str}', defaulting to '{default_value}'")
    return default_value

def safe_title_case(input_string: str) -> str:
    """Converts string to Title Case safely (handles potential errors)."""
    if not isinstance(input_string, str):
        return "" # Return empty if not a string
    # Simple title() might be sufficient if input is clean
    # For more complex cases with mixed casing or punctuation, more robust logic might be needed
    return input_string.title()

# --- Main Lambda Handler ---
def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    if BOTO3_CLIENT_ERROR:
        logger.error(f"Returning error due to Boto3 init failure: {BOTO3_CLIENT_ERROR}")
        raise Exception(f"Configuration Error: {BOTO3_CLIENT_ERROR}") # Fail fast

    # 1. Parse Input Event
    try:
        interpretation_result = event
        required_sources = interpretation_result.get("required_sources", [])
        query_subjects = interpretation_result.get("query_subjects", {})
        original_context = interpretation_result.get("original_context", {})
        primary_task = interpretation_result.get("primary_task")
        timeframe_reference = interpretation_result.get("timeframe_reference") # Get text reference

        # Expecting specific_known to be a list of objects like {"subject": "Blue", "type": "color"}
        specific_known_subjects = query_subjects.get("specific_known", [])

        # Extract country/category - Assuming they are provided correctly formatted
        country_name = original_context.get("country")
        category_name = original_context.get("category")

        if not required_sources or not country_name or not category_name:
             raise ValueError("Input missing required fields: 'required_sources', 'original_context.country', or 'original_context.category'")

        logger.info(f"Required sources: {required_sources}")
        logger.info(f"Query subjects: {query_subjects}")
        logger.info(f"Specific known subjects objects: {specific_known_subjects}")
        logger.info(f"Original context: Country='{country_name}', Category='{category_name}'")
        logger.info(f"Primary task: {primary_task}")
        logger.info(f"Timeframe reference: {timeframe_reference}")

    except (TypeError, ValueError, AttributeError) as e:
        logger.error(f"Failed to parse input event: {e}", exc_info=True)
        raise ValueError(f"Invalid input structure: {e}") # Fail fast

    # 2. Initialize results structure
    aggregated_results = {
        "status": "partial",
        "trends_data": None,
        "mega_trends_data": None,
        "chart_details_data": None, # Will store result of first chart call for now
        # "chart_details_data_list": [], # Use this if implementing multiple chart calls later
        "errors": []
    }

    # 3. Determine which Lambdas to invoke
    invoke_trend_main = "internal_trends_item" in required_sources or "internal_trends_category" in required_sources
    invoke_mega_trends = "internal_mega" in required_sources
    # Trigger chart details if specific known subjects list is not empty
    invoke_charts = bool(specific_known_subjects) # True if list is not empty

    # --- Invoke Trend Analysis Lambda ---
    if invoke_trend_main:
        logger.info(f"Condition met: Preparing to invoke {TREND_MAIN_LAMBDA_NAME}")
        try:
            time_frame_for_trend = map_timeframe_reference(timeframe_reference) # Use helper
            trend_payload = {
                "queryStringParameters": {
                    "country": country_name,
                    "category": category_name,
                    "time_frame": time_frame_for_trend
                }
            }
            logger.debug(f"Payload for {TREND_MAIN_LAMBDA_NAME}: {json.dumps(trend_payload)}")

            response = lambda_client.invoke(
                FunctionName=TREND_MAIN_LAMBDA_NAME,
                InvocationType='RequestResponse',
                Payload=json.dumps(trend_payload)
            )
            logger.info(f"Response received from {TREND_MAIN_LAMBDA_NAME}")

            if response.get('FunctionError'):
                error_payload = json.loads(response['Payload'].read())
                error_msg = f"FunctionError in {TREND_MAIN_LAMBDA_NAME}: {error_payload.get('errorMessage', 'Unknown error')}"
                logger.error(error_msg)
                aggregated_results["errors"].append({"source": TREND_MAIN_LAMBDA_NAME, "error": error_msg, "details": error_payload})
            else:
                result_payload = json.loads(response['Payload'].read())
                logger.debug(f"Result payload from {TREND_MAIN_LAMBDA_NAME} stored.")
                # TODO: Confirm what structure trends_data should have. Storing full payload for now.
                aggregated_results["trends_data"] = result_payload

        except ClientError as e:
             error_msg = f"Boto3 ClientError invoking {TREND_MAIN_LAMBDA_NAME}: {e.response['Error']['Code']}"
             logger.error(error_msg, exc_info=True)
             aggregated_results["errors"].append({"source": TREND_MAIN_LAMBDA_NAME, "error": error_msg})
        except Exception as e:
             error_msg = f"Unexpected error during {TREND_MAIN_LAMBDA_NAME} invocation: {str(e)}"
             logger.exception(error_msg)
             aggregated_results["errors"].append({"source": TREND_MAIN_LAMBDA_NAME, "error": error_msg})

    # --- Invoke Mega Trends Lambda ---
    if invoke_mega_trends:
        logger.info(f"Condition met: Preparing to invoke {MEGA_TRENDS_LAMBDA_NAME}")
        try:
            time_frame_for_mega = map_timeframe_reference(timeframe_reference) # Use helper
            mega_payload = {
                "queryStringParameters": {
                    "country": country_name,
                    "category": category_name,
                    "time_frame": time_frame_for_mega,
                    "next_batch": "Previous", # Fixed value
                    "cat_mode": "False"       # Fixed value
                }
            }
            logger.debug(f"Payload for {MEGA_TRENDS_LAMBDA_NAME}: {json.dumps(mega_payload)}")

            response = lambda_client.invoke(
                FunctionName=MEGA_TRENDS_LAMBDA_NAME,
                InvocationType='RequestResponse',
                Payload=json.dumps(mega_payload)
            )
            logger.info(f"Response received from {MEGA_TRENDS_LAMBDA_NAME}")

            if response.get('FunctionError'):
                error_payload = json.loads(response['Payload'].read())
                error_msg = f"FunctionError in {MEGA_TRENDS_LAMBDA_NAME}: {error_payload.get('errorMessage', 'Unknown error')}"
                logger.error(error_msg)
                aggregated_results["errors"].append({"source": MEGA_TRENDS_LAMBDA_NAME, "error": error_msg, "details": error_payload})
            else:
                result_payload = json.loads(response['Payload'].read())
                logger.debug(f"Result payload from {MEGA_TRENDS_LAMBDA_NAME} stored.")
                # TODO: Confirm what structure mega_trends_data should have. Storing full payload for now.
                aggregated_results["mega_trends_data"] = result_payload

        except ClientError as e:
             error_msg = f"Boto3 ClientError invoking {MEGA_TRENDS_LAMBDA_NAME}: {e.response['Error']['Code']}"
             logger.error(error_msg, exc_info=True)
             aggregated_results["errors"].append({"source": MEGA_TRENDS_LAMBDA_NAME, "error": error_msg})
        except Exception as e:
             error_msg = f"Unexpected error during {MEGA_TRENDS_LAMBDA_NAME} invocation: {str(e)}"
             logger.exception(error_msg)
             aggregated_results["errors"].append({"source": MEGA_TRENDS_LAMBDA_NAME, "error": error_msg})

    # --- Invoke Chart Details Lambda ---
    # Note: Currently only processes the *first* specific subject if multiple exist.
    if invoke_charts:
        first_subject_info = specific_known_subjects[0] # Get the first object e.g., {"subject": "Blue", "type": "color"}
        subject_name = first_subject_info.get("subject")
        subject_type = first_subject_info.get("type") # "color" or "style"

        if subject_name and subject_type:
            logger.info(f"Condition met: Preparing to invoke {CHART_DETAILS_LAMBDA_NAME} for subject '{subject_name}' (type: {subject_type})")
            try:
                # Use 48 for chart details time frame if not specified otherwise by mapping logic
                time_frame_for_chart = map_timeframe_reference(timeframe_reference)
                if time_frame_for_chart not in ["12", "48"]: # Default chart to longer timeframe if not explicit long/historical
                    time_frame_for_chart = "48" # Override default for charts unless specifically 12 or 48
                    logger.debug(f"Overriding chart timeframe to '48' as reference was '{timeframe_reference}'")


                # Construct Category_SubjectName (ensure Title Case)
                # Replace spaces in subject name with underscores for the final key
                formatted_subject_name = safe_title_case(subject_name).replace(' ', '_')
                category_subject_key = f"{safe_title_case(category_name)}_{formatted_subject_name}"

                forecast_value = "True" if primary_task == "get_forecast" else "False"

                chart_payload = {
                    "queryStringParameters": {
                        "country": country_name,
                        "category_subject": category_subject_key,
                        "category": category_name,
                        "time_frame": time_frame_for_chart,
                        "mode": subject_type, # Directly use the type from interpreter output
                        "forecast": forecast_value
                    }
                }
                logger.debug(f"Payload for {CHART_DETAILS_LAMBDA_NAME}: {json.dumps(chart_payload)}")

                response = lambda_client.invoke(
                    FunctionName=CHART_DETAILS_LAMBDA_NAME,
                    InvocationType='RequestResponse',
                    Payload=json.dumps(chart_payload)
                )
                logger.info(f"Response received from {CHART_DETAILS_LAMBDA_NAME}")

                if response.get('FunctionError'):
                    error_payload = json.loads(response['Payload'].read())
                    error_msg = f"FunctionError in {CHART_DETAILS_LAMBDA_NAME}: {error_payload.get('errorMessage', 'Unknown error')}"
                    logger.error(error_msg)
                    aggregated_results["errors"].append({"source": CHART_DETAILS_LAMBDA_NAME, "error": error_msg, "details": error_payload, "subject": subject_name})
                else:
                    result_payload = json.loads(response['Payload'].read())
                    logger.debug(f"Result payload from {CHART_DETAILS_LAMBDA_NAME} stored.")
                    # TODO: Confirm what structure chart_details_data should have. Storing full payload for now.
                    aggregated_results["chart_details_data"] = result_payload # Store result for the first subject

            except ClientError as e:
                 error_msg = f"Boto3 ClientError invoking {CHART_DETAILS_LAMBDA_NAME}: {e.response['Error']['Code']}"
                 logger.error(error_msg, exc_info=True)
                 aggregated_results["errors"].append({"source": CHART_DETAILS_LAMBDA_NAME, "error": error_msg, "subject": subject_name})
            except Exception as e:
                 error_msg = f"Unexpected error during {CHART_DETAILS_LAMBDA_NAME} invocation for '{subject_name}': {str(e)}"
                 logger.exception(error_msg)
                 aggregated_results["errors"].append({"source": CHART_DETAILS_LAMBDA_NAME, "error": error_msg, "subject": subject_name})
        else:
            logger.warning(f"Could not invoke {CHART_DETAILS_LAMBDA_NAME} because subject name or type was missing in specific_known object: {first_subject_info}")
            aggregated_results["errors"].append({"source": CHART_DETAILS_LAMBDA_NAME, "error": "Missing subject name or type in interpreter output", "details": first_subject_info})


    # 4. Finalize Status & Return Results
    if not aggregated_results["errors"]:
        aggregated_results["status"] = "success"
        logger.info("Internal data fetch step completed without errors.")
    else:
        logger.warning(f"Internal data fetching encountered errors: {len(aggregated_results['errors'])} error(s). See 'errors' list for details.")

    # Add original interpretation back for context downstream (e.g., for final response generation)
    aggregated_results["interpretation"] = interpretation_result

    logger.info(f"Returning aggregated results.")
    logger.debug(f"Final payload: {json.dumps(aggregated_results)}") # Log full final payload at debug level
    return aggregated_results