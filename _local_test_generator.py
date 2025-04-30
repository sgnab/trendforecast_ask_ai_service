# _local_test_generator.py
import json
import logging
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# --- Setup Project Root and Add src to Path ---
project_root = Path(__file__).resolve().parent
src_path = project_root / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))
    print(f"Added {src_path} to sys.path")

# --- Load .env for local environment variables ---
try:
    from dotenv import load_dotenv
    dotenv_path = project_root / '.env'
    if dotenv_path.is_file():
        print(f"Loading .env file from: {dotenv_path}")
        load_dotenv(dotenv_path=dotenv_path, override=True)
    else:
        print("No .env file found, relying on system environment variables.")
except ImportError:
    print("python-dotenv not installed, relying on system environment variables.")

# --- Configure Logging ---
log_level = os.environ.get("LOG_LEVEL", "DEBUG").upper() # Default to DEBUG for testing
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("LocalTestGenerator")

# --- Set Environment Variables for the Lambda ---
os.environ['SECRET_NAME'] = os.environ.get('SECRET_NAME', 'YourSecretsName')
# Use the agreed-upon default, check if correct ID is known
os.environ['SYNTHESIS_LLM_MODEL'] = os.environ.get('SYNTHESIS_LLM_MODEL', 'gemini-1.5-flash-latest') # Use Flash default
os.environ['AWS_REGION'] = os.environ.get('AWS_REGION', 'us-west-2')
os.environ['IS_LOCAL'] = 'true' # Enable direct reading of GOOGLE_API_KEY from .env
# Ensure GOOGLE_API_KEY is set in your .env file

logger.info(f"Using Secret Name: {os.environ['SECRET_NAME']}")
logger.info(f"Using Synthesis Model: {os.environ['SYNTHESIS_LLM_MODEL']}")
logger.info(f"Using AWS Region: {os.environ['AWS_REGION']}")
logger.info(f"IS_LOCAL set to: {os.environ['IS_LOCAL']}")
if os.environ.get('GOOGLE_API_KEY'):
    logger.info("GOOGLE_API_KEY environment variable found.")
else:
    logger.warning("GOOGLE_API_KEY environment variable NOT found. Ensure it's in .env for local testing.")


# --- Import Handler AFTER setting environment variables ---
try:
    from generate_final_response_v2 import lambda_handler
except ImportError as e: # Handle SDK missing
    if "google.generativeai" in str(e).lower():
         logger.error(f"Error importing handler: {e}. Is google-generativeai installed?", exc_info=True)
    else:
        logger.error(f"Error importing handler: {e}", exc_info=True)
    sys.exit(1)
except Exception as e:
    logger.error(f"Error during import or initial module load: {e}", exc_info=True)
    sys.exit(1)


# --- Define Sample Combined Input Event ---
# Mimics the expected input structure for GenerateFinalResponse_V2
# Use data similar to previous tests where applicable
test_event_data = {
  "internal_data": {
    "status": "success",
    "trends_data": { # Sample parsed data from trend_analysis_main_page
      "category_summary": {
        "category_name": "Shirts", "growth_recent": -18.92, "average_volume": 157000,
        "chart_data": [{"date": "2024-12-28", "search_volume": 201000}, {"date": "2025-01-28", "search_volume": 135000}]
      },
      "style_details": [
        {"style_name": "Classic Shirts", "average_volume": 1767, "growth_recent": 122, "chart_data": [{"date": "2024-12-28", "search_volume": 1900}]},
        {"style_name": "Preppy Shirts", "average_volume": 16433, "growth_recent": 187, "chart_data": [{"date": "2024-12-28", "search_volume": 4400}]}
      ],
      "color_details": [
         {"color_name": "Blue Shirts", "average_volume": 15900, "growth_recent": 18, "chart_data": [{"date": "2024-12-28", "search_volume": 18100}]},
         {"color_name": "Red Shirts", "average_volume": 31100, "growth_recent": 0, "chart_data": [{"date": "2024-12-28", "search_volume": 33100}]}
      ]
    },
    "mega_trends_data": [ # Sample parsed data from dev_mega_trends
      {"query_name": "Sustainable Fabrics", "category_name": "General", "category_subject": "General_Sustainable Fabrics", "average_volume": 5000, "growth_recent": 250, "chart_data": [] }, # <<< Use [] instead of ...
      {"query_name": "Y2K Revival Jeans", "category_name": "Jeans", "category_subject": "Jeans_Y2K Revival Jeans", "average_volume": 8000, "growth_recent": 300, "chart_data": [] } # <<< Use [] instead of ...
    ],
    "chart_details_data": { # Sample parsed data from chart_details_lambda for "Blue Shirts"
      "category_name": "Shirts", "category_subject": "Shirts_Blue",
      "f2": 15, "f3": 20, "f6": 25, "avg2": 18000, "avg3": 19000, "avg6": 20000,
      "growth_recent": 18, "average_volume": 15900,
      "chart_data": [{"date": "2024-12-28", "search_volume": 18100}, {"date": "2025-01-28", "search_volume": 14800}]
    },
    "errors": [],
    "interpretation": {
      "status": "success",
      "primary_task": "get_trend",
      "required_sources": ["internal_trends_category", "internal_trends_item", "web_search"],
      "query_subjects": {"specific_known": [{"subject": "Blue", "type": "color"}], "unmapped_items": ["shirts"]},
      "timeframe_reference": None,
      "attributes": [],
      "clarification_needed": None,
      "original_context": {"category": "Shirts", "country": "United States", "query": "are blue shirts trending"}
    }
  },
  "external_data": {
    "status": "success_api",
    "query_used": "are blue shirts trending category: Shirts country: United States specific items: Blue",
    "answer": "Blue shirts are seeing stable interest according to recent web data, particularly lighter shades for spring.",
    "results": [
        {"title": "Latest Blue Shirt Trends - FashionSite", "url": "http://fashionsite.example/blue-shirts", "content": "Light blue and denim shirts remain popular..."},
        {"title": "Color Report Spring 2025 - StyleNews", "url": "http://stylenews.example/color-report", "content": "Pastel blue tones gain traction..."}
    ],
    "error": None
  }
}

# --- Mock Gemini Client ---
mock_gemini_model_instance = MagicMock()
mock_gemini_response = MagicMock()
mock_gemini_response.text = """
## Blue Shirts Trend Analysis (United States)

**Summary:** Blue shirts show stable demand with moderate recent growth (18%). While overall shirt category volume is high (157k avg), blue shirts specifically have a significant volume (15.9k avg). Web context suggests lighter blue tones are currently favored.

**Key Observations:**
*   **Internal Data:** Shows 18% recent growth for 'Blue Shirts' specifically. Average search volume is 15,900.
*   **Web Context:** Tavily's search indicates stable interest, highlighting lighter shades and denim shirts. StyleNews notes pastel blue gaining traction for Spring 2025.

**Outlook:** Expect continued stable demand for blue shirts. Consider focusing on pastel and light blue variations for upcoming seasons based on external signals.

*(Disclaimer: Based on provided data.)*
""" # Example markdown response
mock_gemini_model_instance.generate_content.return_value = mock_gemini_response

# Patch the GenerativeModel constructor
@patch('generate_final_response_v2.genai.GenerativeModel', return_value=mock_gemini_model_instance)
def run_test(mock_generative_model_constructor):
    logger.info(" === Calling generate_final_response_v2.lambda_handler locally === ")
    # Reset mock calls for generate_content before each run
    mock_gemini_model_instance.generate_content.reset_mock()

    try:
        result = lambda_handler(test_event_data, None) # Pass combined data directly
        logger.info(" === lambda_handler finished === ")

        # --- Print the Result ---
        print("\n----- Lambda Result (Body) -----")
        try:
            # Try to pretty-print the JSON body
            body_json = json.loads(result.get('body', '{}'))
            print(json.dumps(body_json, indent=2))
        except:
            # Print raw body if not JSON or parse fails
            print(result.get('body'))
        print("-----------------------------")

        # --- Verify Mock LLM Call ---
        print("\n----- Mock LLM Call Verification -----")
        print(f"Gemini Model Instantiated: {mock_generative_model_constructor.call_count > 0}")
        if mock_generative_model_constructor.call_count > 0:
             print(f"  Model Name Used: {mock_generative_model_constructor.call_args.args[0]}") # Print the model name passed

        print(f"generate_content called: {mock_gemini_model_instance.generate_content.call_count > 0}")
        if mock_gemini_model_instance.generate_content.call_count > 0:
            # Print the prompt sent to the LLM
            prompt_sent = mock_gemini_model_instance.generate_content.call_args.args[0]
            print("\n--- Prompt Sent to Synthesis LLM ---")
            print(prompt_sent)
            print("------------------------------------")
        print("----------------------------------")


    except Exception as handler_err:
        logger.exception("Exception occurred during lambda_handler execution!")
        print("\n----- Lambda Execution Error -----")
        print(handler_err)
        print("-----------------------------")

# --- Execute Test ---
if __name__ == "__main__":
    # You can modify test_event_data['internal_data']['interpretation']['primary_task']
    # here to test different personas before calling run_test()
    # e.g., test_event_data['internal_data']['interpretation']['primary_task'] = "get_forecast"
    run_test()