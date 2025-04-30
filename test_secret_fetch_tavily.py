import boto3
import json
import os
from botocore.exceptions import ClientError

# --- Configuration ---
SECRET_ID = "gemini_tavily"  # The name or ARN of your secret
REGION_NAME = "us-west-2"    # The region where the secret is stored
KEY_TO_EXTRACT = "TAVILY_API_KEY" # <<< CHANGED TO TAVILY KEY

# --- Initialize Boto3 Client ---
# Ensure your environment is configured with credentials for an IAM user
# who has secretsmanager:GetSecretValue permission on SECRET_ID in REGION_NAME
session = boto3.session.Session()
secrets_manager = session.client(
    service_name='secretsmanager',
    region_name=REGION_NAME
)

print(f"Attempting to fetch secret '{SECRET_ID}' from region '{REGION_NAME}'...")

try:
    # --- Call Secrets Manager ---
    get_secret_value_response = secrets_manager.get_secret_value(
        SecretId=SECRET_ID
    )

    # --- Extract the Secret String or Binary ---
    secret_string = None
    if 'SecretString' in get_secret_value_response:
        secret_string = get_secret_value_response['SecretString']
        print("SecretString found.")
    # Add binary handling if needed...
    else:
        print(f"ERROR: SecretString not found in response for '{SECRET_ID}'.")
        exit(1)

    # --- Parse the JSON Secret ---
    print("Attempting to parse secret as JSON...")
    try:
        secret_dict = json.loads(secret_string)
        print("Successfully parsed secret JSON.")
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse SecretString as JSON: {e}")
        exit(1)

    # --- Extract the Specific Key ---
    print(f"Attempting to extract key '{KEY_TO_EXTRACT}' from JSON...")
    if KEY_TO_EXTRACT in secret_dict:
        extracted_value = secret_dict[KEY_TO_EXTRACT]
        print("-" * 30)
        print(f"SUCCESS: Found key '{KEY_TO_EXTRACT}'!")
        print(f"Type of value: {type(extracted_value)}")
        if isinstance(extracted_value, str):
            print(f"Length of value: {len(extracted_value)}")
            # Mask most of the key for security
            masked_value = extracted_value[:4] + '...' + extracted_value[-4:] if len(extracted_value) > 8 else extracted_value[:4] + '...'
            print(f"Value: '{masked_value}' (masked)")
        else:
            print(f"Value: {extracted_value} (non-string)")
        print("-" * 30)

    else:
        print(f"ERROR: Key '{KEY_TO_EXTRACT}' NOT FOUND within the JSON structure of secret '{SECRET_ID}'.")
        print(f"Available keys: {list(secret_dict.keys())}")
        exit(1)

except ClientError as e:
    error_code = e.response.get("Error", {}).get("Code")
    error_message = e.response.get("Error", {}).get("Message")
    request_id = e.response.get("ResponseMetadata", {}).get("RequestId")
    print(f"\nAWS API ERROR: {error_code} - {error_message} (Request ID: {request_id})")
    # Add troubleshooting tips as before...
    exit(1)
except Exception as e:
    print(f"\nAn unexpected non-AWS error occurred: {e}")
    exit(1)

# --- Optional: Test Tavily API Call ---
if 'extracted_value' in locals() and isinstance(extracted_value, str):
    print("\nAttempting simple Tavily API call to verify key...")
    try:
        from tavily import TavilyClient
        client = TavilyClient(api_key=extracted_value)
        # Perform a very basic, low-cost search
        response = client.search(query="test", max_results=1)
        print("Tavily API call successful!")
        print(f"Tavily Response Snippet: {str(response)[:200]}...")
    except ImportError:
         print("SKIPPED Tavily API call: tavily-python SDK not installed.")
    except Exception as tavily_err:
         print(f"ERROR during Tavily API call: {tavily_err}")
         print("Check if the TAVILY_API_KEY is valid and active.")