import boto3
import json
import os
import getpass # To securely prompt for the secret key
from botocore.exceptions import ClientError

# --- Configuration ---
SECRET_ID = "gemini_tavily"  # The name or ARN of your secret
REGION_NAME = "us-west-2"    # The region where the secret is stored
KEY_TO_EXTRACT = "GOOGLE_API_KEY" # The specific key within the JSON secret

# --- Get Credentials Explicitly ---
print("-" * 30)
print("⚠️ WARNING: Entering AWS credentials directly.")
print("Ensure you are using credentials for a limited IAM user, NOT the root user.")
print("-" * 30)

# Check if running interactively before prompting
if os.isatty(0): # Check if stdin is connected to a TTY (terminal)
    aws_access_key_id = input("Enter AWS Access Key ID: ").strip()
    aws_secret_access_key = getpass.getpass("Enter AWS Secret Access Key: ").strip() # Use getpass for secret key
else:
    print("ERROR: Cannot prompt for credentials in a non-interactive environment.")
    print("Please configure credentials using standard methods (e.g., environment variables).")
    exit(1)


if not aws_access_key_id or not aws_secret_access_key:
    print("ERROR: Both Access Key ID and Secret Access Key are required.")
    exit(1)

print("Credentials received. Proceeding...")
print("-" * 30)

# --- Initialize Boto3 Client with Explicit Credentials ---
# Pass the collected credentials directly to the session constructor
try:
    session = boto3.session.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=REGION_NAME  # Region can also be set here
    )
    secrets_manager = session.client(service_name='secretsmanager')
    # Note: region_name specified in Session takes precedence over client
    #       but specifying it in both is okay for clarity.
    # secrets_manager = session.client(
    #     service_name='secretsmanager',
    #     region_name=REGION_NAME # Can specify here too
    # )

except Exception as e:
    print(f"ERROR: Failed to initialize Boto3 session/client: {e}")
    exit(1)

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
    elif 'SecretBinary' in get_secret_value_response:
        print("SecretBinary found, attempting to decode as UTF-8 JSON.")
        try:
            secret_string = get_secret_value_response['SecretBinary'].decode('utf-8')
        except UnicodeDecodeError:
            print(f"ERROR: SecretBinary for '{SECRET_ID}' could not be decoded as UTF-8.")
            exit(1)
    else:
        print(f"ERROR: Neither SecretString nor SecretBinary found in response for '{SECRET_ID}'.")
        exit(1)

    # --- Parse the JSON Secret ---
    print("Attempting to parse secret as JSON...")
    try:
        secret_dict = json.loads(secret_string)
        print("Successfully parsed secret JSON.")
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse SecretString as JSON for '{SECRET_ID}'.")
        print(f"JSONDecodeError: {e}")
        print("Please ensure the secret value is valid JSON.")
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
            print(f"Value starts with: '{extracted_value[:4]}...' (masked)")
        else:
            print(f"Value: {extracted_value} (masked for non-string types)")
        print("-" * 30)

    else:
        print(f"ERROR: Key '{KEY_TO_EXTRACT}' NOT FOUND within the JSON structure of secret '{SECRET_ID}'.")
        print(f"Available keys: {list(secret_dict.keys())}")
        exit(1)

except ClientError as e:
    error_code = e.response.get("Error", {}).get("Code")
    error_message = e.response.get("Error", {}).get("Message")
    request_id = e.response.get("ResponseMetadata", {}).get("RequestId")

    print(f"\nAWS API ERROR occurred:")
    print(f"  Error Code:    {error_code}")
    print(f"  Error Message: {error_message}")
    print(f"  Request ID:    {request_id}")
    print(f"  Secret ID:     {SECRET_ID}")
    print(f"  Region:        {REGION_NAME}")

    # Troubleshooting tips remain the same...
    if error_code == 'ResourceNotFoundException':
        print("\nTroubleshooting:")
        print(f" - Verify the secret named or ARN '{SECRET_ID}' exists in the '{REGION_NAME}' region.")
    elif error_code == 'AccessDeniedException':
        print("\nTroubleshooting:")
        print(" - Verify the IAM User whose credentials you entered has 'secretsmanager:GetSecretValue' permission.")
        print(f" - Ensure the permission policy allows access specifically to secret '{SECRET_ID}' (or its ARN).")
        print(" - Check for any SCPs (Service Control Policies) that might be blocking access.")
    # ...(other error handling remains the same)...
    else:
        print("\nTroubleshooting:")
        print(" - Check the entered credentials.")
        print(" - Check network connectivity to the AWS Secrets Manager endpoint.")
        print(f" - Consult the AWS documentation for error code: {error_code}")

    exit(1)

except Exception as e:
    print(f"\nAn unexpected non-AWS error occurred: {e}")
    print("Check the script and environment.")
    exit(1)