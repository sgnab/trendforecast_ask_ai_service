

# Full file: src/interpret_query_v2.py

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
LLM_MODEL_NAME = os.environ.get("INTERPRET_LLM_MODEL", "gemini-2.5-flash-preview-04-17")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")

# --- Constants for Special Tasks ---
BRAND_ANALYSIS_CATEGORY = "BRAND_ANALYSIS"
INTERNAL_BRAND_PERFORMANCE_SOURCE = "internal_brand_performance"
ANALYZE_BRAND_TASK = "analyze_brand_deep_dive"

AMAZON_RADAR_CATEGORY = "AMAZON_RADAR"
INTERNAL_AMAZON_RADAR_SOURCE = "internal_amazon_radar"
SUMMARIZE_AMAZON_TASK = "summarize_amazon_radar"

WEB_SEARCH_GENERAL_TRENDS_CATEGORY = "WEB_SEARCH_GENERAL_TRENDS"
SUMMARIZE_WEB_TRENDS_TASK = "summarize_web_trends"

COMPARE_CATEGORIES_PLACEHOLDER = "COMPARE_CATEGORIES"
COMPARE_CATEGORIES_TASK = "compare_categories_task"

# --- Logger Setup ---
logger = logging.getLogger()
log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
if log_level_str not in valid_log_levels: log_level_str = "INFO"
logger.setLevel(log_level_str)
logger.info(f"Logger initialized with level: {log_level_str}")
logger.info(f"Using Interpreter LLM: {LLM_MODEL_NAME}")

# --- Globals & Config Loading ---
KNOWN_CATEGORIES: Set[str] = set() # Stores lowercase category names
KNOWN_STYLES: Set[str] = set()
KNOWN_COLORS: Set[str] = set()
CONFIG_LOAD_ERROR: Optional[str] = None

VALID_DEPARTMENTS = ["Men", "Women", "Kids", "Fashion", "Beauty"]
CATEGORIES_BY_DEPARTMENT = { # Assuming these are lowercase already
    "men": {'suits', 'formal shoes', 'cufflinks', 'neckties', 'golf apparel', 'belts', 'wallets', 't-shirts', 'jeans', 'jackets', 'hoodies', 'sneakers', 'socks', 'underwear', 'sweaters', 'shorts', 'pajamas', 'swimwear', 'hats', 'gloves', 'scarves', 'slippers', 'backpacks', 'raincoats', 'sunglasses', 'sportswear', 'sandals', 'boots', 'bags', 'watches', 'necklaces', 'bracelets', 'earrings', 'rings', 'shirts', 'pants', 'blouses', 'beachwear'},
    "women": {'dresses', 'skirts', 'heels', 'jewelry sets', 'handbags', 'bras', 'leggings', 'top tanks', 'jumpsuits', 'bikinis', 't-shirts', 'jeans', 'jackets', 'hoodies', 'sneakers', 'socks', 'underwear', 'sweaters', 'shorts', 'pajamas', 'swimwear', 'hats', 'gloves', 'scarves', 'slippers', 'backpacks', 'raincoats', 'sunglasses', 'sportswear', 'sandals', 'boots', 'bags', 'watches', 'necklaces', 'bracelets', 'earrings', 'rings', 'shirts', 'pants', 'blouses', 'beachwear'},
    "kids": {'t-shirts', 'jeans', 'jackets', 'hoodies', 'sneakers', 'socks', 'underwear', 'shorts', 'pajamas', 'swimwear', 'hats', 'gloves', 'scarves', 'slippers', 'backpacks', 'raincoats', 'sunglasses', 'sportswear', 'sandals', 'boots', 'bags', 'watches', 'necklaces', 'bracelets', 'earrings', 'rings', 'shirts', 'pants', 'blouses', 'beachwear', 'sweaters'},
    "fashion": {'jackets', 'socks', 'pajamas', 'swimwear', 'gloves', 'backpacks', 'sunglasses', 'sportswear', 'boots', 'bags', 'shirts', 'pants', 'blouses', 'beachwear', 't-shirts', 'jeans', 'hoodies', 'sneakers', 'underwear', 'shorts', 'hats', 'slippers', 'raincoats', 'rings', 'scarves', 'sandals', 'bracelets', 'sweaters', 'earrings', 'watches', 'necklaces'},
    "beauty": {'makeup', 'skincare', 'haircare', 'perfumes', 'nail polish', 'lipstick', 'mascara'}
}

# Title Case mapping - Build this once during load_config_csvs
KNOWN_CATEGORIES_TITLE_CASE_MAP: Dict[str, str] = {}

def load_config_csvs():
    global KNOWN_CATEGORIES, KNOWN_STYLES, KNOWN_COLORS, CONFIG_LOAD_ERROR
    logger.info(f"Attempting to load config data from: {CONFIG_DIR}")
    KNOWN_CATEGORIES.clear();
    KNOWN_STYLES.clear();
    KNOWN_COLORS.clear();
    CONFIG_LOAD_ERROR = None
    try:
        if not CATEGORIES_CSV.is_file(): raise FileNotFoundError(f"Categories CSV not found at {CATEGORIES_CSV}")
        with open(CATEGORIES_CSV, mode='r', encoding='utf-8-sig') as infile:
            reader = csv.reader(infile);
            header = next(reader);
            logger.debug(f"Categories CSV header: {header}");
            count = 0
            for row in reader:
                if row and row[0].strip(): KNOWN_CATEGORIES.add(row[0].strip().lower()); count += 1  # Store lowercase
            logger.info(f"Loaded {count} standard categories.")
            if count == 0: logger.warning(f"'{CATEGORIES_CSV.name}' contained no data rows.")
        if STYLES_CSV.is_file():
            with open(STYLES_CSV, mode='r', encoding='utf-8-sig') as infile:
                reader = csv.reader(infile);
                header = next(reader);
                logger.debug(f"Styles CSV header: {header}");
                count = 0
                for row in reader:
                    if row and row[0].strip():
                        style = row[0].strip().lower()
                        if style not in KNOWN_STYLES: KNOWN_STYLES.add(style); count += 1
                logger.info(f"Loaded {count} unique styles.")
                if count == 0: logger.warning(f"'{STYLES_CSV.name}' contained no data rows.")
        else:
            logger.warning(f"Styles CSV not found at {STYLES_CSV}, style checking unavailable.")
        if COLORS_CSV.is_file():
            with open(COLORS_CSV, mode='r', encoding='utf-8-sig') as infile:
                reader = csv.reader(infile);
                header = next(reader);
                logger.debug(f"Colors CSV header: {header}");
                count = 0
                for row in reader:
                    if row and row[0].strip():
                        color = row[0].strip().lower()
                        if color not in KNOWN_COLORS: KNOWN_COLORS.add(color); count += 1
                logger.info(f"Loaded {count} unique colors.")
                if count == 0: logger.warning(f"'{COLORS_CSV.name}' contained no data rows.")
        else:
            logger.warning(f"Colors CSV not found at {COLORS_CSV}, color checking unavailable.")
    except FileNotFoundError as e:
        logger.error(f"Config loading failed: {e}"); CONFIG_LOAD_ERROR = str(e)
    except Exception as e:
        logger.exception(
            "CRITICAL ERROR loading config CSVs!"); CONFIG_LOAD_ERROR = f"Unexpected error loading config CSVs: {e}"


load_config_csvs()

# --- Boto3 Client Setup (Unchanged) ---
secrets_manager = None
BOTO3_CLIENT_ERROR = None
try:
    session = boto3.session.Session()
    secrets_manager = session.client(service_name='secretsmanager', region_name=AWS_REGION)
except Exception as e:
    logger.exception("CRITICAL ERROR initializing Boto3 client!"); BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 client: {e}"

# --- Get Secret Value (Unchanged) ---
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

# --- Brand/Amazon Extraction Helpers (Unchanged) ---
def extract_brand_from_query(query: str) -> Optional[str]:
    # (Function unchanged)
    if not query or not isinstance(query, str): return None
    query_lower = query.lower()
    domain_match = re.search(r'([\w-]+\.[a-z]{2,}(\.[a-z]{2})?)', query_lower)
    if domain_match:
        cleaned = clean_domain_for_lookup(domain_match.group(1))
        if cleaned: logger.info(f"Extracted domain: {cleaned}"); return cleaned
    brand_keywords = ["analyze brand", "brand analysis of", "competitors for", "tell me about brand", "brand profile for", "brand overview for", "brand insights for", "brand", "analyze"]
    for keyword in brand_keywords:
        if keyword in query_lower:
            potential_brand_segment = query_lower.split(keyword, 1)[-1].strip()
            potential_brand_segment = re.sub(r'^(the\s+)?', '', potential_brand_segment, flags=re.IGNORECASE)
            potential_brand_segment = re.sub(r'\s+in\s+.*$', '', potential_brand_segment, flags=re.IGNORECASE)
            potential_brand_segment = re.sub(r'\s+performance$', '', potential_brand_segment, flags=re.IGNORECASE)
            potential_brand_segment = re.sub(r"^[^\w(@.)]+|[^\w(@.)]+$", "", potential_brand_segment)
            target_brand_words = potential_brand_segment.split()
            if target_brand_words:
                for i in range(min(3, len(target_brand_words)), 0, -1):
                    brand_candidate = " ".join(target_brand_words[:i])
                    if len(brand_candidate) > 1:
                         logger.info(f"Extracted potential brand keyword: {brand_candidate} from segment '{potential_brand_segment}'")
                         return brand_candidate.strip()
    logger.warning(f"Could not extract brand/domain from query: {query}"); return None

def clean_domain_for_lookup(input_domain: str) -> str:
    # (Function unchanged)
    if not isinstance(input_domain, str): return ""
    cleaned = re.sub(r'^https?:\/\/', '', input_domain.strip(), flags=re.IGNORECASE)
    cleaned = re.sub(r'^www\.', '', cleaned, flags=re.IGNORECASE)
    if cleaned.endswith('/'): cleaned = cleaned[:-1]
    return cleaned.lower()

def extract_amazon_params(query: str) -> Dict[str, Optional[str]]:
    # (Function unchanged)
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
    # Use the lowercase known categories for matching
    dept_key_lower = found_dept_orig_case.lower()
    if dept_key_lower in CATEGORIES_BY_DEPARTMENT:
        possible_categories = CATEGORIES_BY_DEPARTMENT[dept_key_lower]
        for cat_lower in possible_categories:
            # Use word boundaries for matching to avoid partial matches like 'ant' in 'pants'
            pattern = re.compile(r'\b' + re.escape(cat_lower.replace('-', r'\-')) + r'\b', re.IGNORECASE)
            match = pattern.search(original_query) # Search in original case query to find the word
            if match:
                # Use the known title case mapping
                title_cased_cat = KNOWN_CATEGORIES_TITLE_CASE_MAP.get(cat_lower, cat_lower.title()) # Fallback to simple title case
                params["target_category"] = title_cased_cat
                logger.info(f"Found category '{params['target_category']}' (from {cat_lower}) for department '{found_dept_orig_case}'."); break
    if not params["target_category"]: logger.warning(f"Could not determine category for Amazon Radar (Dept: {found_dept_orig_case}).")
    return params


# --- NEW: Robust Comparison Extraction using N-grams and KNOWN_CATEGORIES ---
# In src/interpret_query_v2.py

def extract_comparison_subjects_no_re_simple_split(query: str, known_categories_lower: Set[str],
                                                   known_categories_map: Dict[str, str]) -> List[str]:
    """
    EXTREMELY simple extraction based on splitting by space and known delimiters.
    Assumes query is very clean or pre-processed.
    Looks for exactly two known categories.
    """
    if not query or not isinstance(query, str):
        return []
    specials=['Polo Shirts','Evening Dresses',
              'Cocktail Dresses','High Heels','Ankle Boots','Running Shoes']
    lst=[]
    texts = query.replace("'", "").split()
    for i in texts:
        if i.lower() in known_categories_lower:
            lst.append(i.lower().title())
    if 'high heels' in query.lower() and 'High Heels' not in lst:
        lst.append('High Heels')
    if 'polo shirts' in query.lower() and 'Polo Shirts' not in lst:
        lst.append('Polo Shirts')
        try:
            lst.remove('Shirts')
        except:
            pass
    if 'evening dresses' in query.lower() and 'Evening Dresses' not in lst:
        lst.append('Evening Dresses')
        try:
            lst.remove('Dresses')
        except:
            pass
    if 'cocktail dresses' in query.lower() and 'Cocktail Dresses' not in lst:
        lst.append('Cocktail Dresses')
        try:
            lst.remove('Dresses')
        except:
            pass
    if 'ankle boots' in query.lower() and 'Ankle Boots' not in lst:
        lst.append('Ankle Boots')
        try:
            lst.remove('Boots')
        except:
            pass
    if 'running shoes' in query.lower() and 'Running Shoes' not in lst:
        lst.append('Running Shoes')
        try:
            lst.remove('Shoes')
        except:
            pass
    if 'tank tops' in query.lower() and 'Tank Tops' not in lst:
        lst.append('Tank Tops')
        try:
            lst.remove('Tops')
        except:
            pass
    if 'crop tops' in query.lower() and 'Crop Tops' not in lst:
        lst.append('Crop Tops')
        try:
            lst.remove('Tops')
        except:
            pass
    if 'tote bags'in  query.lower() and 'tote bags' not in lst:
        lst.append('Tote Bags')
        try:
            lst.remove('Bags')
        except:
            pass

    if 't-shirts' in query.lower() and "T-shirts" not in lst:
        lst.append('T-shirts')
    while len(lst)>2:
        x=lst.pop()
    logger.info(f"extracted categories are {lst}")


    return lst

# --- END NEW ROBUST ---


# --- Main Lambda Handler ---
def lambda_handler(event, context):
    # (Initial checks for CONFIG_LOAD_ERROR, BOTO3_CLIENT_ERROR, GEMINI_SDK_AVAILABLE unchanged)
    if CONFIG_LOAD_ERROR: return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Config Error: {CONFIG_LOAD_ERROR}"})}
    if BOTO3_CLIENT_ERROR: return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": BOTO3_CLIENT_ERROR})}
    if not GEMINI_SDK_AVAILABLE: return {"statusCode": 500, "body": json.dumps({"status": "error", "error_message": "Gemini SDK unavailable."})}

    logger.info(f"Received event: {json.dumps(event)}")
    try:
        # (Input parsing unchanged)
        if isinstance(event.get('body'), str): body = json.loads(event['body']); logger.debug("Parsed body from API GW event.")
        elif isinstance(event, dict) and 'query' in event and 'category' in event and 'country' in event: body = event; logger.debug("Using direct event payload.")
        else: raise ValueError("Invalid input structure")
        user_query = body.get('query'); category = body.get('category').lower().title(); country = body.get('country')
        if not user_query or category is None or not country: raise ValueError("Missing required fields")
        logger.info(f"Input - Query: '{user_query}', Cat: '{category}', Country: '{country}'")
    except (json.JSONDecodeError, ValueError, TypeError) as e:
        logger.error(f"Request parsing error: {e}"); return {"statusCode": 400, "body": json.dumps({"status": "error", "error_message": f"Invalid input: {e}"})}

    category_upper = category.upper() if isinstance(category, str) else ""
    original_context_payload = {'category': category, 'country': country, 'query': user_query}

    # --- Placeholder Handling Logic ---

    # 1. Handle BRAND_ANALYSIS (Unchanged)
    if category_upper == BRAND_ANALYSIS_CATEGORY:
        # ... (logic unchanged) ...
        logger.info(f"Detected '{BRAND_ANALYSIS_CATEGORY}' category...")
        extracted_brand = extract_brand_from_query(user_query)
        original_context_payload['target_brand'] = extracted_brand
        if extracted_brand:
            output_payload = {"status": "success", "primary_task": ANALYZE_BRAND_TASK, "required_sources": [INTERNAL_BRAND_PERFORMANCE_SOURCE, "web_search"], "query_subjects": {"specific_known": [], "unmapped_items": [], "target_brand": extracted_brand}, "timeframe_reference": None, "attributes": [], "clarification_needed": None, "original_context": original_context_payload}
            logger.info("Bypassing LLM. Returning direct payload for Brand Analysis.")
            return {"statusCode": 200, "body": json.dumps(output_payload)}
        else: # Clarification needed
            output_payload = {"status": "needs_clarification", "primary_task": "unknown", "required_sources": ["clarify"], "query_subjects": {"specific_known": [], "unmapped_items": []}, "timeframe_reference": None, "attributes": [], "clarification_needed": "Please specify the brand name or website for analysis.", "original_context": original_context_payload}
            return {"statusCode": 200, "body": json.dumps(output_payload)}


    # 2. Handle AMAZON_RADAR (Unchanged)
    elif category_upper == AMAZON_RADAR_CATEGORY:
        # ... (logic unchanged) ...
        logger.info(f"Detected '{AMAZON_RADAR_CATEGORY}' category...")
        amazon_params = extract_amazon_params(user_query)
        target_dept = amazon_params.get("department"); target_cat = amazon_params.get("target_category")
        original_context_payload['target_department'] = target_dept; original_context_payload['target_category'] = target_cat
        clarification_msg = None
        if not target_dept: clarification_msg = "Which department for Amazon Radar (e.g., Men, Women, Kids, Fashion, Beauty)?"
        elif not target_cat: clarification_msg = f"Which category within '{target_dept}' for Amazon Radar?"
        else:
            dept_key_lower = target_dept.lower()
            # Check against lowercase known Amazon categories for the department
            amazon_cats_for_dept = CATEGORIES_BY_DEPARTMENT.get(dept_key_lower, set())
            if target_cat.lower() not in amazon_cats_for_dept:
                 clarification_msg = f"Category '{target_cat}' is not valid for '{target_dept}' on Amazon Radar. Please specify a valid category for {target_dept}."
        if clarification_msg: # Clarification needed
             output_payload = {"status": "needs_clarification", "primary_task": "unknown", "required_sources": ["clarify"], "query_subjects": {"specific_known": [], "unmapped_items": []}, "timeframe_reference": None, "attributes": [], "clarification_needed": clarification_msg, "original_context": original_context_payload}
             return {"statusCode": 200, "body": json.dumps(output_payload)}
        else: # Success
             logger.info(f"Valid Amazon params: Dept='{target_dept}', Cat='{target_cat}'")
             output_payload = {"status": "success", "primary_task": SUMMARIZE_AMAZON_TASK, "required_sources": [INTERNAL_AMAZON_RADAR_SOURCE], "query_subjects": {"specific_known": [], "unmapped_items": []}, "timeframe_reference": None, "attributes": [], "clarification_needed": None, "original_context": original_context_payload}
             logger.info("Bypassing LLM. Returning direct payload for Amazon Radar.")
             return {"statusCode": 200, "body": json.dumps(output_payload)}

    # 3. Handle WEB_SEARCH_GENERAL_TRENDS (Unchanged)
    elif category_upper == WEB_SEARCH_GENERAL_TRENDS_CATEGORY:
        # ... (logic unchanged) ...
         logger.info(f"Detected '{WEB_SEARCH_GENERAL_TRENDS_CATEGORY}' category...")
         output_payload = {
             "status": "success", "primary_task": SUMMARIZE_WEB_TRENDS_TASK, "required_sources": ["web_search"],
             "query_subjects": {"specific_known": [], "unmapped_items": []}, "timeframe_reference": None,
             "attributes": [], "clarification_needed": None, "original_context": original_context_payload
         }
         logger.info("Bypassing LLM. Returning direct payload for General Web Search Trends.")
         return {"statusCode": 200, "body": json.dumps(output_payload)}


    # 4. Handle COMPARE_CATEGORIES Placeholder (Uses NEW robust extraction)
    elif category_upper == COMPARE_CATEGORIES_PLACEHOLDER:
        logger.info(
            f"Detected '{COMPARE_CATEGORIES_PLACEHOLDER}' category. Attempting NO-RE simple extraction from query: '{user_query}'")

        comparison_subjects_names_title_case = extract_comparison_subjects_no_re_simple_split(
            user_query,
            KNOWN_CATEGORIES,  # Pass lowercase set
            KNOWN_CATEGORIES_TITLE_CASE_MAP  # Pass map for original casing
        )

        clarification_message = None

        if len(comparison_subjects_names_title_case) == 2:
            # Success condition: Exactly 2 known categories found
            validated_comparison_subjects = [{"subject": name, "type": "category"} for name in comparison_subjects_names_title_case]
            original_context_payload['comparison_subjects_extracted'] = comparison_subjects_names_title_case
            output_payload = {
                "status": "success", "primary_task": COMPARE_CATEGORIES_TASK,
                "required_sources": ["internal_trends_category"],
                "query_subjects": {
                    "comparison_subjects": validated_comparison_subjects,
                    "specific_known": [], "unmapped_items": []
                },
                "timeframe_reference": None, "attributes": [], "clarification_needed": None,
                "original_context": original_context_payload
            }
            logger.info("Bypassing LLM. Returning direct payload for Category Comparison (Robust Extraction).")
            return {"statusCode": 200, "body": json.dumps(output_payload)}

        elif len(comparison_subjects_names_title_case) > 2:
             clarification_message = f"Your query mentions multiple potential categories ({', '.join(comparison_subjects_names_title_case)}). Please specify exactly which two you'd like to compare."
        elif len(comparison_subjects_names_title_case) == 1:
             clarification_message = f"I found the category '{comparison_subjects_names_title_case[0]}' in your query. Please specify which other category you'd like to compare it with."
        else: # 0 found
             clarification_message = "Sorry, I couldn't identify two known categories to compare in your query. Please rephrase, for example: 'Compare Jeans vs Pants'."

        # Fallback to clarification
        output_payload = {
            "status": "needs_clarification", "primary_task": "unknown", "required_sources": ["clarify"],
            # Include found subjects (0, 1, or >2) in the payload for context
            "query_subjects": {"comparison_subjects": [{"subject": name, "type": "category"} for name in comparison_subjects_names_title_case],
                              "specific_known": [], "unmapped_items": []},
            "timeframe_reference": None, "attributes": [], "clarification_needed": clarification_message,
            "original_context": original_context_payload
        }
        return {"statusCode": 200, "body": json.dumps(output_payload)}

    # 5. Handle Standard Interpretation (LLM Path - Unchanged)
    else:
        logger.info("Category is not a placeholder. Proceeding with standard LLM interpretation.")
        # (Standard LLM interpretation logic is exactly the same as the previous version)
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
        # This is the string that will be formatted and assigned to the 'prompt' variable
        # within the standard LLM interpretation block of interpret_query_v2.py

        # Example of how it would be used in the code:
        # user_query = "..." ; category = "..." ; country = "..."
        # known_styles_list = [...] ; known_colors_list = [...]
        # prompt_template_string = """ ... (the full text below) ... """
        # prompt = prompt_template_string.format(
        #     category=category,
        #     country=country,
        #     known_styles_list_json=json.dumps(known_styles_list) if known_styles_list else "None Provided",
        #     known_colors_list_json=json.dumps(known_colors_list) if known_colors_list else "None Provided",
        #     user_query=user_query
        # )
        category=category.lower().title()
        prompt = f"""Analyze the user query strictly within the given fashion context.
        
        Context:
            - Category: "{category}"
            - Country: "{country}"
            - List of All Known Styles (global): {json.dumps(known_styles_list) if known_styles_list else "None Provided"}
            - List of All Known Colors (global): {json.dumps(known_colors_list) if known_colors_list else "None Provided"}


        User Query: "{user_query.lower()}"

        Instructions:
        1.  Identify the primary analysis task based on the User Query's intent. Choose ONE EXCLUSIVELY from this exact list: ['get_trend', 'get_forecast', 'get_recommendation', 'compare_items', 'summarize_category', 'summarize_mega_trends', 'qa_web_only', 'qa_internal_only', 'qa_combined', 'unknown']. 
            **Prioritize 'summarize_mega_trends' if the query uses keywords like 'mega', 'hot', 'hottest', or 'rising trends' AND does not mention specific items/styles/colors.** Otherwise, determine intent based on keywords like 'forecast', 'recommend', 'compare', 'summarize category', 'trend', 'why', 'news', etc. For general questions about "what's trending" without a specific item, lean towards 'summarize_category' or 'summarize_mega_trends' if applicable. If the query asks 'why' something is trending or for reasoning that requires external knowledge, consider 'qa_web_only' or 'qa_combined'.

        2.  Determine the necessary data sources required for the identified primary_task. Choose one or more EXCLUSIVELY from this exact list: ['internal_trends_category', 'internal_trends_item', 'internal_forecast', 'internal_mega', 'web_search', 'clarify']. Follow these rules STRICTLY:
            -   Web Search Rule: You MUST include 'web_search' if the query explicitly asks 'why', mentions 'news', 'sentiment', 'competitors', 'web', 'web search', 'hot' trends, 'this week', 'global' trends, or clearly requires external context/reasoning not available in internal data. Also, if the task is 'qa_web_only' or 'qa_combined', 'web_search' is mandatory.
            -   Item Detail Rule: If step 3 identifies ANY subjects in `specific_known_subjects` (meaning specific styles or colors are identified for the given category) AND the task requires item-level detail (like 'get_forecast', 'get_recommendation' for an item, 'compare_items', 'get_trend' for a specific item/style/color), you MUST include 'internal_trends_item'.
            -   Forecast Rule: If the primary_task is 'get_forecast' and 'internal_trends_item' is selected (due to specific subjects being present), you MUST ALSO include 'internal_forecast'. Forecasts are only possible for specific items/styles/colors.
            -   Category Context Rule: If the task is broad (e.g., 'summarize_category', or 'get_trend' for the whole category without specific items/styles/colors mentioned or identified), use 'internal_trends_category'. ALSO, if the task is 'get_trend' or 'qa_combined' or 'qa_internal_only' or 'qa_web_only' and step 3 identifies items in `unmapped_items` but NOT in `specific_known_subjects`, you MUST include 'internal_trends_category' to provide context.
            -   Mega Trends Rule: Use 'internal_mega' ONLY if the primary_task is 'summarize_mega_trends'. If 'internal_mega' is selected, DO NOT include 'internal_trends_category', 'internal_trends_item', or 'internal_forecast'. However, 'web_search' CAN be combined with 'internal_mega' if the query implies needing external context for mega trends. Also, as checked in step 3, DO NOT use 'internal_mega' if step 3 identifies ANY subjects in `specific_known_subjects` OR `unmapped_items` (as mega trends are broad, not item-specific).
            -   Clarification Rule: If the query is too ambiguous, invalid, lacks specifics needed for the task (e.g., 'get_forecast' without an item/style/color), or falls outside the Category/Country context, use ONLY 'clarify' as the source.

        3.  Extract key entities mentioned in the User Query. Apply these rules STRICTLY:
            -   First, identify all potential fashion subjects (styles, colors, items like 'bomber jacket') in the query.
            -   For EACH potential subject:
                a. Check for an exact case-insensitive match in the 'All Known Styles' or 'All Known Colors' lists provided in the Context.
                b. If a match IS found: Determine if it's a 'style' or 'color'. 
                   **If it's a 'color', add it directly** to the `specific_known_subjects` list as an object: `{{"subject": "Matched Term Title Case", "type": "color"}}`.
                   **If it's a 'style', THEN check if the matched style is appropriate** for the stated Category context (e.g., 'Dresses' as a style is inappropriate for the 'Shirts' Category). If the style IS appropriate for the category, add it to `specific_known_subjects` as an object: `{{"subject": "Matched Term Title Case", "type": "style"}}`. If the style is NOT appropriate for the category, add the term (Title Case) to `unmapped_items`.
                c. If NO exact match is found in the known lists: Add the term (Title Case) to the `unmapped_items` list.
                d. DO NOT guess or find the 'closest' match. Only exact matches are processed for `specific_known_subjects`.
            -   `specific_known_subjects`: List of objects for matched subjects (colors are always added if matched; styles only if matched AND category-appropriate). Can be empty.
            -   `unmapped_items`: List of terms (Title Case) that were not exact matches, were category-inappropriate styles, or other potential fashion items. Can be empty.
            -   `timeframe_reference`: Any mention of time (e.g., "next 6 months", "latest", "last year"). Return null if none found.
            -   `attributes`: Any other descriptors mentioned (e.g., "material:linen", "price:high"). Return [] if none found.

        4.  Determine the overall 'status'. It MUST be 'needs_clarification' ONLY if 'clarify' is in `required_sources` (from step 2) OR if step 3 added items to `unmapped_items` because they were category-inappropriate styles that prevent analysis. Otherwise (even if 'web_search' is required or there are other `unmapped_items` like unrecognized product names), it MUST be 'success'.

        5.  Provide a concise 'clarification_needed' message (string) ONLY if status is 'needs_clarification' (from step 4), otherwise it MUST be null. Explain *why* clarification is needed (e.g., "Style 'Dresses' is not applicable to the 'Shirts' category. Please specify a relevant style or remove it.", or "Query is too ambiguous, please specify if you want trends or a forecast.", or "Forecasts require a specific style or color for [Category].").

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

            # (Standard LLM output validation and post-processing unchanged)
            required_keys = ["status", "primary_task", "required_sources", "query_subjects", "timeframe_reference", "attributes", "clarification_needed"]
            missing_keys = [key for key in required_keys if key not in llm_output]
            if missing_keys: raise ValueError(f"LLM output missing required keys: {', '.join(missing_keys)}.")
            if not isinstance(llm_output.get("required_sources"), list): raise ValueError("LLM 'required_sources' not list.")
            query_subjects_llm = llm_output.get("query_subjects")
            if not isinstance(query_subjects_llm, dict): raise ValueError("LLM 'query_subjects' not dict.")
            if "specific_known" not in query_subjects_llm or "unmapped_items" not in query_subjects_llm: raise ValueError("LLM 'query_subjects' missing keys.")
            if not isinstance(query_subjects_llm["unmapped_items"], list): raise ValueError("LLM 'unmapped_items' not list.")
            specific_known_llm = query_subjects_llm["specific_known"]
            if not isinstance(specific_known_llm, list): raise ValueError("LLM 'specific_known' not list.")
            for item in specific_known_llm:
                if not isinstance(item, dict): raise ValueError(f"Item in 'specific_known' not dict: {item}")
                if "subject" not in item or "type" not in item: raise ValueError(f"Item missing 'subject'/'type': {item}")
                if item["type"] not in ["color", "style"]: raise ValueError(f"Invalid type '{item['type']}': {item}")
                if not isinstance(item.get("subject"), str): raise ValueError(f"Subject not string: {item}")

            current_task = llm_output.get("primary_task"); current_status = llm_output.get("status")
            if (current_task is None or current_task == "unknown") and \
               (current_status is None or current_status not in ["success", "needs_clarification"]) and \
               not query_subjects_llm.get("specific_known") and not query_subjects_llm.get("unmapped_items"):
                logger.warning("LLM failed to classify a general category query. Applying fallback.")
                llm_output["primary_task"] = "summarize_category"; llm_output["required_sources"] = ["internal_trends_category"]
                llm_output["status"] = "success"; llm_output["clarification_needed"] = None
                if "query_subjects" not in llm_output: llm_output["query_subjects"] = {"specific_known": [], "unmapped_items": []}
                if "timeframe_reference" not in llm_output: llm_output["timeframe_reference"] = None
                if "attributes" not in llm_output: llm_output["attributes"] = []

            primary_task_llm = llm_output.get("primary_task"); required_sources_set = set(llm_output.get("required_sources", []))
            unmapped_items_llm = llm_output.get("query_subjects", {}).get("unmapped_items", [])
            specific_known_llm = llm_output.get("query_subjects", {}).get("specific_known", [])

            if primary_task_llm == "summarize_mega_trends":
                if "internal_mega" not in required_sources_set: required_sources_set.add("internal_mega")
                required_sources_set.add("web_search")
                required_sources_set.discard("internal_trends_item"); required_sources_set.discard("internal_forecast"); required_sources_set.discard("internal_trends_category")

            if primary_task_llm in ["get_trend", "qa_combined", "qa_internal_only", "qa_web_only"] and \
               not specific_known_llm and unmapped_items_llm and \
               "internal_trends_category" not in required_sources_set:
               required_sources_set.add("internal_trends_category")

            if specific_known_llm or unmapped_items_llm:
                if "internal_mega" in required_sources_set and primary_task_llm != "summarize_mega_trends":
                    required_sources_set.discard("internal_mega")
                    if not required_sources_set: required_sources_set.add("internal_trends_category")

            has_mega = "internal_mega" in required_sources_set
            has_other_internal = any(s in required_sources_set for s in ["internal_trends_category", "internal_trends_item", "internal_forecast"])

            if primary_task_llm != "summarize_mega_trends" and has_mega and has_other_internal :
                 required_sources_set.discard("internal_mega")

            llm_output["required_sources"] = sorted(list(required_sources_set))

            if "clarify" in llm_output.get("required_sources", []) and llm_output.get("status") != "needs_clarification":
                llm_output["status"] = "needs_clarification"
            if llm_output.get("status") == "needs_clarification" and not llm_output.get("clarification_needed"):
                llm_output["clarification_needed"] = "Query requires clarification. Please be more specific."

            logger.info(f"LLM interpretation successful (post-processed). Task: {llm_output.get('primary_task')}, Status: {llm_output.get('status')}, Sources: {llm_output.get('required_sources')}")
            llm_output['original_context'] = original_context_payload
            return { "statusCode": 200, "body": json.dumps(llm_output) }

        except (json.JSONDecodeError, ValueError, TypeError) as e:
             logger.error(f"Failed parsing/validating LLM response: {e}", exc_info=True); logger.error(f"LLM Raw Text was: {response.text}")
             return { "statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Failed processing LLM response: {e}", "llm_raw_output": response.text}) }
        except Exception as e:
            logger.exception("Unhandled error during standard LLM interpretation.")
            return { "statusCode": 500, "body": json.dumps({"status": "error", "error_message": f"Internal server error during standard interpretation: {str(e)}"}) }