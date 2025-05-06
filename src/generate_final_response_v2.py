#
# import json
# import logging
# import os
# import re # Added for regex parsing
# from typing import Dict, Optional, List, Any
#
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
# # --- Configuration and Constants ---
# SECRET_NAME = os.environ.get("SECRET_NAME", "YourSecretsName")
# SYNTHESIS_LLM_MODEL = os.environ.get("SYNTHESIS_LLM_MODEL", "gemini-2.0-flash")
# AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
#
# # --- Result Type Indicators ---
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
# INDICATOR_CLARIFICATION = "CLARIFICATION_NEEDED"
# # --- *** MODIFICATION START: Add Brand Analysis Indicator *** ---
# INDICATOR_BRAND_ANALYSIS = "BRAND_ANALYSIS" # New indicator
# # --- *** MODIFICATION END *** ---
#
# # --- Logger Setup ---
# logger = logging.getLogger()
# log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
# valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
# if log_level_str not in valid_log_levels: log_level_str = "INFO"
# logger.setLevel(log_level_str)
# logger.info(f"Logger initialized with level: {log_level_str}")
# logger.info(f"SYNTHESIS_LLM_MODEL: {SYNTHESIS_LLM_MODEL}")
# logger.info(f"SECRET_NAME: {SECRET_NAME}")
#
# # --- Boto3 Client and Secret Handling ---
# secrets_manager = None
# BOTO3_CLIENT_ERROR = None
# try:
#     session = boto3.session.Session()
#     secrets_manager = session.client(service_name='secretsmanager', region_name=AWS_REGION)
# except Exception as e:
#     logger.exception("CRITICAL ERROR initializing Boto3 Secrets Manager client!")
#     BOTO3_CLIENT_ERROR = f"Failed to initialize Boto3 client: {e}"
#
# API_KEY_CACHE: Dict[str, Optional[str]] = {}
#
# def get_secret_value(secret_name: str, key_name: str) -> Optional[str]:
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
#         elif 'SecretBinary' in response:
#              try: secret_dict = json.loads(response['SecretBinary'].decode('utf-8'))
#              except (json.JSONDecodeError, UnicodeDecodeError) as e: logger.error(f"Failed binary decode: {e}"); return None
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
# # --- *** MODIFICATION START: Add Brand Analysis Prompt *** ---
# PERSONA_PROMPTS = {
#     INDICATOR_TREND_DETAIL: """You are a TrendForecast.io Expert Analyst providing insights on specific fashion items... (Keep existing prompt)""",
#     INDICATOR_MEGA_TREND: """You are a TrendForecast.io Senior Strategist providing a synthesized analysis of the '{category_name}' category in '{country_name}', focusing on how high-level mega trends intersect with current category performance... (Keep existing enhanced prompt)""",
#     INDICATOR_CATEGORY_OVERVIEW: """You are a TrendForecast.io Market Analyst providing a high-level overview of a fashion category... (Keep existing prompt)""",
#     INDICATOR_FORECAST: """You are a TrendForecast.io Forecast Specialist providing predictions for a specific item... (Keep existing prompt with formatting instructions)""",
#     # --- New Brand Analysis Prompt ---
#     INDICATOR_BRAND_ANALYSIS: """You are a TrendForecast.io Brand Analyst. Analyze the provided internal performance data (estimated visits/growth per country) for the brand '{brand_domain}' and any relevant external web context (summarized answer or top results).
# Instructions:
# 1. Write a concise summary synthesizing the key findings about the brand's performance across the available countries.
# 2. Highlight countries with notable high/low volume or growth based on the 'Internal Brand Performance Data'.
# 3. Incorporate relevant insights, news, or competitor mentions from the 'External Web Context' to provide a fuller picture.
# 4. Conclude with a brief overall assessment of the brand's current standing based on the combined data.
# 5. Structure the response clearly using markdown formatting (e.g., use '**Subheadings**' for different parts like 'Performance Overview', 'Web Context Insights', 'Overall Assessment', and use '* Bullet points' for lists where appropriate). DO NOT use '###' markdown headings.
# 6. Keep the tone objective and data-driven. Focus ONLY on the provided data.""",
#     # --- End New Brand Analysis Prompt ---
#     INDICATOR_COMPARISON: """You are a TrendForecast.io Comparative Analyst... [TODO: Define prompt]""",
#     INDICATOR_RECOMMENDATION: """You are a TrendForecast.io Recommendation Engine... [TODO: Define prompt]""",
#     INDICATOR_QA_WEB: """You are a helpful AI assistant answering a question based primarily on web search results... (Keep existing prompt)""",
#     INDICATOR_QA_INTERNAL: """You are a helpful AI assistant answering a question based primarily on internal trend data... (Keep existing prompt)""",
#     INDICATOR_QA_COMBINED: """You are a helpful AI assistant answering a question using both internal data and web search results... (Keep existing prompt)""",
#     INDICATOR_UNKNOWN: """You are a helpful AI assistant... (Keep existing prompt)""",
#     INDICATOR_ERROR: "Error processing request.",
# }
# # --- *** MODIFICATION END *** ---
#
#
# # --- *** MODIFICATION START: Update get_task_details *** ---
# def get_task_details(primary_task: str | None) -> tuple[str, str]:
#     indicator = INDICATOR_UNKNOWN # Default
#     # Map primary_task from interpreter to the correct indicator
#     if primary_task == "get_trend": indicator = INDICATOR_TREND_DETAIL
#     elif primary_task == "get_forecast": indicator = INDICATOR_FORECAST
#     elif primary_task == "summarize_mega_trends": indicator = INDICATOR_MEGA_TREND
#     elif primary_task == "summarize_category": indicator = INDICATOR_CATEGORY_OVERVIEW
#     elif primary_task == "compare_items": indicator = INDICATOR_COMPARISON # TODO
#     elif primary_task == "get_recommendation": indicator = INDICATOR_RECOMMENDATION # TODO
#     elif primary_task == "qa_web_only": indicator = INDICATOR_QA_WEB
#     elif primary_task == "qa_internal_only": indicator = INDICATOR_QA_INTERNAL
#     elif primary_task == "qa_combined": indicator = INDICATOR_QA_COMBINED
#     # --- Add mapping for the new Brand Analysis task ---
#     elif primary_task == "analyze_brand_deep_dive": # Match task name from Interpreter
#         indicator = INDICATOR_BRAND_ANALYSIS
#     # --- End mapping ---
#     elif primary_task == "unknown": indicator = INDICATOR_UNKNOWN
#     elif primary_task == "error": indicator = INDICATOR_ERROR
#
#     prompt_template = PERSONA_PROMPTS.get(indicator, PERSONA_PROMPTS[INDICATOR_UNKNOWN])
#     logger.info(f"Mapped primary_task '{primary_task}' to indicator '{indicator}'.")
#     return indicator, prompt_template
# # --- *** MODIFICATION END *** ---
#
# # --- *** MODIFICATION START: Update format_data_for_prompt *** ---
# def format_data_for_prompt(internal_data: Dict, external_data: Dict) -> str:
#     prompt_parts = []
#     internal_data = internal_data or {}
#     external_data = external_data or {}
#     interpretation = internal_data.get("interpretation") if isinstance(internal_data.get("interpretation"), dict) else {}
#     original_context = interpretation.get("original_context") if isinstance(interpretation.get("original_context"), dict) else {}
#     query_subjects = interpretation.get("query_subjects") if isinstance(interpretation.get("query_subjects"), dict) else {}
#     specific_known = query_subjects.get("specific_known", []) if isinstance(query_subjects.get("specific_known"), list) else []
#     primary_task = interpretation.get("primary_task")
#     target_brand = query_subjects.get("target_brand") # Extract target brand if present
#
#     # --- Basic Context (Always Add) ---
#     prompt_parts.append("CONTEXT:")
#     prompt_parts.append(f"- User Query: {original_context.get('query', 'N/A')}")
#     # Use target_brand if available, otherwise default context
#     if primary_task == "analyze_brand_deep_dive" and target_brand:
#          prompt_parts.append(f"- Brand Focus: {target_brand}")
#          # Country might still be relevant if data is country-specific
#          prompt_parts.append(f"- Country Context (if applicable): {original_context.get('country', 'N/A')}")
#     else:
#         prompt_parts.append(f"- Category: {original_context.get('category', 'N/A')}")
#         prompt_parts.append(f"- Country: {original_context.get('country', 'N/A')}")
#         if specific_known and isinstance(specific_known[0], dict):
#             specific_item_name = specific_known[0].get("subject", "N/A")
#             prompt_parts.append(f"- Specific Focus Item: {specific_item_name}")
#
#     prompt_parts.append("\nAVAILABLE DATA:")
#     data_found = False
#
#     # --- Format Brand Performance Data (Only for Brand Analysis Task) ---
#     if primary_task == "analyze_brand_deep_dive":
#         brand_perf_data = internal_data.get("brand_performance_data")
#         if isinstance(brand_perf_data, list) and brand_perf_data:
#             data_found = True
#             prompt_parts.append(f"\nInternal Brand Performance Data ({target_brand or 'Requested Brand'}):")
#             for country_data in brand_perf_data[:10]: # Limit amount of data sent
#                 if isinstance(country_data, dict):
#                     country = country_data.get('country', 'N/A')
#                     visits = country_data.get('estimated_monthly_visits', 'N/A')
#                     growth = country_data.get('estimated_growth_percentage', 'N/A')
#                     # Format nicely
#                     growth_str = f"{growth:.1f}%" if isinstance(growth, (int, float)) else "N/A"
#                     visits_str = f"{visits:,}" if isinstance(visits, (int, float)) else "N/A"
#                     prompt_parts.append(f"- {country}: Est. Visits={visits_str}, Est. Growth={growth_str}")
#             if not data_found: # Check if loop actually added anything
#                  prompt_parts.append("- No performance data found in provided list.")
#                  # Don't set data_found = True if list was empty/malformed
#         else:
#             logger.debug("brand_performance_data missing or invalid for brand analysis task.")
#             prompt_parts.append("\nInternal Brand Performance Data:")
#             prompt_parts.append("- Data not available.")
#
#     # --- Format Other Internal Data (Conditionally) ---
#     # Only include if NOT brand analysis task
#     if primary_task != "analyze_brand_deep_dive":
#         # Category Context
#         trends = internal_data.get("trends_data")
#         if isinstance(trends, dict):
#             data_found = True
#             prompt_parts.append("\nInternal Category Context:")
#             cs = trends.get("category_summary")
#             if isinstance(cs, dict): prompt_parts.append(f"- Overall Category ({cs.get('category_name', 'N/A')}): Avg Vol={cs.get('average_volume', 'N/A')}, Growth={cs.get('growth_recent', 'N/A'):.1f}%")
#             style_details = trends.get("style_details", [])
#             color_details = trends.get("color_details", [])
#             if isinstance(style_details, list):
#                 top_styles = sorted([s for s in style_details if isinstance(s,dict)], key=lambda x: x.get('average_volume', 0), reverse=True)[:3]
#                 if top_styles: prompt_parts.append(f"- Top Styles in Category: {', '.join([s.get('style_name', 'N/A') for s in top_styles])}")
#             if isinstance(color_details, list):
#                 top_colors = sorted([c for c in color_details if isinstance(c,dict)], key=lambda x: x.get('average_volume', 0), reverse=True)[:3]
#                 if top_colors: prompt_parts.append(f"- Top Colors in Category: {', '.join([c.get('color_name', 'N/A') for c in top_colors])}")
#         else: logger.debug("trends_data is missing or not a dict.")
#
#         # Item Details
#         if primary_task not in ["summarize_mega_trends", "summarize_category"]:
#             details = internal_data.get("chart_details_data")
#             if isinstance(details, dict):
#                  data_found = True
#                  prompt_parts.append(f"\nInternal Specific Item Details ({details.get('category_subject', 'N/A')}):")
#                  prompt_parts.append(f"- Avg Vol={details.get('average_volume', 'N/A')}, Growth={details.get('growth_recent', 'N/A'):.1f}%")
#                  if primary_task == "get_forecast":
#                      # ... (forecast details) ...
#                      prompt_parts.append(f"- Data Point f2 (2m Growth %): {details.get('f2', 'N/A')}")
#                      prompt_parts.append(f"- Data Point f3 (3m Growth %): {details.get('f3', 'N/A')}")
#                      prompt_parts.append(f"- Data Point f6 (6m Growth %): {details.get('f6', 'N/A')}")
#                      prompt_parts.append(f"- Data Point avg2 (2m Avg Volume): {details.get('avg2', 'N/A')}")
#                      prompt_parts.append(f"- Data Point avg3 (3m Avg Volume): {details.get('avg3', 'N/A')}")
#                      prompt_parts.append(f"- Data Point avg6 (6m Avg Volume): {details.get('avg6', 'N/A')}")
#             else: logger.debug("chart_details_data is missing or not a dict.")
#
#         # Mega Trends
#         if primary_task == "summarize_mega_trends": # Should be included if task is mega trends
#             mega = internal_data.get("mega_trends_data")
#             if isinstance(mega, list) and mega:
#                 data_found = True
#                 prompt_parts.append("\nInternal Mega Trends (Top Queries):")
#                 top_mega = sorted([m for m in mega if isinstance(m,dict)], key=lambda x: x.get('growth_recent', 0), reverse=True)[:5]
#                 if top_mega:
#                     for m in top_mega: prompt_parts.append(f"- Query: '{m.get('query_name', 'N/A')}', Related Category: {m.get('category_name', 'N/A')}, Growth: {m.get('growth_recent', 'N/A'):.1f}%")
#                 else: prompt_parts.append("- No specific mega trend query data found.")
#             else:
#                 logger.debug("mega_trends_data is missing or invalid for mega_trends task.")
#                 prompt_parts.append("\nInternal Mega Trends (Top Queries):")
#                 prompt_parts.append("- Data not available.")
#
#     # --- External Web Context (Always include if available) ---
#     ext_answer = external_data.get("answer")
#     ext_results = external_data.get("results", [])
#     if not isinstance(ext_results, list): ext_results = []
#
#     if ext_answer or ext_results: # Check if there's any web data
#          data_found = True # Web data also counts as data being found
#          prompt_parts.append("\nExternal Web Context:")
#          if ext_answer:
#               prompt_parts.append(f"- Synthesized Answer: {ext_answer}")
#          if ext_results:
#               prompt_parts.append("- Top Results Snippets:")
#               for i, res in enumerate(ext_results[:3]):
#                    if isinstance(res, dict):
#                       title = res.get('title', 'N/A')
#                       content_snippet = res.get('content', '')[:150]
#                       prompt_parts.append(f"  - [{i+1}] {title}: {content_snippet}...")
#     else:
#          logger.debug("No external web data available for prompt.")
#          # Add note only if web search was expected
#          if interpretation.get("required_sources") and "web_search" in interpretation["required_sources"]:
#               prompt_parts.append("\nExternal Web Context:")
#               prompt_parts.append("- No relevant web context found or provided.")
#
#
#     # --- Final check ---
#     if not data_found:
#         logger.warning("No significant internal or external data blocks were available to format for prompt.")
#         query = original_context.get('query', 'N/A')
#         # Adjust message based on task type
#         if primary_task == "analyze_brand_deep_dive":
#              brand = target_brand or 'the requested brand'
#              return f"No specific performance data or web context available to analyze for brand '{brand}'."
#         else:
#              cat = original_context.get('category', 'N/A')
#              country = original_context.get('country', 'N/A')
#              return f"No specific data available to analyze for query '{query}' on category '{cat}' in {country}."
#
#     return "\n".join(prompt_parts)
# # --- *** MODIFICATION END *** ---
#
#
# # --- Parser Function (Keep flat subsection parser) ---
# def parse_markdown_flat_subsections(markdown_text: str) -> Dict[str, Any]:
#     # (Keep the parse_markdown_flat_subsections function as defined previously)
#     if not markdown_text or not isinstance(markdown_text, str):
#         return {"subsections": []}
#     all_subsections: List[Dict] = []
#     current_subsection: Optional[Dict] = None
#     content_buffer: List[str] = []
#     lines = markdown_text.strip().split('\n')
#     def finalize_buffer():
#         nonlocal content_buffer, current_subsection
#         if not content_buffer or current_subsection is None:
#              content_buffer = []
#              return
#         text = "\n".join(content_buffer).strip()
#         if text:
#             if current_subsection.get("content") is None: current_subsection["content"] = text
#             else: current_subsection["content"] += "\n" + text
#         content_buffer = []
#     try:
#         for line in lines:
#             line = line.strip()
#             if not line: finalize_buffer(); continue
#             if line.startswith("###"): finalize_buffer(); current_subsection = None; continue
#             subheading_match = re.match(r'^\*\*\s*(.*?):?\s*\*\*$', line)
#             if subheading_match:
#                 finalize_buffer()
#                 subtitle = subheading_match.group(1).strip()
#                 current_subsection = {"subheading": subtitle, "content": None, "points": []}
#                 all_subsections.append(current_subsection)
#                 continue
#             bullet_match = re.match(r'^\*\s+(.*)', line)
#             if bullet_match and current_subsection:
#                 finalize_buffer()
#                 point_text = bullet_match.group(1).strip()
#                 point_object = {"text": point_text}
#                 if "points" not in current_subsection: current_subsection["points"] = []
#                 current_subsection["points"].append(point_object)
#                 continue
#             if current_subsection: content_buffer.append(line)
#         finalize_buffer()
#         for sub in all_subsections:
#              if sub.get("content") is None: sub["content"] = ""
#              if "points" not in sub: sub["points"] = []
#         logger.info("Successfully parsed markdown summary into flat subsection list.")
#         return {"subsections": all_subsections}
#     except Exception as e:
#         logger.error(f"Error parsing markdown summary: {e}", exc_info=True)
#         return {"subsections": [{"subheading":"Error", "content":f"Error parsing summary: {e}", "points":[]}]}
#
#
# # --- Build Final Payload (Keep as is) ---
# def build_final_payload_for_bubble(
#     ai_summary_text: str,
#     ai_summary_structured: Dict[str, Any],
#     internal_data: Dict,
#     external_data: Dict,
#     task_indicator: str,
#     final_status: str,
#     error_message: Optional[str]
#     ) -> Dict:
#     # (Keep existing build_final_payload_for_bubble function as is)
#     # It already includes ai_summary_structured, no specific changes needed for brand yet
#     logger.debug("Building final payload object with flat subsection list for Bubble...")
#     payload = {
#         "ai_summary_structured": ai_summary_structured,
#         "result_type_indicator": task_indicator if final_status != INDICATOR_ERROR else INDICATOR_ERROR,
#         "status": final_status,
#         "error_message": error_message,
#         "category_trend": [], "top_styles": [], "top_colors": [], "item_trend": [],
#         "item_metrics": [], "mega_trends_top": [], "web_links": [], "web_answer": None
#     }
#     internal_data = internal_data or {}
#     trends_data = internal_data.get("trends_data") if isinstance(internal_data.get("trends_data"), dict) else None
#     chart_details_data = internal_data.get("chart_details_data") if isinstance(internal_data.get("chart_details_data"), dict) else None
#     mega_trends_data = internal_data.get("mega_trends_data") if isinstance(internal_data.get("mega_trends_data"), list) else None
#     # --- Include brand performance data if present ---
#     brand_performance_data = internal_data.get("brand_performance_data") # Get brand data
#
#     external_data = external_data or {}
#     external_results = external_data.get("results", []) if isinstance(external_data.get("results", []), list) else []
#     external_answer = external_data.get("answer") if isinstance(external_data.get("answer"), str) else None
#
#     if trends_data:
#         cs = trends_data.get("category_summary"); # ...(rest of trends population)...
#         if isinstance(cs, dict):
#              chart = cs.get("chart_data")
#              if isinstance(chart, list) and chart: payload["category_trend"] = chart; logger.debug("Populated category_trend list.")
#         all_styles = trends_data.get("style_details", [])
#         if isinstance(all_styles, list):
#              valid_styles = [s for s in all_styles if isinstance(s,dict)]
#              if valid_styles:
#                  top_styles_list = sorted(valid_styles, key=lambda x: x.get('average_volume', 0), reverse=True)[:5]
#                  payload["top_styles"] = [{"name": s.get("style_name"), "growth": s.get("growth_recent"), "volume": s.get("average_volume")} for s in top_styles_list]; logger.debug("Populated top_styles list.")
#         all_colors = trends_data.get("color_details", [])
#         if isinstance(all_colors, list):
#             valid_colors = [c for c in all_colors if isinstance(c,dict)]
#             if valid_colors:
#                 top_colors_list = sorted(valid_colors, key=lambda x: x.get('average_volume', 0), reverse=True)[:5]
#                 payload["top_colors"] = [{"name": c.get("color_name"), "growth": c.get("growth_recent"), "volume": c.get("average_volume")} for c in top_colors_list]; logger.debug("Populated top_colors list.")
#
#     if chart_details_data: # ...(rest of item population)...
#          item_metrics_obj = {
#              "name": chart_details_data.get("category_subject"), "growth": chart_details_data.get("growth_recent"),
#              "volume": chart_details_data.get("average_volume"), "forecast_growth": None, "forecast_volume": None }
#          if task_indicator == INDICATOR_FORECAST:
#              item_metrics_obj["forecast_growth"] = {"f2": chart_details_data.get("f2"), "f3": chart_details_data.get("f3"), "f6": chart_details_data.get("f6")}
#              item_metrics_obj["forecast_volume"] = {"avg2": chart_details_data.get("avg2"), "avg3": chart_details_data.get("avg3"), "avg6": chart_details_data.get("avg6")}
#          payload["item_metrics"] = [item_metrics_obj]; logger.debug("Populated item_metrics list.")
#          item_chart = chart_details_data.get("chart_data")
#          if isinstance(item_chart, list) and item_chart: payload["item_trend"] = item_chart; logger.debug("Populated item_trend list.")
#
#     if mega_trends_data: # ...(rest of mega trends population)...
#         valid_mega = [m for m in mega_trends_data if isinstance(m,dict)]
#         if valid_mega:
#             top_mega_list = sorted(valid_mega, key=lambda x: x.get('growth_recent', 0), reverse=True)[:10]
#             payload["mega_trends_top"] = [{"name": m.get("query_name"), "growth": m.get("growth_recent"), "volume": m.get("average_volume"), "category": m.get("category_name")} for m in top_mega_list]; logger.debug("Populated mega_trends_top list.")
#
#     # --- *** MODIFICATION START: Add brand performance data to payload *** ---
#     # Add the structured brand performance list directly if it exists
#     if brand_performance_data and isinstance(brand_performance_data, list):
#          payload["brand_performance_summary"] = brand_performance_data # Add the list
#          logger.debug(f"Populated brand_performance_summary list with {len(brand_performance_data)} items.")
#     # --- *** MODIFICATION END *** ---
#
#     if external_results: # ...(rest of web links population)...
#          web_links_list = [{"title": r.get("title"), "url": r.get("url")} for r in external_results if isinstance(r, dict) and r.get("title") and r.get("url")][:5]
#          if web_links_list: payload["web_links"] = web_links_list; logger.debug("Populated web_links list.")
#     if external_answer: payload["web_answer"] = external_answer; logger.debug("Populated web_answer string.")
#
#     logger.info(f"Built final bubble payload object with keys: {list(payload.keys())}")
#     return payload
#
#
# # --- Main Lambda Handler ---
# def lambda_handler(event, context):
#     logger.info(f"Received combined event: {json.dumps(event)}")
#
#     if not GEMINI_SDK_AVAILABLE:
#         error_payload = build_final_payload_for_bubble("Error: LLM SDK unavailable.", {"subsections": []}, {}, {}, INDICATOR_ERROR, INDICATOR_ERROR, "LLM SDK unavailable.")
#         return {"statusCode": 500, "body": json.dumps(error_payload)}
#     if BOTO3_CLIENT_ERROR:
#         error_payload = build_final_payload_for_bubble(f"Error: {BOTO3_CLIENT_ERROR}", {"subsections": []}, {}, {}, INDICATOR_ERROR, INDICATOR_ERROR, BOTO3_CLIENT_ERROR)
#         return {"statusCode": 500, "body": json.dumps(error_payload)}
#
#     internal_data = event.get("internal_data", {})
#     external_data = event.get("external_data", {})
#     interpretation = internal_data.get("interpretation", {}) if isinstance(internal_data, dict) else {}
#     original_context = interpretation.get("original_context", {}) if isinstance(interpretation, dict) else {}
#     primary_task = interpretation.get("primary_task")
#     user_query = original_context.get("query", "the user query")
#     # --- *** MODIFICATION START: Extract target brand for prompt formatting *** ---
#     query_subjects = interpretation.get("query_subjects", {}) if isinstance(interpretation.get("query_subjects"), dict) else {}
#     target_brand = query_subjects.get("target_brand") # Get brand if available
#     # --- *** MODIFICATION END *** ---
#
#     upstream_errors = []
#     if isinstance(internal_data, dict) and internal_data.get("errors"): upstream_errors.extend(internal_data["errors"])
#     if isinstance(internal_data, dict) and internal_data.get("errorType"): upstream_errors.append({"source": "FetchInternalDataRouter", "error": internal_data.get("errorType"), "details": internal_data.get("cause", internal_data.get("errorMessage"))})
#     if isinstance(external_data, dict) and external_data.get("error"): upstream_errors.append({"source": "FetchExternalContext", "error": external_data["error"]})
#     if isinstance(external_data, dict) and external_data.get("errorType"): upstream_errors.append({"source": "FetchExternalContext", "error": external_data.get("errorType"), "details": external_data.get("cause", external_data.get("errorMessage"))})
#
#     # --- *** MODIFICATION START: Pass task to get_task_details *** ---
#     result_type_indicator, prompt_template = get_task_details(primary_task)
#     # --- *** MODIFICATION END *** ---
#     logger.info(f"Using result indicator: {result_type_indicator}")
#
#     specific_item_name = "N/A"
#     if isinstance(interpretation.get("query_subjects"), dict):
#          specific_known = query_subjects.get("specific_known", [])
#          if specific_known and isinstance(specific_known[0], dict): specific_item_name = specific_known[0].get("subject", "N/A")
#
#     formatted_data_context = ""
#     try:
#         # --- Pass primary_task to format_data_for_prompt ---
#         formatted_data_context = format_data_for_prompt(internal_data, external_data) # Updated function handles task internally
#
#         # --- Format the prompt, including brand_domain if it's a brand task ---
#         prompt_format_args = {
#             "specific_item_name": specific_item_name,
#             "category_name": original_context.get('category', 'N/A'), # Still needed for some prompts
#             "country_name": original_context.get('country', 'N/A'),   # Still needed for some prompts
#             "user_query": user_query,
#             "brand_domain": target_brand or "N/A" # Add brand_domain for the new prompt
#         }
#         synthesis_prompt = prompt_template.format(**prompt_format_args)
#         synthesis_prompt += "\n\n" + formatted_data_context
#         logger.debug(f"Constructed Synthesis Prompt:\n{synthesis_prompt}")
#     except KeyError as key_err:
#         logger.error(f"Missing key in prompt template formatting: {key_err}. Prompt Key: {result_type_indicator}", exc_info=True)
#         error_payload = build_final_payload_for_bubble(f"Error: Could not prepare prompt. Missing key: {key_err}", {"subsections": []}, {}, {}, INDICATOR_ERROR, INDICATOR_ERROR, str(key_err))
#         return {"statusCode": 500, "body": json.dumps(error_payload)}
#     except Exception as e:
#         logger.error(f"Error formatting data for synthesis prompt: {e}", exc_info=True)
#         error_payload = build_final_payload_for_bubble(f"Error: Could not prepare data for AI synthesis. {e}", {"subsections": []}, {}, {}, INDICATOR_ERROR, INDICATOR_ERROR, str(e))
#         return {"statusCode": 500, "body": json.dumps(error_payload)}
#
#
#     google_api_key = get_secret_value(SECRET_NAME, "GOOGLE_API_KEY")
#     if not google_api_key:
#         error_payload = build_final_payload_for_bubble("Error: API key configuration error (Google).", {"subsections": []}, internal_data, external_data, INDICATOR_ERROR, INDICATOR_ERROR, "API key config error")
#         return {"statusCode": 500, "body": json.dumps(error_payload)}
#
#     ai_summary_text = "Error: AI synthesis failed."
#     llm_error = None
#     try:
#         if not formatted_data_context or formatted_data_context.startswith("No specific data available"):
#              logger.warning("Skipping LLM call as no significant data was formatted for prompt.")
#              ai_summary_text = PERSONA_PROMPTS[INDICATOR_UNKNOWN].format(
#                  user_query=user_query,
#                  category_name=original_context.get('category', 'N/A'),
#                  country_name=original_context.get('country', 'N/A')
#              )
#              result_type_indicator = INDICATOR_UNKNOWN
#         else:
#              logger.info(f"Calling Synthesis LLM: {SYNTHESIS_LLM_MODEL}...")
#              genai.configure(api_key=google_api_key)
#              model = genai.GenerativeModel(SYNTHESIS_LLM_MODEL)
#              response = model.generate_content(synthesis_prompt)
#              logger.info("Synthesis LLM response received.")
#              ai_summary_text = response.text
#              logger.debug(f"Synthesis LLM Raw Response Text:\n{ai_summary_text}")
#
#     except Exception as e:
#         logger.error(f"Synthesis LLM call failed: {e}", exc_info=True)
#         llm_error = f"Synthesis LLM call failed: {str(e)}"
#         ai_summary_text = "An error occurred during the analysis synthesis."
#
#     final_bubble_payload = {}
#     ai_summary_structured = {}
#
#     try:
#         logger.info("Attempting to parse AI summary markdown...")
#         ai_summary_structured = parse_markdown_flat_subsections(ai_summary_text) # Use the flat parser
#
#         final_status = "success"
#         user_error_message = None
#
#         if llm_error:
#             final_status = INDICATOR_ERROR; user_error_message = llm_error; result_type_indicator = INDICATOR_ERROR
#             ai_summary_structured = { "subsections": [{"subheading":"Error", "content": ai_summary_text, "points":[]}] }
#         elif upstream_errors:
#             final_status = "partial_data_success"; logger.warning(f"Upstream errors detected: {upstream_errors}")
#             if ai_summary_structured.get("subsections"):
#                  intro_prefix = "Note: Analysis may be incomplete due to errors fetching some data.\n\n"
#                  first_sub = ai_summary_structured["subsections"][0]
#                  if first_sub.get("content"): first_sub["content"] = intro_prefix + first_sub["content"]
#                  else: first_sub["content"] = intro_prefix.strip()
#
#         final_bubble_payload = build_final_payload_for_bubble(
#             ai_summary_text=ai_summary_text,
#             ai_summary_structured=ai_summary_structured,
#             internal_data=internal_data,
#             external_data=external_data,
#             task_indicator=result_type_indicator,
#             final_status=final_status,
#             error_message=user_error_message
#         )
#         logger.info(f"Final payload status: {final_status}")
#
#     except Exception as e:
#          logger.error(f"Error during summary parsing or final payload building: {e}", exc_info=True)
#          final_bubble_payload = {
#              "ai_summary_structured": { "subsections": [{"subheading":"Error", "content":f"An critical error occurred preparing the final response: {e}", "points":[]}] },
#              "result_type_indicator": INDICATOR_ERROR, "status": INDICATOR_ERROR, "error_message": f"Payload construction/parsing error: {str(e)}",
#              "category_trend": [], "top_styles": [], "top_colors": [], "item_trend": [], "item_metrics": [], "mega_trends_top": [], "web_links": [], "web_answer": None
#          }
#
#     return {
#         "statusCode": 200,
#         "body": json.dumps(final_bubble_payload)
#     }

import json
import logging
import os
import re
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

# --- Configuration and Constants ---
SECRET_NAME = os.environ.get("SECRET_NAME", "YourSecretsName")
SYNTHESIS_LLM_MODEL = os.environ.get("SYNTHESIS_LLM_MODEL", "gemini-2.0-flash")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")

# --- Result Type Indicators ---
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
INDICATOR_BRAND_ANALYSIS = "BRAND_ANALYSIS"
# --- *** MODIFICATION START: Add Amazon Radar Indicator *** ---
INDICATOR_AMAZON_RADAR = "AMAZON_RADAR" # New indicator
# --- *** MODIFICATION END *** ---

# --- Logger Setup ---
logger = logging.getLogger()
log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
if log_level_str not in valid_log_levels: log_level_str = "INFO"
logger.setLevel(log_level_str)
logger.info(f"Logger initialized with level: {log_level_str}")
logger.info(f"SYNTHESIS_LLM_MODEL: {SYNTHESIS_LLM_MODEL}")
logger.info(f"SECRET_NAME: {SECRET_NAME}")

# --- Boto3 Client and Secret Handling ---
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
            logger.error(f"Key '{key_name}' not found/not string in '{secret_name}'."); API_KEY_CACHE[cache_key] = None; return None
        API_KEY_CACHE[cache_key] = key_value; logger.info(f"Key '{key_name}' successfully retrieved and cached."); return key_value
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code"); logger.error(f"AWS ClientError for '{secret_name}': {error_code}"); API_KEY_CACHE[cache_key] = None; return None
    except Exception as e:
        logger.exception(f"Unexpected error retrieving secret '{secret_name}'."); API_KEY_CACHE[cache_key] = None; return None

# --- *** MODIFICATION START: Add Amazon Radar Prompt *** ---
PERSONA_PROMPTS = {
    INDICATOR_TREND_DETAIL: """You are a TrendForecast.io Expert Analyst... (Keep existing)""",
    INDICATOR_MEGA_TREND: """You are a TrendForecast.io Senior Strategist... (Keep existing)""",
    INDICATOR_CATEGORY_OVERVIEW: """You are a TrendForecast.io Market Analyst... (Keep existing)""",
    INDICATOR_FORECAST: """You are a TrendForecast.io Forecast Specialist... (Keep existing)""",
    INDICATOR_BRAND_ANALYSIS: """You are a TrendForecast.io Brand Analyst. Analyze the provided internal performance data (estimated visits/growth per country) for the brand '{brand_domain}' and any relevant external web context... (Keep existing)""",
    # --- New Amazon Radar Prompt ---
    INDICATOR_AMAZON_RADAR: """You are a TrendForecast.io Amazon Market Analyst. Your task is to summarize key findings from the provided Amazon product data for the '{target_category}' category in the '{target_department}' department for country '{country_name}'.
The data includes a list of top products by revenue, and overall market size context.

Instructions:
1. Briefly state the category, department, and country being analyzed.
2. Based on the 'Top Amazon Products' list, identify and discuss the top 3-5 products. For each, mention:
    - A descriptive name (you might infer this from the product URL or ASIN if not directly provided, or simply use the ASIN).
    - Key metrics like estimated revenue, number of orders (if available), product price, and star rating.
3. Note any common characteristics among these top products (e.g., price range, popular features mentioned if discernible, saturation level).
4. Refer to the 'Category Department Market Size' information to provide context on the overall market share or revenue contribution of this category within the department.
5. Conclude with a brief summary of the Amazon landscape for this specific category/department.
6. Structure your response clearly using markdown formatting (e.g., use '**Subheadings**' for different parts like 'Top Products', 'Market Context', 'Overall Summary', and use '* Bullet points' for lists). DO NOT use '###' markdown headings.
7. Keep the tone objective and data-driven. Focus ONLY on the provided data.""",
    # --- End New Amazon Radar Prompt ---
    INDICATOR_COMPARISON: """You are a TrendForecast.io Comparative Analyst... [TODO: Define prompt]""",
    INDICATOR_RECOMMENDATION: """You are a TrendForecast.io Recommendation Engine... [TODO: Define prompt]""",
    INDICATOR_QA_WEB: """You are a helpful AI assistant... (Keep existing)""",
    INDICATOR_QA_INTERNAL: """You are a helpful AI assistant... (Keep existing)""",
    INDICATOR_QA_COMBINED: """You are a helpful AI assistant... (Keep existing)""",
    INDICATOR_UNKNOWN: """You are a helpful AI assistant... (Keep existing)""",
    INDICATOR_ERROR: "Error processing request.",
}
# --- *** MODIFICATION END *** ---


# --- *** MODIFICATION START: Update get_task_details *** ---
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
    elif primary_task == "analyze_brand_deep_dive": indicator = INDICATOR_BRAND_ANALYSIS
    # --- Add mapping for Amazon Radar task ---
    elif primary_task == "summarize_amazon_radar": # Match task name from Interpreter
        indicator = INDICATOR_AMAZON_RADAR
    # --- End mapping ---
    elif primary_task == "unknown": indicator = INDICATOR_UNKNOWN
    elif primary_task == "error": indicator = INDICATOR_ERROR

    prompt_template = PERSONA_PROMPTS.get(indicator, PERSONA_PROMPTS[INDICATOR_UNKNOWN])
    logger.info(f"Mapped primary_task '{primary_task}' to indicator '{indicator}'.")
    return indicator, prompt_template
# --- *** MODIFICATION END *** ---

# --- *** MODIFICATION START: Update format_data_for_prompt *** ---
def format_data_for_prompt(internal_data: Dict, external_data: Dict) -> str:
    prompt_parts = []
    internal_data = internal_data or {}
    external_data = external_data or {} # Ensure external_data is a dict
    interpretation = internal_data.get("interpretation") if isinstance(internal_data.get("interpretation"), dict) else {}
    original_context = interpretation.get("original_context") if isinstance(interpretation.get("original_context"), dict) else {}
    query_subjects = interpretation.get("query_subjects") if isinstance(interpretation.get("query_subjects"), dict) else {}
    specific_known = query_subjects.get("specific_known", []) if isinstance(query_subjects.get("specific_known"), list) else []
    primary_task = interpretation.get("primary_task")
    target_brand = query_subjects.get("target_brand")

    # --- Basic Context (Always Add) ---
    prompt_parts.append("CONTEXT:")
    prompt_parts.append(f"- User Query: {original_context.get('query', 'N/A')}")
    # Adjust context display based on task
    if primary_task == "analyze_brand_deep_dive" and target_brand:
         prompt_parts.append(f"- Brand Focus: {target_brand}")
         prompt_parts.append(f"- Country Context (if applicable for web search): {original_context.get('country', 'N/A')}")
    elif primary_task == "summarize_amazon_radar":
         prompt_parts.append(f"- Amazon Radar For: Category '{original_context.get('target_category', 'N/A')}', Department '{original_context.get('target_department', 'N/A')}'")
         prompt_parts.append(f"- Country: {original_context.get('country', 'N/A')}")
    else:
        prompt_parts.append(f"- Category: {original_context.get('category', 'N/A')}") # Original category from input
        prompt_parts.append(f"- Country: {original_context.get('country', 'N/A')}")
        if specific_known and isinstance(specific_known[0], dict):
            specific_item_name = specific_known[0].get("subject", "N/A")
            prompt_parts.append(f"- Specific Focus Item: {specific_item_name}")

    prompt_parts.append("\nAVAILABLE DATA:")
    data_found = False

    # --- Format Brand Performance Data ---
    if primary_task == "analyze_brand_deep_dive":
        brand_perf_data = internal_data.get("brand_performance_data")
        if isinstance(brand_perf_data, list) and brand_perf_data:
            data_found = True; prompt_parts.append(f"\nInternal Brand Performance Data ({target_brand or 'Requested Brand'}):")
            for country_data in brand_perf_data[:10]: # Limit to top 10 countries for prompt
                if isinstance(country_data, dict):
                    country = country_data.get('country', 'N/A')
                    visits = country_data.get('estimated_monthly_visits', 'N/A')
                    growth = country_data.get('estimated_growth_percentage', 'N/A')
                    growth_str = f"{growth:.1f}%" if isinstance(growth, (int, float)) else ("N/A" if growth is None else str(growth))
                    visits_str = f"{visits:,}" if isinstance(visits, (int, float)) else "N/A"
                    prompt_parts.append(f"- {country}: Est. Visits={visits_str}, Est. Growth={growth_str}")
        else:
            logger.debug("brand_performance_data missing or invalid for brand analysis task.")
            prompt_parts.append("\nInternal Brand Performance Data: - Data not available.")

    # --- Format Amazon Radar Data ---
    elif primary_task == "summarize_amazon_radar":
        amazon_data = internal_data.get("amazon_radar_data")
        if isinstance(amazon_data, dict):
            products = amazon_data.get("country_department_category", [])
            market_info = amazon_data.get("category_dep_market_size")

            if isinstance(products, list) and products:
                data_found = True
                prompt_parts.append("\nTop Amazon Products (Max 10 Displayed):")
                for i, p_item in enumerate(products[:10]): # Pass max 10 products to LLM
                    if isinstance(p_item, dict):
                        name = p_item.get("asin", "N/A") # Use ASIN if name not directly available
                        price = p_item.get("product_price", "N/A")
                        currency = p_item.get("currency", "")
                        rating = p_item.get("product_star_rating", "N/A")
                        revenue = p_item.get("estimated_revenue", "N/A")
                        orders = p_item.get("estimated_orders", "N/A")
                        prompt_parts.append(
                            f"- Product {i+1} (ASIN: {name}): Price={price} {currency}, Rating={rating}/5, Est. Revenue={revenue:,.0f}, Est. Orders={orders:,.0f}"
                        )
            else:
                logger.debug("Amazon product list missing or empty for radar task.")
                prompt_parts.append("\nTop Amazon Products: - Data not available.")

            if isinstance(market_info, dict):
                data_found = True # Even if only market info is found
                prompt_parts.append("\nAmazon Category Department Market Size Context:")
                share = market_info.get("department_in_country_share", "N/A")
                prompt_parts.append(f"- Category's Share in Department: {share}% (approx.)")
                # Could add top few from pie_chart if concise
            else:
                logger.debug("Amazon market info missing for radar task.")
                prompt_parts.append("\nAmazon Category Department Market Size Context: - Data not available.")
        else:
            logger.debug("amazon_radar_data missing or invalid for radar task.")
            prompt_parts.append("\nAmazon Radar Data: - Data not available.")

    # --- Format Other Internal Data (Conditionally, if not brand or amazon task) ---
    if primary_task not in ["analyze_brand_deep_dive", "summarize_amazon_radar"]:
        # Category Context
        trends = internal_data.get("trends_data") # ... (keep existing logic) ...
        if isinstance(trends, dict):
            data_found = True; prompt_parts.append("\nInternal Category Context:")
            cs = trends.get("category_summary"); # ...
            if isinstance(cs, dict): prompt_parts.append(f"- Overall Category ({cs.get('category_name', 'N/A')}): Avg Vol={cs.get('average_volume', 'N/A')}, Growth={cs.get('growth_recent', 'N/A'):.1f}%")
            style_details = trends.get("style_details", []); color_details = trends.get("color_details", [])
            if isinstance(style_details, list):
                top_styles = sorted([s for s in style_details if isinstance(s,dict)], key=lambda x: x.get('average_volume', 0), reverse=True)[:3]
                if top_styles: prompt_parts.append(f"- Top Styles in Category: {', '.join([s.get('style_name', 'N/A') for s in top_styles])}")
            if isinstance(color_details, list):
                top_colors = sorted([c for c in color_details if isinstance(c,dict)], key=lambda x: x.get('average_volume', 0), reverse=True)[:3]
                if top_colors: prompt_parts.append(f"- Top Colors in Category: {', '.join([c.get('color_name', 'N/A') for c in top_colors])}")
        else: logger.debug("trends_data missing or not a dict.")

        # Item Details
        if primary_task not in ["summarize_mega_trends", "summarize_category"]: # ... (keep existing logic) ...
            details = internal_data.get("chart_details_data")
            if isinstance(details, dict):
                 data_found = True; prompt_parts.append(f"\nInternal Specific Item Details ({details.get('category_subject', 'N/A')}):")
                 prompt_parts.append(f"- Avg Vol={details.get('average_volume', 'N/A')}, Growth={details.get('growth_recent', 'N/A'):.1f}%")
                 if primary_task == "get_forecast":
                     prompt_parts.append(f"- Data Point f2 (2m Growth %): {details.get('f2', 'N/A')}"); prompt_parts.append(f"- Data Point f3 (3m Growth %): {details.get('f3', 'N/A')}"); prompt_parts.append(f"- Data Point f6 (6m Growth %): {details.get('f6', 'N/A')}")
                     prompt_parts.append(f"- Data Point avg2 (2m Avg Volume): {details.get('avg2', 'N/A')}"); prompt_parts.append(f"- Data Point avg3 (3m Avg Volume): {details.get('avg3', 'N/A')}"); prompt_parts.append(f"- Data Point avg6 (6m Avg Volume): {details.get('avg6', 'N/A')}")
            else: logger.debug("chart_details_data missing or not a dict.")

        # Mega Trends
        if primary_task == "summarize_mega_trends": # ... (keep existing logic) ...
            mega = internal_data.get("mega_trends_data")
            if isinstance(mega, list) and mega:
                data_found = True; prompt_parts.append("\nInternal Mega Trends (Top Queries):")
                top_mega = sorted([m for m in mega if isinstance(m,dict)], key=lambda x: x.get('growth_recent', 0), reverse=True)[:5]
                if top_mega:
                    for m in top_mega: prompt_parts.append(f"- Query: '{m.get('query_name', 'N/A')}', Related Category: {m.get('category_name', 'N/A')}, Growth: {m.get('growth_recent', 'N/A'):.1f}%")
                else: prompt_parts.append("- No specific mega trend query data found.")
            else: logger.debug("mega_trends_data is missing or invalid for mega_trends task."); prompt_parts.append("\nInternal Mega Trends (Top Queries): - Data not available.")

    # --- External Web Context (Always include if available for any task needing web search) ---
    if "web_search" in interpretation.get("required_sources", []):
        ext_answer = external_data.get("answer")
        ext_results = external_data.get("results", [])
        if not isinstance(ext_results, list): ext_results = []

        if ext_answer or ext_results:
             data_found = True
             prompt_parts.append("\nExternal Web Context:")
             if ext_answer: prompt_parts.append(f"- Synthesized Answer: {ext_answer}")
             if ext_results:
                  prompt_parts.append("- Top Results Snippets:")
                  for i, res in enumerate(ext_results[:3]):
                       if isinstance(res, dict):
                          title = res.get('title', 'N/A'); content_snippet = res.get('content', '')[:150]
                          prompt_parts.append(f"  - [{i+1}] {title}: {content_snippet}...")
        else:
             logger.debug("Web search requested, but no external web data (answer or results) available for prompt.")
             prompt_parts.append("\nExternal Web Context: - No relevant web context found or provided.")

    # --- Final check ---
    if not data_found:
        logger.warning("No significant internal or external data blocks were available to format for prompt.")
        query = original_context.get('query', 'N/A')
        if primary_task == "analyze_brand_deep_dive": brand = target_brand or 'the brand'; return f"No specific data or web context available to analyze for '{brand}'."
        elif primary_task == "summarize_amazon_radar": cat = original_context.get('target_category','N/A'); dept = original_context.get('target_department','N/A'); return f"No Amazon product data available for category '{cat}' in department '{dept}'."
        else: cat = original_context.get('category', 'N/A'); country = original_context.get('country', 'N/A'); return f"No specific data available for query '{query}' on category '{cat}' in {country}."

    return "\n".join(prompt_parts)
# --- *** MODIFICATION END *** ---


# --- Parser Function (Keep flat subsection parser) ---
def parse_markdown_flat_subsections(markdown_text: str) -> Dict[str, Any]:
    # (Keep the parse_markdown_flat_subsections function as defined previously)
    if not markdown_text or not isinstance(markdown_text, str): return {"subsections": []}
    all_subsections: List[Dict] = []; current_subsection: Optional[Dict] = None; content_buffer: List[str] = []
    lines = markdown_text.strip().split('\n')
    def finalize_buffer():
        nonlocal content_buffer, current_subsection
        if not content_buffer or current_subsection is None: content_buffer = []; return
        text = "\n".join(content_buffer).strip()
        if text:
            if current_subsection.get("content") is None: current_subsection["content"] = text
            else: current_subsection["content"] += "\n" + text
        content_buffer = []
    try:
        for line in lines:
            line = line.strip()
            if not line: finalize_buffer(); continue
            if line.startswith("###"): finalize_buffer(); current_subsection = None; continue
            subheading_match = re.match(r'^\*\*\s*(.*?):?\s*\*\*$', line)
            if subheading_match:
                finalize_buffer(); subtitle = subheading_match.group(1).strip()
                current_subsection = {"subheading": subtitle, "content": None, "points": []}
                all_subsections.append(current_subsection); continue
            bullet_match = re.match(r'^\*\s+(.*)', line)
            if bullet_match and current_subsection:
                finalize_buffer(); point_text = bullet_match.group(1).strip(); point_object = {"text": point_text}
                if "points" not in current_subsection: current_subsection["points"] = []
                current_subsection["points"].append(point_object); continue
            if current_subsection: content_buffer.append(line)
        finalize_buffer()
        for sub in all_subsections:
             if sub.get("content") is None: sub["content"] = ""
             if "points" not in sub: sub["points"] = []
        logger.info("Successfully parsed markdown summary into flat subsection list.")
        return {"subsections": all_subsections}
    except Exception as e:
        logger.error(f"Error parsing markdown summary: {e}", exc_info=True)
        return {"subsections": [{"subheading":"Error", "content":f"Error parsing summary: {e}", "points":[]}]}

# --- *** MODIFICATION START: Update build_final_payload_for_bubble *** ---
def build_final_payload_for_bubble(
    ai_summary_text: str,
    ai_summary_structured: Dict[str, Any],
    internal_data: Dict,
    external_data: Dict,
    task_indicator: str,
    final_status: str,
    error_message: Optional[str]
    ) -> Dict:
    logger.debug("Building final payload object for Bubble...")
    payload = {
        "ai_summary_structured": ai_summary_structured,
        "result_type_indicator": task_indicator if final_status != INDICATOR_ERROR else INDICATOR_ERROR,
        "status": final_status,
        "error_message": error_message,
        "category_trend": [], "top_styles": [], "top_colors": [], "item_trend": [],
        "item_metrics": [], "mega_trends_top": [], "web_links": [], "web_answer": None,
        "brand_performance_summary": [], # Initialize new key for brand data
        "amazon_radar_products": []     # Initialize new key for Amazon data
    }
    # Ensure internal_data and external_data are dicts
    internal_data = internal_data or {}
    external_data = external_data or {}

    # Populate standard data lists
    trends_data = internal_data.get("trends_data") if isinstance(internal_data.get("trends_data"), dict) else None
    if trends_data: # ... (keep existing population logic) ...
        cs = trends_data.get("category_summary");
        if isinstance(cs, dict):
             chart = cs.get("chart_data")
             if isinstance(chart, list) and chart: payload["category_trend"] = chart
        all_styles = trends_data.get("style_details", [])
        if isinstance(all_styles, list):
             valid_styles = [s for s in all_styles if isinstance(s,dict)]
             if valid_styles: payload["top_styles"] = [{"name": s.get("style_name"), "growth": s.get("growth_recent"), "volume": s.get("average_volume")} for s in sorted(valid_styles, key=lambda x: x.get('average_volume', 0), reverse=True)[:5]]
        all_colors = trends_data.get("color_details", [])
        if isinstance(all_colors, list):
            valid_colors = [c for c in all_colors if isinstance(c,dict)]
            if valid_colors: payload["top_colors"] = [{"name": c.get("color_name"), "growth": c.get("growth_recent"), "volume": c.get("average_volume")} for c in sorted(valid_colors, key=lambda x: x.get('average_volume', 0), reverse=True)[:5]]

    chart_details_data = internal_data.get("chart_details_data") if isinstance(internal_data.get("chart_details_data"), dict) else None
    if chart_details_data: # ... (keep existing population logic) ...
         item_metrics_obj = {"name": chart_details_data.get("category_subject"), "growth": chart_details_data.get("growth_recent"), "volume": chart_details_data.get("average_volume"), "forecast_growth": None, "forecast_volume": None }
         if task_indicator == INDICATOR_FORECAST:
             item_metrics_obj["forecast_growth"] = {"f2": chart_details_data.get("f2"), "f3": chart_details_data.get("f3"), "f6": chart_details_data.get("f6")}
             item_metrics_obj["forecast_volume"] = {"avg2": chart_details_data.get("avg2"), "avg3": chart_details_data.get("avg3"), "avg6": chart_details_data.get("avg6")}
         payload["item_metrics"] = [item_metrics_obj]
         item_chart = chart_details_data.get("chart_data")
         if isinstance(item_chart, list) and item_chart: payload["item_trend"] = item_chart

    mega_trends_data = internal_data.get("mega_trends_data") if isinstance(internal_data.get("mega_trends_data"), list) else None
    if mega_trends_data: # ... (keep existing population logic) ...
        valid_mega = [m for m in mega_trends_data if isinstance(m,dict)]
        if valid_mega: payload["mega_trends_top"] = [{"name": m.get("query_name"), "growth": m.get("growth_recent"), "volume": m.get("average_volume"), "category": m.get("category_name")} for m in sorted(valid_mega, key=lambda x: x.get('growth_recent', 0), reverse=True)[:10]]

    # Populate brand performance data
    brand_performance_data = internal_data.get("brand_performance_data")
    if brand_performance_data and isinstance(brand_performance_data, list):
         payload["brand_performance_summary"] = brand_performance_data
         logger.debug(f"Populated brand_performance_summary list with {len(brand_performance_data)} items.")

    # Populate Amazon Radar data
    amazon_radar_data_from_internal = internal_data.get("amazon_radar_data")
    if task_indicator == INDICATOR_AMAZON_RADAR and isinstance(amazon_radar_data_from_internal, dict):
        products_list = amazon_radar_data_from_internal.get("country_department_category", [])
        if isinstance(products_list, list):
            # Select key fields and limit to max 10 products for Bubble
            cleaned_products = []
            for p_item in products_list[:10]:
                if isinstance(p_item, dict):
                    cleaned_products.append({
                        "asin": p_item.get("asin"),
                        "product_url": p_item.get("product_url"),
                        "product_photo": p_item.get("product_photo"),
                        "product_price": p_item.get("product_price"),
                        "currency": p_item.get("currency"),
                        "estimated_revenue": p_item.get("estimated_revenue"),
                        "estimated_orders": p_item.get("estimated_orders"),
                        "number_of_reviews": p_item.get("number_of_reviews"),
                        "product_star_rating": p_item.get("product_star_rating"),
                        "saturation": p_item.get("saturation")
                        # Add inferred name if needed:
                        # "inferred_name": p_item.get("asin") # Or derive from URL
                    })
            payload["amazon_radar_products"] = cleaned_products
            logger.debug(f"Populated amazon_radar_products list with {len(cleaned_products)} items.")
        # Optionally, add market size info to payload if needed by UI
        # market_info = amazon_radar_data_from_internal.get("category_dep_market_size")
        # if isinstance(market_info, dict): payload["amazon_market_context"] = market_info

    # Populate web links and answer
    external_results = external_data.get("results", []) if isinstance(external_data.get("results", []), list) else []
    external_answer = external_data.get("answer") if isinstance(external_data.get("answer"), str) else None
    if external_results:
         payload["web_links"] = [{"title": r.get("title"), "url": r.get("url")} for r in external_results if isinstance(r, dict) and r.get("title") and r.get("url")][:5]
    if external_answer: payload["web_answer"] = external_answer

    logger.info(f"Built final bubble payload object with keys: {list(payload.keys())}")
    return payload
# --- *** MODIFICATION END *** ---


# --- Main Lambda Handler ---
def lambda_handler(event, context):
    logger.info(f"Received combined event: {json.dumps(event)}")

    # Pre-checks (ensure fallback structures match)
    if not GEMINI_SDK_AVAILABLE:
        error_payload = build_final_payload_for_bubble("Error: LLM SDK unavailable.", {"subsections": []}, {}, {}, INDICATOR_ERROR, INDICATOR_ERROR, "LLM SDK unavailable.")
        return {"statusCode": 500, "body": json.dumps(error_payload)}
    if BOTO3_CLIENT_ERROR:
        error_payload = build_final_payload_for_bubble(f"Error: {BOTO3_CLIENT_ERROR}", {"subsections": []}, {}, {}, INDICATOR_ERROR, INDICATOR_ERROR, BOTO3_CLIENT_ERROR)
        return {"statusCode": 500, "body": json.dumps(error_payload)}

    # Data extraction
    internal_data = event.get("internal_data", {})
    external_data = event.get("external_data", {})
    interpretation = internal_data.get("interpretation", {}) if isinstance(internal_data, dict) else {}
    original_context = interpretation.get("original_context", {}) if isinstance(interpretation, dict) else {}
    primary_task = interpretation.get("primary_task")
    user_query = original_context.get("query", "the user query")
    query_subjects = interpretation.get("query_subjects", {}) if isinstance(interpretation.get("query_subjects"), dict) else {}
    target_brand = query_subjects.get("target_brand") # For brand tasks
    # For Amazon Radar, specific target category/department from original_context
    amazon_target_category = original_context.get("target_category", "N/A")
    amazon_target_department = original_context.get("target_department", "N/A")


    upstream_errors = [] # (Keep upstream error collection logic)
    if isinstance(internal_data, dict) and internal_data.get("errors"): upstream_errors.extend(internal_data["errors"])
    if isinstance(internal_data, dict) and internal_data.get("errorType"): upstream_errors.append({"source": "FetchInternalDataRouter", "error": internal_data.get("errorType"), "details": internal_data.get("cause", internal_data.get("errorMessage"))})
    if isinstance(external_data, dict) and external_data.get("error"): upstream_errors.append({"source": "FetchExternalContext", "error": external_data["error"]})
    if isinstance(external_data, dict) and external_data.get("errorType"): upstream_errors.append({"source": "FetchExternalContext", "error": external_data.get("errorType"), "details": external_data.get("cause", external_data.get("errorMessage"))})


    result_type_indicator, prompt_template = get_task_details(primary_task)
    logger.info(f"Using result indicator: {result_type_indicator}")

    specific_item_name = "N/A"
    if isinstance(interpretation.get("query_subjects"), dict):
         specific_known = query_subjects.get("specific_known", [])
         if specific_known and isinstance(specific_known[0], dict): specific_item_name = specific_known[0].get("subject", "N/A")

    formatted_data_context = ""
    try:
        formatted_data_context = format_data_for_prompt(internal_data, external_data) # Already handles task-specific formatting

        # --- Prepare arguments for prompt_template.format() ---
        prompt_format_args = {
            "specific_item_name": specific_item_name,
            "category_name": original_context.get('category', 'N/A'), # For general prompts
            "country_name": original_context.get('country', 'N/A'),   # For general prompts
            "user_query": user_query,
            "brand_domain": target_brand or "N/A", # For brand analysis prompt
            # --- Add args for Amazon Radar prompt ---
            "target_category": amazon_target_category,
            "target_department": amazon_target_department
        }
        synthesis_prompt = prompt_template.format(**prompt_format_args)
        synthesis_prompt += "\n\n" + formatted_data_context
        logger.debug(f"Constructed Synthesis Prompt:\n{synthesis_prompt}")
    except KeyError as key_err:
        logger.error(f"Missing key in prompt template formatting: {key_err}. Prompt Key: {result_type_indicator}", exc_info=True)
        error_payload = build_final_payload_for_bubble(f"Error: Could not prepare prompt. Missing key: {key_err}", {"subsections": []}, {}, {}, INDICATOR_ERROR, INDICATOR_ERROR, str(key_err))
        return {"statusCode": 500, "body": json.dumps(error_payload)}
    except Exception as e:
        logger.error(f"Error formatting data for synthesis prompt: {e}", exc_info=True)
        error_payload = build_final_payload_for_bubble(f"Error: Could not prepare data for AI synthesis. {e}", {"subsections": []}, {}, {}, INDICATOR_ERROR, INDICATOR_ERROR, str(e))
        return {"statusCode": 500, "body": json.dumps(error_payload)}


    google_api_key = get_secret_value(SECRET_NAME, "GOOGLE_API_KEY")
    if not google_api_key:
        error_payload = build_final_payload_for_bubble("Error: API key config.", {"subsections": []}, internal_data, external_data, INDICATOR_ERROR, INDICATOR_ERROR, "API key config error")
        return {"statusCode": 500, "body": json.dumps(error_payload)}

    ai_summary_text = "Error: AI synthesis failed."
    llm_error = None
    try:
        if not formatted_data_context or formatted_data_context.startswith("No specific data available"):
             logger.warning("Skipping LLM call as no significant data was formatted for prompt.")
             ai_summary_text = PERSONA_PROMPTS[INDICATOR_UNKNOWN].format(user_query=user_query, category_name=original_context.get('category', 'N/A'), country_name=original_context.get('country', 'N/A'))
             result_type_indicator = INDICATOR_UNKNOWN
        else:
             logger.info(f"Calling Synthesis LLM: {SYNTHESIS_LLM_MODEL}...")
             genai.configure(api_key=google_api_key)
             model = genai.GenerativeModel(SYNTHESIS_LLM_MODEL)
             response = model.generate_content(synthesis_prompt)
             logger.info("Synthesis LLM response received.")
             ai_summary_text = response.text
             logger.debug(f"Synthesis LLM Raw Response Text:\n{ai_summary_text}")
    except Exception as e:
        logger.error(f"Synthesis LLM call failed: {e}", exc_info=True)
        llm_error = f"Synthesis LLM call failed: {str(e)}"
        ai_summary_text = "An error occurred during the analysis synthesis."

    final_bubble_payload = {}
    ai_summary_structured = {}

    try:
        logger.info("Attempting to parse AI summary markdown...")
        ai_summary_structured = parse_markdown_flat_subsections(ai_summary_text) # Use the flat parser

        final_status = "success"; user_error_message = None
        if llm_error:
            final_status = INDICATOR_ERROR; user_error_message = llm_error; result_type_indicator = INDICATOR_ERROR
            ai_summary_structured = { "subsections": [{"subheading":"Error", "content": ai_summary_text, "points":[]}] }
        elif upstream_errors:
            final_status = "partial_data_success"; logger.warning(f"Upstream errors detected: {upstream_errors}")
            if ai_summary_structured.get("subsections"):
                 intro_prefix = "Note: Analysis may be incomplete due to errors fetching some data.\n\n"
                 first_sub = ai_summary_structured["subsections"][0]
                 if first_sub.get("content"): first_sub["content"] = intro_prefix + first_sub["content"]
                 else: first_sub["content"] = intro_prefix.strip()

        final_bubble_payload = build_final_payload_for_bubble(
            ai_summary_text=ai_summary_text, ai_summary_structured=ai_summary_structured,
            internal_data=internal_data, external_data=external_data,
            task_indicator=result_type_indicator, final_status=final_status, error_message=user_error_message
        )
        logger.info(f"Final payload status: {final_status}")
    except Exception as e:
         logger.error(f"Error during summary parsing or final payload building: {e}", exc_info=True)
         final_bubble_payload = {
             "ai_summary_structured": { "subsections": [{"subheading":"Error", "content":f"An critical error occurred preparing the final response: {e}", "points":[]}] },
             "result_type_indicator": INDICATOR_ERROR, "status": INDICATOR_ERROR, "error_message": f"Payload construction/parsing error: {str(e)}",
             "category_trend": [], "top_styles": [], "top_colors": [], "item_trend": [], "item_metrics": [], "mega_trends_top": [], "web_links": [], "web_answer": None,
             "brand_performance_summary": [], "amazon_radar_products": []
         }

    return {"statusCode": 200, "body": json.dumps(final_bubble_payload)}