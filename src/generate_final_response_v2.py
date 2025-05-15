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

# import json
# import logging
# import os
# import re
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
# INDICATOR_BRAND_ANALYSIS = "BRAND_ANALYSIS"
# # --- *** MODIFICATION START: Add Amazon Radar Indicator *** ---
# INDICATOR_AMAZON_RADAR = "AMAZON_RADAR" # New indicator
# # --- *** MODIFICATION END *** ---
# INDICATOR_WEB_SUMMARY = "WEB_SUMMARY" # New indicator
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
#     global API_KEY_CACHE; cache_key = f"{secret_name}:{key_name}"
#     if cache_key in API_KEY_CACHE: logger.debug(f"Using cached secret key: {cache_key}"); return API_KEY_CACHE[cache_key]
#     if BOTO3_CLIENT_ERROR: logger.error(f"Boto3 client error: {BOTO3_CLIENT_ERROR}"); return None
#     if not secrets_manager: logger.error("Secrets Manager client not initialized."); return None
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
#         if not isinstance(secret_dict, dict): logger.error("Parsed secret not dict."); return None
#         key_value = secret_dict.get(key_name)
#         if not key_value or not isinstance(key_value, str):
#             logger.error(f"Key '{key_name}' not found/not string in '{secret_name}'."); API_KEY_CACHE[cache_key] = None; return None
#         API_KEY_CACHE[cache_key] = key_value; logger.info(f"Key '{key_name}' successfully retrieved and cached."); return key_value
#     except ClientError as e:
#         error_code = e.response.get("Error", {}).get("Code"); logger.error(f"AWS ClientError for '{secret_name}': {error_code}"); API_KEY_CACHE[cache_key] = None; return None
#     except Exception as e:
#         logger.exception(f"Unexpected error retrieving secret '{secret_name}'."); API_KEY_CACHE[cache_key] = None; return None
#
# # --- *** MODIFICATION START: Add Amazon Radar Prompt *** ---
# PERSONA_PROMPTS = {
#     INDICATOR_TREND_DETAIL: """You are a TrendForecast.io Expert Analyst... (Keep existing)""",
#     INDICATOR_MEGA_TREND: """You are a TrendForecast.io Senior Strategist... (Keep existing)""",
#     INDICATOR_CATEGORY_OVERVIEW: """You are a TrendForecast.io Market Analyst... (Keep existing)""",
#     INDICATOR_FORECAST: """You are a TrendForecast.io Forecast Specialist... (Keep existing)""",
#     INDICATOR_BRAND_ANALYSIS: """You are a TrendForecast.io Brand Analyst. Analyze the provided internal performance data (estimated visits/growth per country) for the brand '{brand_domain}' and any relevant external web context... (Keep existing)""",
#     # --- New Amazon Radar Prompt ---
#     INDICATOR_AMAZON_RADAR: """You are a TrendForecast.io Amazon Market Analyst. Your task is to summarize key findings from the provided Amazon product data for the '{target_category}' category in the '{target_department}' department for country '{country_name}'.
# The data includes a list of top products by revenue, and overall market size context.
#
# Instructions:
# 1. Briefly state the category, department, and country being analyzed.
# 2. Based on the 'Top Amazon Products' list, identify and discuss the top 3-5 products. For each, mention:
#     - A descriptive name (you might infer this from the product URL or ASIN if not directly provided, or simply use the ASIN).
#     - Key metrics like estimated revenue, number of orders (if available), product price, and star rating.
# 3. Note any common characteristics among these top products (e.g., price range, popular features mentioned if discernible, saturation level).
# 4. Refer to the 'Category Department Market Size' information to provide context on the overall market share or revenue contribution of this category within the department.
# 5. Conclude with a brief summary of the Amazon landscape for this specific category/department.
# 6. Structure your response clearly using markdown formatting (e.g., use '**Subheadings**' for different parts like 'Top Products', 'Market Context', 'Overall Summary', and use '* Bullet points' for lists). DO NOT use '###' markdown headings.
# 7. Keep the tone objective and data-driven. Focus ONLY on the provided data.""",
#     INDICATOR_WEB_SUMMARY: """You are a helpful AI assistant answering a question based on the provided web search context (synthesized answer or top results). Directly answer the original user query: '{user_query}'. Synthesize the information from the provided 'External Web Context'. Cite sources implicitly if appropriate (e.g., "According to recent web information...", "Search results suggest..."). Keep the answer concise and focused on the user's query. Use standard markdown formatting.""",
#     # --- End New Amazon Radar Prompt ---
#     INDICATOR_COMPARISON: """You are a TrendForecast.io Comparative Analyst... [TODO: Define prompt]""",
#     INDICATOR_RECOMMENDATION: """You are a TrendForecast.io Recommendation Engine... [TODO: Define prompt]""",
#     INDICATOR_QA_WEB: """You are a helpful AI assistant... (Keep existing)""",
#     INDICATOR_QA_INTERNAL: """You are a helpful AI assistant... (Keep existing)""",
#     INDICATOR_QA_COMBINED: """You are a helpful AI assistant... (Keep existing)""",
#     INDICATOR_UNKNOWN: """You are a helpful AI assistant... (Keep existing)""",
#     INDICATOR_ERROR: "Error processing request.",
# }
# # --- *** MODIFICATION END *** ---
#
#
# # --- *** MODIFICATION START: Update get_task_details *** ---
# def get_task_details(primary_task: str | None) -> tuple[str, str]:
#     indicator = INDICATOR_UNKNOWN
#     if primary_task == "get_trend": indicator = INDICATOR_TREND_DETAIL
#     elif primary_task == "get_forecast": indicator = INDICATOR_FORECAST
#     elif primary_task == "summarize_mega_trends": indicator = INDICATOR_MEGA_TREND
#     elif primary_task == "summarize_category": indicator = INDICATOR_CATEGORY_OVERVIEW
#     elif primary_task == "compare_items": indicator = INDICATOR_COMPARISON
#     elif primary_task == "get_recommendation": indicator = INDICATOR_RECOMMENDATION
#     elif primary_task == "qa_web_only": indicator = INDICATOR_QA_WEB
#     elif primary_task == "qa_internal_only": indicator = INDICATOR_QA_INTERNAL
#     elif primary_task == "qa_combined": indicator = INDICATOR_QA_COMBINED
#     elif primary_task == "analyze_brand_deep_dive": indicator = INDICATOR_BRAND_ANALYSIS
#     # --- Add mapping for Amazon Radar task ---
#     elif primary_task == "summarize_amazon_radar": # Match task name from Interpreter
#         indicator = INDICATOR_AMAZON_RADAR
#     elif primary_task == "summarize_web_trends": indicator = INDICATOR_WEB_RESEARCH
#
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
#     external_data = external_data or {} # Ensure external_data is a dict
#     interpretation = internal_data.get("interpretation") if isinstance(internal_data.get("interpretation"), dict) else {}
#     original_context = interpretation.get("original_context") if isinstance(interpretation.get("original_context"), dict) else {}
#     query_subjects = interpretation.get("query_subjects") if isinstance(interpretation.get("query_subjects"), dict) else {}
#     specific_known = query_subjects.get("specific_known", []) if isinstance(query_subjects.get("specific_known"), list) else []
#     primary_task = interpretation.get("primary_task")
#     target_brand = query_subjects.get("target_brand")
#
#     # --- Basic Context (Always Add) ---
#     prompt_parts.append("CONTEXT:")
#     prompt_parts.append(f"- User Query: {original_context.get('query', 'N/A')}")
#     # Adjust context display based on task
#     if primary_task == "analyze_brand_deep_dive" and target_brand:
#          prompt_parts.append(f"- Brand Focus: {target_brand}")
#          prompt_parts.append(f"- Country Context (if applicable for web search): {original_context.get('country', 'N/A')}")
#     elif primary_task == "summarize_amazon_radar":
#          prompt_parts.append(f"- Amazon Radar For: Category '{original_context.get('target_category', 'N/A')}', Department '{original_context.get('target_department', 'N/A')}'")
#          prompt_parts.append(f"- Country: {original_context.get('country', 'N/A')}")
#     else:
#         prompt_parts.append(f"- Category: {original_context.get('category', 'N/A')}") # Original category from input
#         prompt_parts.append(f"- Country: {original_context.get('country', 'N/A')}")
#         if specific_known and isinstance(specific_known[0], dict):
#             specific_item_name = specific_known[0].get("subject", "N/A")
#             prompt_parts.append(f"- Specific Focus Item: {specific_item_name}")
#
#     prompt_parts.append("\nAVAILABLE DATA:")
#     data_found = False
#
#     # --- Format Brand Performance Data ---
#     if primary_task == "analyze_brand_deep_dive":
#         brand_perf_data = internal_data.get("brand_performance_data")
#         if isinstance(brand_perf_data, list) and brand_perf_data:
#             data_found = True; prompt_parts.append(f"\nInternal Brand Performance Data ({target_brand or 'Requested Brand'}):")
#             for country_data in brand_perf_data[:10]: # Limit to top 10 countries for prompt
#                 if isinstance(country_data, dict):
#                     country = country_data.get('country', 'N/A')
#                     visits = country_data.get('estimated_monthly_visits', 'N/A')
#                     growth = country_data.get('estimated_growth_percentage', 'N/A')
#                     growth_str = f"{growth:.1f}%" if isinstance(growth, (int, float)) else ("N/A" if growth is None else str(growth))
#                     visits_str = f"{visits:,}" if isinstance(visits, (int, float)) else "N/A"
#                     prompt_parts.append(f"- {country}: Est. Visits={visits_str}, Est. Growth={growth_str}")
#         else:
#             logger.debug("brand_performance_data missing or invalid for brand analysis task.")
#             prompt_parts.append("\nInternal Brand Performance Data: - Data not available.")
#
#     # --- Format Amazon Radar Data ---
#     elif primary_task == "summarize_amazon_radar":
#         amazon_data = internal_data.get("amazon_radar_data")
#         if isinstance(amazon_data, dict):
#             products = amazon_data.get("country_department_category", [])
#             market_info = amazon_data.get("category_dep_market_size")
#
#             if isinstance(products, list) and products:
#                 data_found = True
#                 prompt_parts.append("\nTop Amazon Products (Max 10 Displayed):")
#                 for i, p_item in enumerate(products[:10]): # Pass max 10 products to LLM
#                     if isinstance(p_item, dict):
#                         name = p_item.get("asin", "N/A") # Use ASIN if name not directly available
#                         price = p_item.get("product_price", "N/A")
#                         currency = p_item.get("currency", "")
#                         rating = p_item.get("product_star_rating", "N/A")
#                         revenue = p_item.get("estimated_revenue", "N/A")
#                         orders = p_item.get("estimated_orders", "N/A")
#                         prompt_parts.append(
#                             f"- Product {i+1} (ASIN: {name}): Price={price} {currency}, Rating={rating}/5, Est. Revenue={revenue:,.0f}, Est. Orders={orders:,.0f}"
#                         )
#             else:
#                 logger.debug("Amazon product list missing or empty for radar task.")
#                 prompt_parts.append("\nTop Amazon Products: - Data not available.")
#
#             if isinstance(market_info, dict):
#                 data_found = True # Even if only market info is found
#                 prompt_parts.append("\nAmazon Category Department Market Size Context:")
#                 share = market_info.get("department_in_country_share", "N/A")
#                 prompt_parts.append(f"- Category's Share in Department: {share}% (approx.)")
#                 # Could add top few from pie_chart if concise
#             else:
#                 logger.debug("Amazon market info missing for radar task.")
#                 prompt_parts.append("\nAmazon Category Department Market Size Context: - Data not available.")
#         else:
#             logger.debug("amazon_radar_data missing or invalid for radar task.")
#             prompt_parts.append("\nAmazon Radar Data: - Data not available.")
#
#     # --- Format Other Internal Data (Conditionally, if not brand or amazon task) ---
#     if primary_task not in ["analyze_brand_deep_dive", "summarize_amazon_radar"]:
#         # Category Context
#         trends = internal_data.get("trends_data") # ... (keep existing logic) ...
#         if isinstance(trends, dict):
#             data_found = True; prompt_parts.append("\nInternal Category Context:")
#             cs = trends.get("category_summary"); # ...
#             if isinstance(cs, dict): prompt_parts.append(f"- Overall Category ({cs.get('category_name', 'N/A')}): Avg Vol={cs.get('average_volume', 'N/A')}, Growth={cs.get('growth_recent', 'N/A'):.1f}%")
#             style_details = trends.get("style_details", []); color_details = trends.get("color_details", [])
#             if isinstance(style_details, list):
#                 top_styles = sorted([s for s in style_details if isinstance(s,dict)], key=lambda x: x.get('average_volume', 0), reverse=True)[:3]
#                 if top_styles: prompt_parts.append(f"- Top Styles in Category: {', '.join([s.get('style_name', 'N/A') for s in top_styles])}")
#             if isinstance(color_details, list):
#                 top_colors = sorted([c for c in color_details if isinstance(c,dict)], key=lambda x: x.get('average_volume', 0), reverse=True)[:3]
#                 if top_colors: prompt_parts.append(f"- Top Colors in Category: {', '.join([c.get('color_name', 'N/A') for c in top_colors])}")
#         else: logger.debug("trends_data missing or not a dict.")
#
#         # Item Details
#         if primary_task not in ["summarize_mega_trends", "summarize_category"]: # ... (keep existing logic) ...
#             details = internal_data.get("chart_details_data")
#             if isinstance(details, dict):
#                  data_found = True; prompt_parts.append(f"\nInternal Specific Item Details ({details.get('category_subject', 'N/A')}):")
#                  prompt_parts.append(f"- Avg Vol={details.get('average_volume', 'N/A')}, Growth={details.get('growth_recent', 'N/A'):.1f}%")
#                  if primary_task == "get_forecast":
#                      prompt_parts.append(f"- Data Point f2 (2m Growth %): {details.get('f2', 'N/A')}"); prompt_parts.append(f"- Data Point f3 (3m Growth %): {details.get('f3', 'N/A')}"); prompt_parts.append(f"- Data Point f6 (6m Growth %): {details.get('f6', 'N/A')}")
#                      prompt_parts.append(f"- Data Point avg2 (2m Avg Volume): {details.get('avg2', 'N/A')}"); prompt_parts.append(f"- Data Point avg3 (3m Avg Volume): {details.get('avg3', 'N/A')}"); prompt_parts.append(f"- Data Point avg6 (6m Avg Volume): {details.get('avg6', 'N/A')}")
#             else: logger.debug("chart_details_data missing or not a dict.")
#
#         # Mega Trends
#         if primary_task == "summarize_mega_trends": # ... (keep existing logic) ...
#             mega = internal_data.get("mega_trends_data")
#             if isinstance(mega, list) and mega:
#                 data_found = True; prompt_parts.append("\nInternal Mega Trends (Top Queries):")
#                 top_mega = sorted([m for m in mega if isinstance(m,dict)], key=lambda x: x.get('growth_recent', 0), reverse=True)[:5]
#                 if top_mega:
#                     for m in top_mega: prompt_parts.append(f"- Query: '{m.get('query_name', 'N/A')}', Related Category: {m.get('category_name', 'N/A')}, Growth: {m.get('growth_recent', 'N/A'):.1f}%")
#                 else: prompt_parts.append("- No specific mega trend query data found.")
#             else: logger.debug("mega_trends_data is missing or invalid for mega_trends task."); prompt_parts.append("\nInternal Mega Trends (Top Queries): - Data not available.")
#
#     # --- External Web Context (Always include if available for any task needing web search) ---
#     if "web_search" in interpretation.get("required_sources", []):
#         ext_answer = external_data.get("answer")
#         ext_results = external_data.get("results", [])
#         if not isinstance(ext_results, list): ext_results = []
#
#         if ext_answer or ext_results:
#              data_found = True
#              prompt_parts.append("\nExternal Web Context:")
#              if ext_answer: prompt_parts.append(f"- Synthesized Answer: {ext_answer}")
#              if ext_results:
#                   prompt_parts.append("- Top Results Snippets:")
#                   for i, res in enumerate(ext_results[:3]):
#                        if isinstance(res, dict):
#                           title = res.get('title', 'N/A'); content_snippet = res.get('content', '')[:150]
#                           prompt_parts.append(f"  - [{i+1}] {title}: {content_snippet}...")
#         else:
#              logger.debug("Web search requested, but no external web data (answer or results) available for prompt.")
#              prompt_parts.append("\nExternal Web Context: - No relevant web context found or provided.")
#
#     # --- Final check ---
#     if not data_found:
#         logger.warning("No significant internal or external data blocks were available to format for prompt.")
#         query = original_context.get('query', 'N/A')
#         if primary_task == "analyze_brand_deep_dive": brand = target_brand or 'the brand'; return f"No specific data or web context available to analyze for '{brand}'."
#         elif primary_task == "summarize_amazon_radar": cat = original_context.get('target_category','N/A'); dept = original_context.get('target_department','N/A'); return f"No Amazon product data available for category '{cat}' in department '{dept}'."
#         else: cat = original_context.get('category', 'N/A'); country = original_context.get('country', 'N/A'); return f"No specific data available for query '{query}' on category '{cat}' in {country}."
#
#     return "\n".join(prompt_parts)
# # --- *** MODIFICATION END *** ---
#
#
# # --- Parser Function (Keep flat subsection parser) ---
# def parse_markdown_flat_subsections(markdown_text: str) -> Dict[str, Any]:
#     # (Keep the parse_markdown_flat_subsections function as defined previously)
#     if not markdown_text or not isinstance(markdown_text, str): return {"subsections": []}
#     all_subsections: List[Dict] = []; current_subsection: Optional[Dict] = None; content_buffer: List[str] = []
#     lines = markdown_text.strip().split('\n')
#     def finalize_buffer():
#         nonlocal content_buffer, current_subsection
#         if not content_buffer or current_subsection is None: content_buffer = []; return
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
#                 finalize_buffer(); subtitle = subheading_match.group(1).strip()
#                 current_subsection = {"subheading": subtitle, "content": None, "points": []}
#                 all_subsections.append(current_subsection); continue
#             bullet_match = re.match(r'^\*\s+(.*)', line)
#             if bullet_match and current_subsection:
#                 finalize_buffer(); point_text = bullet_match.group(1).strip(); point_object = {"text": point_text}
#                 if "points" not in current_subsection: current_subsection["points"] = []
#                 current_subsection["points"].append(point_object); continue
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
# # --- *** MODIFICATION START: Update build_final_payload_for_bubble *** ---
# def build_final_payload_for_bubble(
#     ai_summary_text: str,
#     ai_summary_structured: Dict[str, Any],
#     internal_data: Dict,
#     external_data: Dict,
#     task_indicator: str,
#     final_status: str,
#     error_message: Optional[str]
#     ) -> Dict:
#     logger.debug("Building final payload object for Bubble...")
#     payload = {
#         "ai_summary_structured": ai_summary_structured,
#         "result_type_indicator": task_indicator if final_status != INDICATOR_ERROR else INDICATOR_ERROR,
#         "status": final_status,
#         "error_message": error_message,
#         "category_trend": [], "top_styles": [], "top_colors": [], "item_trend": [],
#         "item_metrics": [], "mega_trends_top": [], "web_links": [], "web_answer": None,
#         "brand_performance_summary": [], # Initialize new key for brand data
#         "amazon_radar_products": []     # Initialize new key for Amazon data
#     }
#     # Ensure internal_data and external_data are dicts
#     internal_data = internal_data or {}
#     external_data = external_data or {}
#
#     # Populate standard data lists
#     trends_data = internal_data.get("trends_data") if isinstance(internal_data.get("trends_data"), dict) else None
#     if trends_data: # ... (keep existing population logic) ...
#         cs = trends_data.get("category_summary");
#         if isinstance(cs, dict):
#              chart = cs.get("chart_data")
#              if isinstance(chart, list) and chart: payload["category_trend"] = chart
#         all_styles = trends_data.get("style_details", [])
#         if isinstance(all_styles, list):
#              valid_styles = [s for s in all_styles if isinstance(s,dict)]
#              if valid_styles: payload["top_styles"] = [{"name": s.get("style_name"), "growth": s.get("growth_recent"), "volume": s.get("average_volume")} for s in sorted(valid_styles, key=lambda x: x.get('average_volume', 0), reverse=True)[:5]]
#         all_colors = trends_data.get("color_details", [])
#         if isinstance(all_colors, list):
#             valid_colors = [c for c in all_colors if isinstance(c,dict)]
#             if valid_colors: payload["top_colors"] = [{"name": c.get("color_name"), "growth": c.get("growth_recent"), "volume": c.get("average_volume")} for c in sorted(valid_colors, key=lambda x: x.get('average_volume', 0), reverse=True)[:5]]
#
#     chart_details_data = internal_data.get("chart_details_data") if isinstance(internal_data.get("chart_details_data"), dict) else None
#     if chart_details_data: # ... (keep existing population logic) ...
#          item_metrics_obj = {"name": chart_details_data.get("category_subject"), "growth": chart_details_data.get("growth_recent"), "volume": chart_details_data.get("average_volume"), "forecast_growth": None, "forecast_volume": None }
#          if task_indicator == INDICATOR_FORECAST:
#              item_metrics_obj["forecast_growth"] = {"f2": chart_details_data.get("f2"), "f3": chart_details_data.get("f3"), "f6": chart_details_data.get("f6")}
#              item_metrics_obj["forecast_volume"] = {"avg2": chart_details_data.get("avg2"), "avg3": chart_details_data.get("avg3"), "avg6": chart_details_data.get("avg6")}
#          payload["item_metrics"] = [item_metrics_obj]
#          item_chart = chart_details_data.get("chart_data")
#          if isinstance(item_chart, list) and item_chart: payload["item_trend"] = item_chart
#
#     mega_trends_data = internal_data.get("mega_trends_data") if isinstance(internal_data.get("mega_trends_data"), list) else None
#     if mega_trends_data: # ... (keep existing population logic) ...
#         valid_mega = [m for m in mega_trends_data if isinstance(m,dict)]
#         if valid_mega: payload["mega_trends_top"] = [{"name": m.get("query_name"), "growth": m.get("growth_recent"), "volume": m.get("average_volume"), "category": m.get("category_name")} for m in sorted(valid_mega, key=lambda x: x.get('growth_recent', 0), reverse=True)[:10]]
#
#     # Populate brand performance data
#     brand_performance_data = internal_data.get("brand_performance_data")
#     if brand_performance_data and isinstance(brand_performance_data, list):
#          payload["brand_performance_summary"] = brand_performance_data
#          logger.debug(f"Populated brand_performance_summary list with {len(brand_performance_data)} items.")
#
#     # Populate Amazon Radar data
#     amazon_radar_data_from_internal = internal_data.get("amazon_radar_data")
#     if task_indicator == INDICATOR_AMAZON_RADAR and isinstance(amazon_radar_data_from_internal, dict):
#         products_list = amazon_radar_data_from_internal.get("country_department_category", [])
#         if isinstance(products_list, list):
#             # Select key fields and limit to max 10 products for Bubble
#             cleaned_products = []
#             for p_item in products_list[:10]:
#                 if isinstance(p_item, dict):
#                     cleaned_products.append({
#                         "asin": p_item.get("asin"),
#                         "product_url": p_item.get("product_url"),
#                         "product_photo": p_item.get("product_photo"),
#                         "product_price": p_item.get("product_price"),
#                         "currency": p_item.get("currency"),
#                         "estimated_revenue": p_item.get("estimated_revenue"),
#                         "estimated_orders": p_item.get("estimated_orders"),
#                         "number_of_reviews": p_item.get("number_of_reviews"),
#                         "product_star_rating": p_item.get("product_star_rating"),
#                         "saturation": p_item.get("saturation")
#                         # Add inferred name if needed:
#                         # "inferred_name": p_item.get("asin") # Or derive from URL
#                     })
#             payload["amazon_radar_products"] = cleaned_products
#             logger.debug(f"Populated amazon_radar_products list with {len(cleaned_products)} items.")
#         # Optionally, add market size info to payload if needed by UI
#         # market_info = amazon_radar_data_from_internal.get("category_dep_market_size")
#         # if isinstance(market_info, dict): payload["amazon_market_context"] = market_info
#
#     # Populate web links and answer
#     external_results = external_data.get("results", []) if isinstance(external_data.get("results", []), list) else []
#     external_answer = external_data.get("answer") if isinstance(external_data.get("answer"), str) else None
#     if external_results:
#          payload["web_links"] = [{"title": r.get("title"), "url": r.get("url")} for r in external_results if isinstance(r, dict) and r.get("title") and r.get("url")][:5]
#     if external_answer: payload["web_answer"] = external_answer
#
#     logger.info(f"Built final bubble payload object with keys: {list(payload.keys())}")
#     return payload
# # --- *** MODIFICATION END *** ---
#
#
# # --- Main Lambda Handler ---
# def lambda_handler(event, context):
#     logger.info(f"Received combined event: {json.dumps(event)}")
#
#     # Pre-checks (ensure fallback structures match)
#     if not GEMINI_SDK_AVAILABLE:
#         error_payload = build_final_payload_for_bubble("Error: LLM SDK unavailable.", {"subsections": []}, {}, {}, INDICATOR_ERROR, INDICATOR_ERROR, "LLM SDK unavailable.")
#         return {"statusCode": 500, "body": json.dumps(error_payload)}
#     if BOTO3_CLIENT_ERROR:
#         error_payload = build_final_payload_for_bubble(f"Error: {BOTO3_CLIENT_ERROR}", {"subsections": []}, {}, {}, INDICATOR_ERROR, INDICATOR_ERROR, BOTO3_CLIENT_ERROR)
#         return {"statusCode": 500, "body": json.dumps(error_payload)}
#
#     # Data extraction
#     internal_data = event.get("internal_data", {})
#     external_data = event.get("external_data", {})
#     interpretation = internal_data.get("interpretation", {}) if isinstance(internal_data, dict) else {}
#     original_context = interpretation.get("original_context", {}) if isinstance(interpretation, dict) else {}
#     primary_task = interpretation.get("primary_task")
#     user_query = original_context.get("query", "the user query")
#     query_subjects = interpretation.get("query_subjects", {}) if isinstance(interpretation.get("query_subjects"), dict) else {}
#     target_brand = query_subjects.get("target_brand") # For brand tasks
#     # For Amazon Radar, specific target category/department from original_context
#     amazon_target_category = original_context.get("target_category", "N/A")
#     amazon_target_department = original_context.get("target_department", "N/A")
#
#
#     upstream_errors = [] # (Keep upstream error collection logic)
#     if isinstance(internal_data, dict) and internal_data.get("errors"): upstream_errors.extend(internal_data["errors"])
#     if isinstance(internal_data, dict) and internal_data.get("errorType"): upstream_errors.append({"source": "FetchInternalDataRouter", "error": internal_data.get("errorType"), "details": internal_data.get("cause", internal_data.get("errorMessage"))})
#     if isinstance(external_data, dict) and external_data.get("error"): upstream_errors.append({"source": "FetchExternalContext", "error": external_data["error"]})
#     if isinstance(external_data, dict) and external_data.get("errorType"): upstream_errors.append({"source": "FetchExternalContext", "error": external_data.get("errorType"), "details": external_data.get("cause", external_data.get("errorMessage"))})
#
#
#     result_type_indicator, prompt_template = get_task_details(primary_task)
#     logger.info(f"Using result indicator: {result_type_indicator}")
#
#     specific_item_name = "N/A"
#     if isinstance(interpretation.get("query_subjects"), dict):
#          specific_known = query_subjects.get("specific_known", [])
#          if specific_known and isinstance(specific_known[0], dict): specific_item_name = specific_known[0].get("subject", "N/A")
#
#     formatted_data_context = ""
#     try:
#         formatted_data_context = format_data_for_prompt(internal_data, external_data) # Already handles task-specific formatting
#
#         # --- Prepare arguments for prompt_template.format() ---
#         prompt_format_args = {
#             "specific_item_name": specific_item_name,
#             "category_name": original_context.get('category', 'N/A'), # For general prompts
#             "country_name": original_context.get('country', 'N/A'),   # For general prompts
#             "user_query": user_query,
#             "brand_domain": target_brand or "N/A", # For brand analysis prompt
#             # --- Add args for Amazon Radar prompt ---
#             "target_category": amazon_target_category,
#             "target_department": amazon_target_department
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
#         error_payload = build_final_payload_for_bubble("Error: API key config.", {"subsections": []}, internal_data, external_data, INDICATOR_ERROR, INDICATOR_ERROR, "API key config error")
#         return {"statusCode": 500, "body": json.dumps(error_payload)}
#
#     ai_summary_text = "Error: AI synthesis failed."
#     llm_error = None
#     try:
#         if not formatted_data_context or formatted_data_context.startswith("No specific data available"):
#              logger.warning("Skipping LLM call as no significant data was formatted for prompt.")
#              ai_summary_text = PERSONA_PROMPTS[INDICATOR_UNKNOWN].format(user_query=user_query, category_name=original_context.get('category', 'N/A'), country_name=original_context.get('country', 'N/A'))
#              result_type_indicator = INDICATOR_UNKNOWN
#         else:
#              logger.info(f"Calling Synthesis LLM: {SYNTHESIS_LLM_MODEL}...")
#              genai.configure(api_key=google_api_key)
#              model = genai.GenerativeModel(SYNTHESIS_LLM_MODEL)
#              response = model.generate_content(synthesis_prompt)
#              logger.info("Synthesis LLM response received.")
#              ai_summary_text = response.text
#              logger.debug(f"Synthesis LLM Raw Response Text:\n{ai_summary_text}")
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
#         final_status = "success"; user_error_message = None
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
#             ai_summary_text=ai_summary_text, ai_summary_structured=ai_summary_structured,
#             internal_data=internal_data, external_data=external_data,
#             task_indicator=result_type_indicator, final_status=final_status, error_message=user_error_message
#         )
#         logger.info(f"Final payload status: {final_status}")
#     except Exception as e:
#          logger.error(f"Error during summary parsing or final payload building: {e}", exc_info=True)
#          final_bubble_payload = {
#              "ai_summary_structured": { "subsections": [{"subheading":"Error", "content":f"An critical error occurred preparing the final response: {e}", "points":[]}] },
#              "result_type_indicator": INDICATOR_ERROR, "status": INDICATOR_ERROR, "error_message": f"Payload construction/parsing error: {str(e)}",
#              "category_trend": [], "top_styles": [], "top_colors": [], "item_trend": [], "item_metrics": [], "mega_trends_top": [], "web_links": [], "web_answer": None,
#              "brand_performance_summary": [], "amazon_radar_products": []
#          }
#
#     return {"statusCode": 200, "body": json.dumps(final_bubble_payload)}





# import json
# import logging
# import os
# import re
# # --- MODIFICATION: Removed markdown parser import ---
# # from typing import Dict, Optional, List, Any # Keep typing
# from typing import Dict, Optional, List, Any, Tuple # Added Tuple
# from decimal import Decimal # Needed for replace_decimals
#
# import boto3
# from botocore.exceptions import ClientError
#
# try:
#     import google.generativeai as genai
#     # --- MODIFICATION: Need specific types for generation config ---
#     import google.generativeai.types as genai_types
#     GEMINI_SDK_AVAILABLE = True
# except ImportError:
#     genai = None
#     genai_types = None
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
# INDICATOR_BRAND_ANALYSIS = "BRAND_ANALYSIS"
# INDICATOR_AMAZON_RADAR = "AMAZON_RADAR"
# INDICATOR_WEB_SUMMARY = "WEB_SUMMARY"
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
#     # (Keep existing implementation)
#     is_local = os.environ.get("IS_LOCAL", "false").lower() == "true"
#     if is_local:
#         direct_key = os.environ.get(key_name)
#         if direct_key: logger.info(f"Using direct env var '{key_name}' (local mode)"); return direct_key
#         else: logger.warning(f"Direct env var '{key_name}' not found. Trying Secrets Manager...")
#     global API_KEY_CACHE; cache_key = f"{secret_name}:{key_name}"
#     if cache_key in API_KEY_CACHE: logger.debug(f"Using cached secret key: {cache_key}"); return API_KEY_CACHE[cache_key]
#     if BOTO3_CLIENT_ERROR: logger.error(f"Boto3 client error: {BOTO3_CLIENT_ERROR}"); return None
#     if not secrets_manager: logger.error("Secrets Manager client not initialized."); return None
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
#         if not isinstance(secret_dict, dict): logger.error("Parsed secret not dict."); return None
#         key_value = secret_dict.get(key_name)
#         if not key_value or not isinstance(key_value, str):
#             logger.error(f"Key '{key_name}' not found/not string in '{secret_name}'."); API_KEY_CACHE[cache_key] = None; return None
#         API_KEY_CACHE[cache_key] = key_value; logger.info(f"Key '{key_name}' successfully retrieved and cached."); return key_value
#     except ClientError as e:
#         error_code = e.response.get("Error", {}).get("Code"); logger.error(f"AWS ClientError for '{secret_name}': {error_code}"); API_KEY_CACHE[cache_key] = None; return None
#     except Exception as e:
#         logger.exception(f"Unexpected error retrieving secret '{secret_name}'."); API_KEY_CACHE[cache_key] = None; return None
#
# # --- *** MODIFICATION START: Define Default Schema *** ---
# # Define the standard structure we want the LLM to return
# DEFAULT_AI_SUMMARY_STRUCTURE = {
#     "overall_summary": "",
#     "sections": [
#         {"id": "category_context", "heading": "Current Category Context", "content": "", "points": []},
#         {"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "", "points": []},
#         {"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "", "points": []},
#         {"id": "mega_trends", "heading": "Key Mega Trends", "content": "", "points": []},
#         {"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "", "points": []},
#         {"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "", "points": []},
#         {"id": "web_context", "heading": "Web Context & News", "content": "", "points": []},
#         {"id": "recommendations", "heading": "Recommendations", "content": "", "points": []},
#         {"id": "comparison", "heading": "Comparison Points", "content": "", "points": []}
#         # Add more sections here (e.g., tiktok_radar) as features are added
#     ]
# }
#
# def get_default_summary_structure() -> Dict:
#     """Returns a deep copy of the default summary structure."""
#     # Use deep copy to avoid modifying the original template
#     return json.loads(json.dumps(DEFAULT_AI_SUMMARY_STRUCTURE))
#
# def validate_structured_summary(summary_obj: Any) -> bool:
#     """Performs basic validation on the structure received from the LLM."""
#     if not isinstance(summary_obj, dict): return False
#     if "overall_summary" not in summary_obj or not isinstance(summary_obj["overall_summary"], str): return False
#     if "sections" not in summary_obj or not isinstance(summary_obj["sections"], list): return False
#     for section in summary_obj["sections"]:
#         if not isinstance(section, dict): return False
#         if not all(key in section for key in ["id", "heading", "content", "points"]): return False
#         if not isinstance(section.get("points"), list): return False
#         for point in section["points"]:
#             if not isinstance(point, dict) or "text" not in point: return False
#     return True
#
# # --- *** MODIFICATION END *** ---
#
# # --- *** MODIFICATION START: Update INDICATOR_FORECAST Prompt for JSON Output *** ---
# # --- *** MODIFICATION START: Update ALL Prompts for JSON Output *** ---
# PERSONA_PROMPTS = {
#     # --- Trend Detail ---
#     INDICATOR_TREND_DETAIL: """You are a TrendForecast.io Expert Analyst providing insights on {specific_item_name} in {country_name}.
# Analyze the provided internal data (category summary, style/color details, specific item details) and any relevant 'External Web Context'.
#
# Instructions:
# 1. Generate ONLY a valid JSON object adhering EXACTLY to the schema provided below.
# 2. Populate the `overall_summary` field with a concise narrative summarizing the key trend findings for {specific_item_name}.
# 3. Populate the `item_trend_detail` section:
#     - Set the `content` field to describe the current trend status (e.g., growing, declining, stable) based on recent historical performance (avg volume, growth) and potentially supporting points from web context or category context. Mention the specific avg volume and growth % from the data.
#     - Use the `points` list for specific supporting details or drivers if applicable (format as {{"text": "..."}}).
# 4. If relevant 'Internal Category Context' data is provided, summarize how the specific item's trend relates to the broader category in the `category_context` section's `content` field.
# 5. If 'External Web Context' is available and relevant, summarize key insights/news influencing the item's trend in the `web_context` section's `content` or `points`.
# 6. For all other sections in the schema (e.g., `forecast_outlook`, `mega_trends`, etc.), provide empty values: `content: ""` and `points: []`.
# 7. DO NOT include markdown formatting (like **, *) within the JSON string values unless part of the actual data.
#
# JSON Schema to follow:
# ```json
# {{
#   "overall_summary": "string",
#   "sections": [
#     {{"id": "category_context", "heading": "Current Category Context", "content": "string", "points": [{{"text": "string"}}, ...]}},
#     {{"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "string", "points": [{{"text": "string"}}, ...]}},
#     {{"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "", "points": []}},
#     {{"id": "mega_trends", "heading": "Key Mega Trends", "content": "", "points": []}},
#     {{"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "", "points": []}},
#     {{"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "", "points": []}},
#     {{"id": "web_context", "heading": "Web Context & News", "content": "string", "points": [{{"text": "string"}}, ...]}},
#     {{"id": "recommendations", "heading": "Recommendations", "content": "", "points": []}},
#     {{"id": "comparison", "heading": "Comparison Points", "content": "", "points": []}}
#   ]
# }}
# ```""",
#
#     # --- Mega Trend ---
#     INDICATOR_MEGA_TREND: """You are a TrendForecast.io Senior Strategist providing a synthesized analysis of the '{category_name}' category in '{country_name}'.
# Analyze the provided data sources: Internal Mega Trends (list of top trending search queries/topics), Internal Category Context (overall category performance, top styles/colors), and External Web Context (synthesized answer or top results).
#
# Instructions:
# 1. Generate ONLY a valid JSON object adhering EXACTLY to the schema provided below.
# 2. Populate the `overall_summary` field with a concise narrative synthesizing the key findings about mega trends and their relation to the category context and web insights.
# 3. Populate the `mega_trends` section:
#     - Set the `content` field to describe the main themes emerging from the top queries.
#     - Use the `points` list to list the top 3-5 identified mega trends (format as {{"text": "Trend Name: Supporting query example"}}).
# 4. Populate the `category_context` section:
#     - Set the `content` field to discuss how the identified mega trends relate to or influence the observed category performance, top styles, and top colors. Note alignments or disconnects.
# 5. Populate the `web_context` section:
#     - Set the `content` or `points` field to summarize relevant confirmations or additional context from the web search data.
# 6. For all other sections (e.g., `item_trend_detail`, `forecast_outlook`, etc.), provide empty values: `content: ""` and `points: []`.
# 7. DO NOT include markdown formatting within the JSON string values.
#
# JSON Schema to follow:
# ```json
# {{
#   "overall_summary": "string",
#   "sections": [
#     {{"id": "category_context", "heading": "Current Category Context", "content": "string", "points": [{{"text": "string"}}, ...]}},
#     {{"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "", "points": []}},
#     {{"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "", "points": []}},
#     {{"id": "mega_trends", "heading": "Key Mega Trends", "content": "string", "points": [{{"text": "string"}}, ...]}},
#     {{"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "", "points": []}},
#     {{"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "", "points": []}},
#     {{"id": "web_context", "heading": "Web Context & News", "content": "string", "points": [{{"text": "string"}}, ...]}},
#     {{"id": "recommendations", "heading": "Recommendations", "content": "", "points": []}},
#     {{"id": "comparison", "heading": "Comparison Points", "content": "", "points": []}}
#   ]
# }}
# ```""",
#
#     # --- Category Overview ---
#     INDICATOR_CATEGORY_OVERVIEW: """You are a TrendForecast.io Market Analyst providing a high-level overview for the '{category_name}' category in '{country_name}'.
# Analyze the provided category summary data, top style/color details, and web context.
#
# Instructions:
# 1. Generate ONLY a valid JSON object adhering EXACTLY to the schema provided below.
# 2. Populate the `overall_summary` field with a concise narrative summarizing the category's current state.
# 3. Populate the `category_context` section:
#     - Set the `content` field to describe the overall performance (growth, volume).
#     - Use the `points` list to highlight the top 2-3 performing styles and top 2-3 performing colors based on the data (format as {{"text": "Top Style: [Name] (Growth: X%, Vol: Y)"}} or {{"text": "Top Color: [Name] (Growth: X%, Vol: Y)"}}).
# 4. If 'External Web Context' is available, summarize relevant insights in the `web_context` section's `content` or `points`.
# 5. For all other sections (e.g., `item_trend_detail`, `forecast_outlook`, etc.), provide empty values: `content: ""` and `points: []`.
# 6. DO NOT include markdown formatting within the JSON string values.
#
# JSON Schema to follow:
# ```json
# {{
#   "overall_summary": "string",
#   "sections": [
#     {{"id": "category_context", "heading": "Current Category Context", "content": "string", "points": [{{"text": "string"}}, ...]}},
#     {{"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "", "points": []}},
#     {{"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "", "points": []}},
#     {{"id": "mega_trends", "heading": "Key Mega Trends", "content": "", "points": []}},
#     {{"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "", "points": []}},
#     {{"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "", "points": []}},
#     {{"id": "web_context", "heading": "Web Context & News", "content": "string", "points": [{{"text": "string"}}, ...]}},
#     {{"id": "recommendations", "heading": "Recommendations", "content": "", "points": []}},
#     {{"id": "comparison", "heading": "Comparison Points", "content": "", "points": []}}
#   ]
# }}
# ```""",
#
#     # --- Forecast --- (Already Updated in previous step)
#     INDICATOR_FORECAST: """You are a TrendForecast.io Forecast Specialist providing predictions for {specific_item_name} in {country_name}.
# Analyze the provided specific item details (avg volume, growth, forecast data points f2, f3, f6, avg2, avg3, avg6) and any relevant 'External Web Context'.
#
# Instructions:
# 1. Generate ONLY a valid JSON object adhering EXACTLY to the schema provided below.
# 2. Populate the `overall_summary` field with a concise narrative summarizing the forecast outlook.
# 3. Populate the `forecast_outlook` section:
#     - Set the `content` field to explain the growth trajectory based on the forecast numbers.
#     - Populate the `points` list with individual forecast metrics. Each point MUST be an object `{{"text": "..."}}`. Use the EXACT phrasing: "forecasted growth in search demand is X% for Y months" or "forecasted average search demand is Z for Y months". Include all 6 data points (f2, f3, f6, avg2, avg3, avg6).
# 4. Populate the `item_trend_detail` section's `content` field with a brief mention of the item's recent historical performance (avg vol, growth) from the provided data.
# 5. If 'External Web Context' is available and relevant, summarize key points influencing the forecast in the `web_context` section's `content` or `points`.
# 6. For all other sections in the schema (e.g., `category_context`, `mega_trends`, etc.), provide empty values: `content: ""` and `points: []`.
# 7. DO NOT include markdown formatting within the JSON string values unless part of the actual data.
#
# JSON Schema to follow:
# ```json
# {{
#   "overall_summary": "string",
#   "sections": [
#     {{"id": "category_context", "heading": "Current Category Context", "content": "", "points": []}},
#     {{"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "string", "points": []}},
#     {{"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "string", "points": [{{"text": "string"}}, ...]}},
#     {{"id": "mega_trends", "heading": "Key Mega Trends", "content": "", "points": []}},
#     {{"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "", "points": []}},
#     {{"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "", "points": []}},
#     {{"id": "web_context", "heading": "Web Context & News", "content": "string", "points": [{{"text": "string"}}, ...]}},
#     {{"id": "recommendations", "heading": "Recommendations", "content": "", "points": []}},
#     {{"id": "comparison", "heading": "Comparison Points", "content": "", "points": []}}
#   ]
# }}
# ```""",
#
#     # --- Brand Analysis ---
#     INDICATOR_BRAND_ANALYSIS: """You are a TrendForecast.io Brand Analyst for brand '{brand_domain}'.
# Analyze the provided 'Internal Brand Performance Data' (estimated visits/growth per country) and any relevant 'External Web Context' (summarized answer or top results).
#
# Instructions:
# 1. Generate ONLY a valid JSON object adhering EXACTLY to the schema provided below.
# 2. Populate the `overall_summary` field with a concise narrative summarizing the brand's key performance findings and web context insights.
# 3. Populate the `brand_performance` section:
#     - Set the `content` field to discuss overall performance trends across countries.
#     - Use the `points` list to highlight specific country performance (e.g., {{"text": "USA: High volume (X visits), Strong growth (Y%)"}}). Include highlights for 2-3 key countries.
# 4. Populate the `web_context` section:
#     - Set the `content` or `points` to summarize key findings from the web search (news, competitor mentions, sentiment).
# 5. For all other sections (e.g., `category_context`, `forecast_outlook`, etc.), provide empty values: `content: ""` and `points: []`.
# 6. DO NOT include markdown formatting within the JSON string values.
#
# JSON Schema to follow:
# ```json
# {{
#   "overall_summary": "string",
#   "sections": [
#     {{"id": "category_context", "heading": "Current Category Context", "content": "", "points": []}},
#     {{"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "", "points": []}},
#     {{"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "", "points": []}},
#     {{"id": "mega_trends", "heading": "Key Mega Trends", "content": "", "points": []}},
#     {{"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "string", "points": [{{"text": "string"}}, ...]}},
#     {{"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "", "points": []}},
#     {{"id": "web_context", "heading": "Web Context & News", "content": "string", "points": [{{"text": "string"}}, ...]}},
#     {{"id": "recommendations", "heading": "Recommendations", "content": "", "points": []}},
#     {{"id": "comparison", "heading": "Comparison Points", "content": "", "points": []}}
#   ]
# }}
# ```""",
#
#     # --- Amazon Radar ---
#     INDICATOR_AMAZON_RADAR: """You are a TrendForecast.io Amazon Market Analyst for '{target_category}' ({target_department}) in {country_name}.
# Analyze the provided 'Top Amazon Products' list and 'Amazon Category Department Market Size Context'.
#
# Instructions:
# 1. Generate ONLY a valid JSON object adhering EXACTLY to the schema provided below.
# 2. Populate the `overall_summary` field with a concise narrative summarizing the key findings from the Amazon data.
# 3. Populate the `amazon_radar` section:
#     - Set the `content` field to discuss general observations (e.g., price ranges, saturation, common brands if apparent).
#     - Use the `points` list to highlight the top 3 products, mentioning key metrics like ASIN, Price, Rating, Est. Revenue (format as {{"text": "Top Product: ASIN XXX, Price $Y, Rating Z, Revenue ~$W"}}).
# 4. Populate the `category_context` section's `content` field *only* with the provided market share information (e.g., "This category represents approx. X% share of the department on Amazon.").
# 5. For all other sections (e.g., `item_trend_detail`, `forecast_outlook`, etc.), provide empty values: `content: ""` and `points: []`.
# 6. DO NOT include markdown formatting within the JSON string values.
#
# JSON Schema to follow:
# ```json
# {{
#   "overall_summary": "string",
#   "sections": [
#     {{"id": "category_context", "heading": "Current Category Context", "content": "string", "points": []}}, // For market share
#     {{"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "", "points": []}},
#     {{"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "", "points": []}},
#     {{"id": "mega_trends", "heading": "Key Mega Trends", "content": "", "points": []}},
#     {{"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "", "points": []}},
#     {{"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "string", "points": [{{"text": "string"}}, ...]}},
#     {{"id": "web_context", "heading": "Web Context & News", "content": "", "points": []}},
#     {{"id": "recommendations", "heading": "Recommendations", "content": "", "points": []}},
#     {{"id": "comparison", "heading": "Comparison Points", "content": "", "points": []}}
#   ]
# }}
# ```""",
#
#     # --- Web Summary (Consolidated from QA_Web) ---
#     INDICATOR_WEB_SUMMARY: """You are a helpful AI assistant answering the query '{user_query}' for {country_name}.
# Analyze ONLY the provided 'External Web Context' (Synthesized Answer or Top Results Snippets).
#
# Instructions:
# 1. Generate ONLY a valid JSON object adhering EXACTLY to the schema provided below.
# 2. Populate the `overall_summary` field with a concise answer to the user's query, synthesized *only* from the provided web context.
# 3. Populate the `web_context` section:
#     - Set the `content` field to elaborate on the answer or provide key supporting details from the web context.
#     - Use the `points` list to list key findings or source snippets (format as {{"text": "..."}}).
# 4. For all other sections (e.g., `category_context`, `item_trend_detail`, etc.), provide empty values: `content: ""` and `points: []`.
# 5. DO NOT include markdown formatting within the JSON string values.
#
# JSON Schema to follow:
# ```json
# {{
#   "overall_summary": "string",
#   "sections": [
#     {{"id": "category_context", "heading": "Current Category Context", "content": "", "points": []}},
#     {{"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "", "points": []}},
#     {{"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "", "points": []}},
#     {{"id": "mega_trends", "heading": "Key Mega Trends", "content": "", "points": []}},
#     {{"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "", "points": []}},
#     {{"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "", "points": []}},
#     {{"id": "web_context", "heading": "Web Context & News", "content": "string", "points": [{{"text": "string"}}, ...]}},
#     {{"id": "recommendations", "heading": "Recommendations", "content": "", "points": []}},
#     {{"id": "comparison", "heading": "Comparison Points", "content": "", "points": []}}
#   ]
# }}
# ```""",
#
#     # --- TODO: Update These Prompts for JSON Schema ---
#     INDICATOR_COMPARISON: """[TODO: Update to output JSON schema]""",
#     INDICATOR_RECOMMENDATION: """[TODO: Update to output JSON schema]""",
#     INDICATOR_QA_WEB: """[TODO: Remove or alias to WEB_SUMMARY]""", # Intentionally left, can be removed later
#     INDICATOR_QA_INTERNAL: """[TODO: Update to output JSON schema, decide which sections apply]""",
#     INDICATOR_QA_COMBINED: """[TODO: Update to output JSON schema, decide which sections apply]""",
#
#     # --- Fallback/Error ---
#     INDICATOR_UNKNOWN: """[This prompt text is less critical as the fallback structure is generated in code, but could be updated to output JSON if needed] You are a helpful AI assistant. The user's request ('{user_query}' for {category_name} in {country_name}) could not be fully processed due to missing or invalid internal/external data needed for a categorized response. Please state that you cannot provide a specific analysis without the necessary underlying data.""",
#     INDICATOR_ERROR: "Error processing request.", # This is never sent to LLM
# }
# # --- *** MODIFICATION END *** ---
# # --- *** MODIFICATION END *** ---
#
#
# # --- *** MODIFICATION START: Update get_task_details *** ---
# # No change needed here based on last update, already maps tasks
# def get_task_details(primary_task: str | None) -> Tuple[str, str]:
#     indicator = INDICATOR_UNKNOWN
#     if primary_task == "get_trend": indicator = INDICATOR_TREND_DETAIL
#     elif primary_task == "get_forecast": indicator = INDICATOR_FORECAST
#     elif primary_task == "summarize_mega_trends": indicator = INDICATOR_MEGA_TREND
#     elif primary_task == "summarize_category": indicator = INDICATOR_CATEGORY_OVERVIEW
#     elif primary_task == "compare_items": indicator = INDICATOR_COMPARISON
#     elif primary_task == "get_recommendation": indicator = INDICATOR_RECOMMENDATION
#     elif primary_task == "analyze_brand_deep_dive": indicator = INDICATOR_BRAND_ANALYSIS
#     elif primary_task == "summarize_amazon_radar": indicator = INDICATOR_AMAZON_RADAR
#     elif primary_task == "summarize_web_trends": indicator = INDICATOR_WEB_SUMMARY
#     elif primary_task == "unknown": indicator = INDICATOR_UNKNOWN
#     elif primary_task == "error": indicator = INDICATOR_ERROR
#     # Removed QA specific indicators - they should resolve to one of the above based on sources
#     prompt_template = PERSONA_PROMPTS.get(indicator, PERSONA_PROMPTS[INDICATOR_UNKNOWN])
#     logger.info(f"Mapped primary_task '{primary_task}' to indicator '{indicator}'.")
#     return indicator, prompt_template
# # --- *** MODIFICATION END *** ---
#
# # --- *** MODIFICATION START: Update format_data_for_prompt *** ---
# # Keep this function mostly as is, the LLM will use the data provided
# # The prompt now tells the LLM *how* to structure the output, not this function.
# def format_data_for_prompt(internal_data: Dict, external_data: Dict) -> str:
#     # (Keep existing implementation - it gathers the context string well)
#     prompt_parts = []
#     internal_data = internal_data or {}
#     external_data = external_data or {}
#     interpretation = internal_data.get("interpretation") if isinstance(internal_data.get("interpretation"), dict) else {}
#     original_context = interpretation.get("original_context") if isinstance(interpretation.get("original_context"), dict) else {}
#     query_subjects = interpretation.get("query_subjects") if isinstance(interpretation.get("query_subjects"), dict) else {}
#     specific_known = query_subjects.get("specific_known", []) if isinstance(query_subjects.get("specific_known"), list) else []
#     primary_task = interpretation.get("primary_task")
#     target_brand = query_subjects.get("target_brand")
#
#     prompt_parts.append("CONTEXT:")
#     prompt_parts.append(f"- User Query: {original_context.get('query', 'N/A')}")
#     if primary_task == "analyze_brand_deep_dive" and target_brand:
#          prompt_parts.append(f"- Brand Focus: {target_brand}"); prompt_parts.append(f"- Country Context: {original_context.get('country', 'N/A')}")
#     elif primary_task == "summarize_amazon_radar":
#          prompt_parts.append(f"- Amazon Radar For: Category '{original_context.get('target_category', 'N/A')}', Department '{original_context.get('target_department', 'N/A')}'"); prompt_parts.append(f"- Country: {original_context.get('country', 'N/A')}")
#     elif primary_task == "summarize_web_trends":
#          prompt_parts.append(f"- Topic: General fashion trends"); prompt_parts.append(f"- Country: {original_context.get('country', 'N/A')}")
#     else:
#         prompt_parts.append(f"- Category: {original_context.get('category', 'N/A')}"); prompt_parts.append(f"- Country: {original_context.get('country', 'N/A')}")
#         if specific_known and isinstance(specific_known[0], dict): specific_item_name = specific_known[0].get("subject", "N/A"); prompt_parts.append(f"- Specific Focus Item: {specific_item_name}")
#
#     prompt_parts.append("\nAVAILABLE DATA:")
#     data_found = False
#
#     brand_perf_data = internal_data.get("brand_performance_data")
#     if isinstance(brand_perf_data, list) and brand_perf_data:
#         data_found = True; prompt_parts.append(f"\nInternal Brand Performance Data ({target_brand or 'Requested Brand'}):")
#         for country_data in brand_perf_data[:10]:
#             if isinstance(country_data, dict):
#                 country = country_data.get('country', 'N/A'); visits = country_data.get('estimated_monthly_visits', 'N/A'); growth = country_data.get('estimated_growth_percentage', 'N/A')
#                 growth_str = f"{growth:.1f}%" if isinstance(growth, (int, float)) else ("N/A" if growth is None else str(growth)); visits_str = f"{visits:,}" if isinstance(visits, (int, float)) else "N/A"
#                 prompt_parts.append(f"- {country}: Est. Visits={visits_str}, Est. Growth={growth_str}")
#     elif primary_task == "analyze_brand_deep_dive": prompt_parts.append("\nInternal Brand Performance Data: - Data not available.")
#
#     amazon_data = internal_data.get("amazon_radar_data")
#     if isinstance(amazon_data, dict):
#         products = amazon_data.get("country_department_category", []); market_info = amazon_data.get("category_dep_market_size")
#         if isinstance(products, list) and products:
#             data_found = True; prompt_parts.append("\nTop Amazon Products (by Revenue, Max 10 Provided):")
#             for i, p_item in enumerate(products[:10]):
#                 if isinstance(p_item, dict):
#                     asin = p_item.get("asin", "N/A"); price = p_item.get("product_price", "N/A"); currency = p_item.get("currency", ""); rating = p_item.get("product_star_rating", "N/A"); revenue = p_item.get("estimated_revenue", "N/A"); orders = p_item.get("estimated_orders", "N/A"); reviews = p_item.get("number_of_reviews", "N/A")
#                     price_str = f"{price:,.2f}" if isinstance(price, (int, float)) else "N/A"; revenue_str = f"{revenue:,.0f}" if isinstance(revenue, (int, float)) else "N/A"; orders_str = f"{orders:,.0f}" if isinstance(orders, (int, float)) else "N/A"; reviews_str = f"{reviews:,.0f}" if isinstance(reviews, (int, float)) else "N/A"
#                     prompt_parts.append(f"- Item {i+1} (ASIN: {asin}): Price ~{price_str} {currency}, Rating={rating}/5, Est. Revenue={revenue_str}, Est. Orders={orders_str}, Reviews={reviews_str}")
#         elif primary_task == "summarize_amazon_radar": prompt_parts.append("\nTop Amazon Products: - Data not available.")
#         if isinstance(market_info, dict):
#             data_found = True; prompt_parts.append("\nAmazon Category Department Market Size Context:")
#             share = market_info.get("department_in_country_share", "N/A"); prompt_parts.append(f"- Category's Share in Department: {share}% (approx.)")
#         elif primary_task == "summarize_amazon_radar": prompt_parts.append("\nAmazon Category Department Market Size Context: - Data not available.")
#     elif primary_task == "summarize_amazon_radar": prompt_parts.append("\nAmazon Radar Data: - Data not available.")
#
#     trends = internal_data.get("trends_data")
#     if isinstance(trends, dict):
#         data_found = True; prompt_parts.append("\nInternal Category Context:")
#         cs = trends.get("category_summary");
#         if isinstance(cs, dict): prompt_parts.append(f"- Overall Category ({cs.get('category_name', 'N/A')}): Avg Vol={cs.get('average_volume', 'N/A')}, Growth={cs.get('growth_recent', 'N/A'):.1f}%")
#         style_details = trends.get("style_details", []); color_details = trends.get("color_details", [])
#         if isinstance(style_details, list): top_styles = sorted([s for s in style_details if isinstance(s,dict)], key=lambda x: x.get('average_volume', 0), reverse=True)[:3];
#         if top_styles: prompt_parts.append(f"- Top Styles in Category: {', '.join([s.get('style_name', 'N/A') for s in top_styles])}")
#         if isinstance(color_details, list): top_colors = sorted([c for c in color_details if isinstance(c,dict)], key=lambda x: x.get('average_volume', 0), reverse=True)[:3];
#         if top_colors: prompt_parts.append(f"- Top Colors in Category: {', '.join([c.get('color_name', 'N/A') for c in top_colors])}")
#     elif primary_task not in ["analyze_brand_deep_dive", "summarize_amazon_radar", "summarize_web_trends"]: logger.debug("trends_data is missing or not a dict.")
#
#     details = internal_data.get("chart_details_data")
#     if isinstance(details, dict):
#          data_found = True; prompt_parts.append(f"\nInternal Specific Item Details ({details.get('category_subject', 'N/A')}):")
#          prompt_parts.append(f"- Avg Vol={details.get('average_volume', 'N/A')}, Growth={details.get('growth_recent', 'N/A'):.1f}%")
#          if primary_task == "get_forecast":
#              prompt_parts.append(f"- Data Point f2 (2m Growth %): {details.get('f2', 'N/A')}"); prompt_parts.append(f"- Data Point f3 (3m Growth %): {details.get('f3', 'N/A')}"); prompt_parts.append(f"- Data Point f6 (6m Growth %): {details.get('f6', 'N/A')}")
#              prompt_parts.append(f"- Data Point avg2 (2m Avg Volume): {details.get('avg2', 'N/A')}"); prompt_parts.append(f"- Data Point avg3 (3m Avg Volume): {details.get('avg3', 'N/A')}"); prompt_parts.append(f"- Data Point avg6 (6m Avg Volume): {details.get('avg6', 'N/A')}")
#     elif primary_task == "get_forecast" or primary_task == "get_trend": logger.debug("chart_details_data is missing or not a dict.")
#
#     mega = internal_data.get("mega_trends_data")
#     if isinstance(mega, list) and mega:
#         data_found = True; prompt_parts.append("\nInternal Mega Trends (Top Queries):")
#         top_mega = sorted([m for m in mega if isinstance(m,dict)], key=lambda x: x.get('growth_recent', 0), reverse=True)[:5];
#         if top_mega:
#             for m in top_mega: prompt_parts.append(f"- Query: '{m.get('query_name', 'N/A')}', Related Category: {m.get('category_name', 'N/A')}, Growth: {m.get('growth_recent', 'N/A'):.1f}%")
#         else: prompt_parts.append("- No specific mega trend query data found.")
#     elif primary_task == "summarize_mega_trends": logger.debug("mega_trends_data is missing or invalid."); prompt_parts.append("\nInternal Mega Trends (Top Queries): - Data not available.")
#
#     if "web_search" in interpretation.get("required_sources", []):
#         ext_answer = external_data.get("answer"); ext_results = external_data.get("results", [])
#         if not isinstance(ext_results, list): ext_results = []
#         if ext_answer or ext_results:
#              data_found = True; prompt_parts.append("\nExternal Web Context:")
#              if ext_answer: prompt_parts.append(f"- Synthesized Answer: {ext_answer}")
#              if ext_results:
#                   prompt_parts.append("- Top Results Snippets:")
#                   for i, res in enumerate(ext_results[:3]):
#                        if isinstance(res, dict): title = res.get('title', 'N/A'); content_snippet = res.get('content', '')[:150]; prompt_parts.append(f"  - [{i+1}] {title}: {content_snippet}...")
#         else: logger.debug("Web search requested, but no external data found."); prompt_parts.append("\nExternal Web Context: - No relevant web context found or provided.")
#
#     if not data_found:
#         logger.warning("No significant internal or external data blocks were available to format for prompt.")
#         query = original_context.get('query', 'N/A')
#         if primary_task == "analyze_brand_deep_dive": brand = target_brand or 'the brand'; return f"No specific data or web context available to analyze for '{brand}'."
#         elif primary_task == "summarize_amazon_radar": cat = original_context.get('target_category','N/A'); dept = original_context.get('target_department','N/A'); return f"No Amazon product data available for category '{cat}' in department '{dept}'."
#         else: cat = original_context.get('category', 'N/A'); country = original_context.get('country', 'N/A'); return f"No specific data available for query '{query}' on category '{cat}' in {country}."
#
#     return "\n".join(prompt_parts)
# # --- *** MODIFICATION END *** ---
#
#
# # --- *** MODIFICATION START: Remove Markdown Parser *** ---
# # def parse_markdown_flat_subsections(markdown_text: str) -> Dict[str, Any]:
# #     # ... (Removed function definition) ...
# # --- *** MODIFICATION END *** ---
#
#
# # --- Build Final Payload ---
# def build_final_payload_for_bubble(
#     # --- MODIFICATION START: Remove ai_summary_text param ---
#     # ai_summary_text: str,
#     # --- MODIFICATION END ---
#     ai_summary_structured: Dict[str, Any], # Parsed structure
#     internal_data: Dict,
#     external_data: Dict,
#     task_indicator: str,
#     final_status: str,
#     error_message: Optional[str]
#     ) -> Dict:
#     # --- MODIFICATION START: Update payload init/logging ---
#     logger.debug("Building final payload object with structured summary for Bubble...")
#     payload = {
#         "ai_summary_structured": ai_summary_structured, # Include the structured object from LLM/fallback
#         "result_type_indicator": task_indicator if final_status != INDICATOR_ERROR else INDICATOR_ERROR,
#         "status": final_status,
#         "error_message": error_message,
#         "category_trend": [], "top_styles": [], "top_colors": [], "item_trend": [],
#         "item_metrics": [], "mega_trends_top": [], "web_links": [], "web_answer": None,
#         "brand_performance_summary": [],
#         "amazon_radar_products": []
#     }
#     # --- MODIFICATION END ---
#
#     # --- Populate supporting data lists (Keep existing logic) ---
#     internal_data = internal_data or {}; external_data = external_data or {}
#     trends_data = internal_data.get("trends_data") # ...
#     if trends_data: # ... (keep population logic) ...
#         cs = trends_data.get("category_summary");
#         if isinstance(cs, dict): chart = cs.get("chart_data"); payload["category_trend"] = chart if isinstance(chart, list) and chart else []
#         all_styles = trends_data.get("style_details", []); valid_styles = [s for s in all_styles if isinstance(s,dict)]
#         if valid_styles: payload["top_styles"] = [{"name": s.get("style_name"), "growth": s.get("growth_recent"), "volume": s.get("average_volume")} for s in sorted(valid_styles, key=lambda x: x.get('average_volume', 0), reverse=True)[:5]]
#         all_colors = trends_data.get("color_details", []); valid_colors = [c for c in all_colors if isinstance(c,dict)]
#         if valid_colors: payload["top_colors"] = [{"name": c.get("color_name"), "growth": c.get("growth_recent"), "volume": c.get("average_volume")} for c in sorted(valid_colors, key=lambda x: x.get('average_volume', 0), reverse=True)[:5]]
#     chart_details_data = internal_data.get("chart_details_data") # ...
#     if chart_details_data: # ... (keep population logic) ...
#          item_metrics_obj = {"name": chart_details_data.get("category_subject"), "growth": chart_details_data.get("growth_recent"), "volume": chart_details_data.get("average_volume"), "forecast_growth": None, "forecast_volume": None }
#          if task_indicator == INDICATOR_FORECAST:
#              item_metrics_obj["forecast_growth"] = {"f2": chart_details_data.get("f2"), "f3": chart_details_data.get("f3"), "f6": chart_details_data.get("f6")}
#              item_metrics_obj["forecast_volume"] = {"avg2": chart_details_data.get("avg2"), "avg3": chart_details_data.get("avg3"), "avg6": chart_details_data.get("avg6")}
#          payload["item_metrics"] = [item_metrics_obj]
#          item_chart = chart_details_data.get("chart_data"); payload["item_trend"] = item_chart if isinstance(item_chart, list) and item_chart else []
#     mega_trends_data = internal_data.get("mega_trends_data") # ...
#     if mega_trends_data: # ... (keep population logic) ...
#         valid_mega = [m for m in mega_trends_data if isinstance(m,dict)]
#         if valid_mega: payload["mega_trends_top"] = [{"name": m.get("query_name"), "growth": m.get("growth_recent"), "volume": m.get("average_volume"), "category": m.get("category_name")} for m in sorted(valid_mega, key=lambda x: x.get('growth_recent', 0), reverse=True)[:10]]
#     brand_performance_data = internal_data.get("brand_performance_data")
#     if brand_performance_data and isinstance(brand_performance_data, list):
#          payload["brand_performance_summary"] = brand_performance_data
#          logger.debug(f"Populated brand_performance_summary list.")
#     amazon_radar_data_from_internal = internal_data.get("amazon_radar_data")
#     if task_indicator == INDICATOR_AMAZON_RADAR and isinstance(amazon_radar_data_from_internal, dict):
#         products_list = amazon_radar_data_from_internal.get("country_department_category", [])
#         if isinstance(products_list, list):
#             cleaned_products = []
#             for p_item in products_list[:10]: # Limit to 10 for Bubble
#                 if isinstance(p_item, dict): cleaned_products.append({"asin": p_item.get("asin"), "product_url": p_item.get("product_url"), "product_photo": p_item.get("product_photo"), "product_price": p_item.get("product_price"), "currency": p_item.get("currency"), "estimated_revenue": p_item.get("estimated_revenue"), "estimated_orders": p_item.get("estimated_orders"), "number_of_reviews": p_item.get("number_of_reviews"), "product_star_rating": p_item.get("product_star_rating"), "saturation": p_item.get("saturation")})
#             payload["amazon_radar_products"] = cleaned_products
#             logger.debug(f"Populated amazon_radar_products list.")
#     external_results = external_data.get("results", []) if isinstance(external_data.get("results", []), list) else []
#     external_answer = external_data.get("answer") if isinstance(external_data.get("answer"), str) else None
#     if external_results: payload["web_links"] = [{"title": r.get("title"), "url": r.get("url")} for r in external_results if isinstance(r, dict) and r.get("title") and r.get("url")][:5]
#     if external_answer: payload["web_answer"] = external_answer
#
#     logger.info(f"Built final bubble payload object with keys: {list(payload.keys())}")
#     return payload
#
# # --- Main Lambda Handler ---
# # --- Main Lambda Handler ---
# def lambda_handler(event, context):
#     logger.info(f"Received combined event: {json.dumps(event)}")
#
#     # Pre-checks
#     if not GEMINI_SDK_AVAILABLE:
#         error_struct = get_default_summary_structure()
#         error_struct["overall_summary"] = "Error: LLM SDK unavailable."
#         error_payload = build_final_payload_for_bubble( # Removed ai_summary_text
#             ai_summary_structured=error_struct,
#             internal_data={}, external_data={}, task_indicator=INDICATOR_ERROR,
#             final_status=INDICATOR_ERROR, error_message="LLM SDK unavailable."
#         )
#         return {"statusCode": 500, "body": json.dumps(error_payload)}
#     if BOTO3_CLIENT_ERROR:
#         error_struct = get_default_summary_structure()
#         error_struct["overall_summary"] = f"Error: {BOTO3_CLIENT_ERROR}"
#         error_payload = build_final_payload_for_bubble( # Removed ai_summary_text
#              ai_summary_structured=error_struct,
#              internal_data={}, external_data={}, task_indicator=INDICATOR_ERROR,
#              final_status=INDICATOR_ERROR, error_message=BOTO3_CLIENT_ERROR
#         )
#         return {"statusCode": 500, "body": json.dumps(error_payload)}
#
#     # Data extraction
#     internal_data = event.get("internal_data", {})
#     external_data = event.get("external_data", {})
#     interpretation = internal_data.get("interpretation", {}) if isinstance(internal_data, dict) else {}
#     original_context = interpretation.get("original_context", {}) if isinstance(interpretation, dict) else {}
#     primary_task = interpretation.get("primary_task")
#     user_query = original_context.get("query", "the user query")
#     query_subjects = interpretation.get("query_subjects", {}) if isinstance(interpretation.get("query_subjects"), dict) else {}
#     target_brand = query_subjects.get("target_brand")
#     amazon_target_category = original_context.get("target_category", "N/A")
#     amazon_target_department = original_context.get("target_department", "N/A")
#
#     # Upstream Error collection
#     upstream_errors = []
#     if isinstance(internal_data, dict) and internal_data.get("errors"): upstream_errors.extend(internal_data["errors"])
#     if isinstance(internal_data, dict) and internal_data.get("errorType"): upstream_errors.append({"source": "FetchInternalDataRouter", "error": internal_data.get("errorType"), "details": internal_data.get("cause", internal_data.get("errorMessage"))})
#     if isinstance(external_data, dict) and external_data.get("error"): upstream_errors.append({"source": "FetchExternalContext", "error": external_data["error"]})
#     if isinstance(external_data, dict) and external_data.get("errorType"): upstream_errors.append({"source": "FetchExternalContext", "error": external_data.get("errorType"), "details": external_data.get("cause", external_data.get("errorMessage"))})
#
#     result_type_indicator, prompt_template = get_task_details(primary_task)
#     logger.info(f"Using result indicator: {result_type_indicator}")
#
#     specific_item_name = "N/A"
#     if isinstance(interpretation.get("query_subjects"), dict):
#          specific_known = query_subjects.get("specific_known", [])
#          if specific_known and isinstance(specific_known[0], dict): specific_item_name = specific_known[0].get("subject", "N/A")
#
#     formatted_data_context = ""
#     try:
#         formatted_data_context = format_data_for_prompt(internal_data, external_data)
#         prompt_format_args = {
#             "specific_item_name": specific_item_name,
#             "category_name": original_context.get('category', 'N/A'),
#             "country_name": original_context.get('country', 'N/A'),
#             "user_query": user_query,
#             "brand_domain": target_brand or "N/A",
#             "target_category": amazon_target_category,
#             "target_department": amazon_target_department
#         }
#         synthesis_prompt = prompt_template.format(**prompt_format_args)
#         synthesis_prompt += "\n\n" + formatted_data_context
#         logger.debug(f"Constructed Synthesis Prompt:\n{synthesis_prompt}")
#     except KeyError as key_err:
#         logger.error(f"Missing key in prompt template formatting: {key_err}...", exc_info=True)
#         error_struct = get_default_summary_structure()
#         error_struct["overall_summary"] = f"Error: Could not prepare prompt. Missing key: {key_err}"
#         error_payload = build_final_payload_for_bubble( # Removed ai_summary_text
#             ai_summary_structured=error_struct,
#             internal_data=internal_data, external_data=external_data, task_indicator=INDICATOR_ERROR,
#             final_status=INDICATOR_ERROR, error_message=str(key_err)
#         )
#         return {"statusCode": 500, "body": json.dumps(error_payload)}
#     except Exception as e:
#         logger.error(f"Error formatting data for synthesis prompt: {e}", exc_info=True)
#         error_struct = get_default_summary_structure()
#         error_struct["overall_summary"] = f"Error: Could not prepare data for AI synthesis: {e}"
#         error_payload = build_final_payload_for_bubble( # Removed ai_summary_text
#             ai_summary_structured=error_struct,
#             internal_data=internal_data, external_data=external_data, task_indicator=INDICATOR_ERROR,
#             final_status=INDICATOR_ERROR, error_message=str(e)
#         )
#         return {"statusCode": 500, "body": json.dumps(error_payload)}
#
#     # --- Secret Retrieval ---
#     google_api_key = get_secret_value(SECRET_NAME, "GOOGLE_API_KEY")
#     if not google_api_key:
#         error_struct = get_default_summary_structure()
#         error_struct["overall_summary"] = "Error: API key configuration error (Google)."
#         error_payload = build_final_payload_for_bubble( # Removed ai_summary_text
#             ai_summary_structured=error_struct,
#             internal_data=internal_data, external_data=external_data,
#             task_indicator=INDICATOR_ERROR, final_status=INDICATOR_ERROR,
#             error_message="API key config error"
#         )
#         return {"statusCode": 500, "body": json.dumps(error_payload)}
#
#     # --- LLM Call ---
#     ai_summary_structured = None # Will hold the dict from LLM
#     llm_error = None
#     try:
#         if not formatted_data_context or formatted_data_context.startswith("No specific data available"):
#              logger.warning("Skipping LLM call as no significant data was formatted for prompt.")
#              ai_summary_structured = get_default_summary_structure()
#              ai_summary_structured["overall_summary"] = PERSONA_PROMPTS[INDICATOR_UNKNOWN].format(
#                  user_query=user_query,
#                  category_name=original_context.get('category', 'N/A'),
#                  country_name=original_context.get('country', 'N/A')
#              )
#              # WARNING: This prompt itself might need updating if it uses keys not available
#              # for the UNKNOWN indicator, like brand_domain. For now, assuming it's simple.
#              result_type_indicator = INDICATOR_UNKNOWN
#         else:
#              logger.info(f"Calling Synthesis LLM: {SYNTHESIS_LLM_MODEL}...")
#              genai.configure(api_key=google_api_key)
#              model = genai.GenerativeModel(SYNTHESIS_LLM_MODEL)
#              generation_config = genai_types.GenerationConfig(response_mime_type="application/json")
#              response = model.generate_content(synthesis_prompt, generation_config=generation_config)
#              logger.info("Synthesis LLM response received.")
#              raw_llm_text = response.text # Keep raw text for debugging if needed
#              logger.debug(f"LLM Raw Response Text:\n{raw_llm_text}")
#
#              try:
#                  cleaned_text = raw_llm_text.strip()
#                  if cleaned_text.startswith("```json"): cleaned_text = cleaned_text[7:]
#                  if cleaned_text.endswith("```"): cleaned_text = cleaned_text[:-3]
#                  cleaned_text = cleaned_text.strip()
#                  if not cleaned_text: raise ValueError("LLM returned empty JSON string.")
#
#                  llm_output_json = json.loads(cleaned_text)
#
#                  if validate_structured_summary(llm_output_json):
#                      ai_summary_structured = llm_output_json
#                      logger.info("Successfully parsed and validated structured summary from LLM.")
#                  else:
#                      logger.error(f"LLM JSON output failed validation. Structure received: {json.dumps(llm_output_json)}")
#                      raise ValueError("LLM JSON output failed schema validation.")
#
#              except (json.JSONDecodeError, ValueError) as parse_err:
#                   logger.error(f"Failed to parse or validate LLM JSON response: {parse_err}", exc_info=True)
#                   llm_error = f"LLM response parsing/validation error: {parse_err}. Raw text: {raw_llm_text}"
#                   ai_summary_structured = get_default_summary_structure()
#                   ai_summary_structured["overall_summary"] = f"Error: Could not process AI analysis. Details: {parse_err}"
#
#     except Exception as e:
#         logger.error(f"Synthesis LLM call failed: {e}", exc_info=True)
#         llm_error = f"Synthesis LLM call failed: {str(e)}"
#         ai_summary_structured = get_default_summary_structure()
#         ai_summary_structured["overall_summary"] = "An error occurred during the analysis synthesis."
#
#     # --- Build Final Payload ---
#     final_bubble_payload = {}
#     try:
#         final_status = "success"; user_error_message = None
#         if llm_error:
#             final_status = INDICATOR_ERROR; user_error_message = llm_error; result_type_indicator = INDICATOR_ERROR
#             # Ensure fallback structure is used if parsing/validation failed
#             if not ai_summary_structured or not validate_structured_summary(ai_summary_structured):
#                  ai_summary_structured = get_default_summary_structure()
#                  ai_summary_structured["overall_summary"] = "An error occurred during AI synthesis or response processing."
#
#         elif upstream_errors:
#             final_status = "partial_data_success"; logger.warning(f"Upstream errors detected: {upstream_errors}")
#             # Add note to the structured summary
#             if isinstance(ai_summary_structured, dict) and "overall_summary" in ai_summary_structured:
#                  intro_prefix = "Note: Analysis may be incomplete due to errors fetching some data. "
#                  ai_summary_structured["overall_summary"] = intro_prefix + ai_summary_structured.get("overall_summary", "")
#
#         # Call the builder function WITHOUT ai_summary_text
#         final_bubble_payload = build_final_payload_for_bubble(
#             ai_summary_structured=ai_summary_structured,
#             internal_data=internal_data, external_data=external_data,
#             task_indicator=result_type_indicator, final_status=final_status, error_message=user_error_message
#         )
#         logger.info(f"Final payload status: {final_status}")
#
#     except Exception as e:
#          logger.error(f"Error during final payload building: {e}", exc_info=True)
#          error_struct = get_default_summary_structure()
#          error_struct["overall_summary"] = f"An critical error occurred preparing the final response: {e}"
#          # Construct final error payload - also remove ai_summary_text here
#          final_bubble_payload = {
#              "ai_summary_structured": error_struct,
#              "result_type_indicator": INDICATOR_ERROR, "status": INDICATOR_ERROR,
#              "error_message": f"Payload construction error: {str(e)}",
#              "category_trend": [], "top_styles": [], "top_colors": [], "item_trend": [],
#              "item_metrics": [], "mega_trends_top": [], "web_links": [], "web_answer": None,
#              "brand_performance_summary": [], "amazon_radar_products": []
#          }
#
#     return {"statusCode": 200, "body": json.dumps(final_bubble_payload)}


import json
import logging
import os
import re
from typing import Dict, Optional, List, Any, Tuple
from decimal import Decimal  # Needed for replace_decimals

import boto3
from botocore.exceptions import ClientError

try:
    import google.generativeai as genai
    import google.generativeai.types as genai_types

    GEMINI_SDK_AVAILABLE = True
except ImportError:
    genai = None
    genai_types = None
    GEMINI_SDK_AVAILABLE = False
    logging.basicConfig(level="ERROR")
    logging.error("CRITICAL: google-generativeai SDK not found! Install it.")

# --- Configuration and Constants ---
SECRET_NAME = os.environ.get("SECRET_NAME", "YourSecretsName")
SYNTHESIS_LLM_MODEL = os.environ.get("SYNTHESIS_LLM_MODEL", "gemini-2.0-flash")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
COMPARE_CATEGORIES_TASK_NAME = "compare_categories_task"
# --- Result Type Indicators ---
INDICATOR_TREND_DETAIL = "TREND_DETAIL"
INDICATOR_MEGA_TREND = "MEGA_TREND"
INDICATOR_CATEGORY_OVERVIEW = "CATEGORY_OVERVIEW"
INDICATOR_FORECAST = "FORECAST"
# --- NEW: Comparison Indicator ---
INDICATOR_CATEGORY_COMPARISON = "CATEGORY_COMPARISON"
# --- END NEW ---
INDICATOR_RECOMMENDATION = "RECOMMENDATION"
INDICATOR_QA_WEB = "QA_WEB"  # Keep for now, alias later if needed
INDICATOR_QA_INTERNAL = "QA_INTERNAL"  # Keep for now
INDICATOR_QA_COMBINED = "QA_COMBINED"  # Keep for now
INDICATOR_UNKNOWN = "UNKNOWN"
INDICATOR_ERROR = "ERROR"
INDICATOR_CLARIFICATION = "CLARIFICATION_NEEDED"
INDICATOR_BRAND_ANALYSIS = "BRAND_ANALYSIS"
INDICATOR_AMAZON_RADAR = "AMAZON_RADAR"
INDICATOR_WEB_SUMMARY = "WEB_SUMMARY"

# --- Logger Setup ---
# (Setup unchanged)
logger = logging.getLogger()
log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
if log_level_str not in valid_log_levels: log_level_str = "INFO"
logger.setLevel(log_level_str)
logger.info(f"Logger initialized with level: {log_level_str}")
logger.info(f"SYNTHESIS_LLM_MODEL: {SYNTHESIS_LLM_MODEL}")
logger.info(f"SECRET_NAME: {SECRET_NAME}")

# --- Boto3 Client and Secret Handling ---
# (Setup unchanged)
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
        if direct_key:
            logger.info(f"Using direct env var '{key_name}' (local mode)"); return direct_key
        else:
            logger.warning(f"Direct env var '{key_name}' not found. Trying Secrets Manager...")
    global API_KEY_CACHE;
    cache_key = f"{secret_name}:{key_name}"
    if cache_key in API_KEY_CACHE: logger.debug(f"Using cached secret key: {cache_key}"); return API_KEY_CACHE[
        cache_key]
    if BOTO3_CLIENT_ERROR: logger.error(f"Boto3 client error: {BOTO3_CLIENT_ERROR}"); return None
    if not secrets_manager: logger.error("Secrets Manager client not initialized."); return None
    try:
        logger.info(f"Fetching secret '{secret_name}' to get key '{key_name}'")
        response = secrets_manager.get_secret_value(SecretId=secret_name)
        secret_dict = None
        if 'SecretString' in response:
            try:
                secret_dict = json.loads(response['SecretString'])
            except json.JSONDecodeError as e:
                logger.error(f"Failed JSON parse: {e}"); return None
        elif 'SecretBinary' in response:
            try:
                secret_dict = json.loads(response['SecretBinary'].decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(f"Failed binary decode: {e}"); return None
        else:
            logger.error("Secret value not found."); return None
        if not isinstance(secret_dict, dict): logger.error("Parsed secret not dict."); return None
        key_value = secret_dict.get(key_name)
        if not key_value or not isinstance(key_value, str):
            logger.error(f"Key '{key_name}' not found or not string in secret '{secret_name}'.");
            API_KEY_CACHE[cache_key] = None;
            return None
        API_KEY_CACHE[cache_key] = key_value;
        logger.info(f"Key '{key_name}' successfully retrieved and cached.");
        return key_value
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code");
        logger.error(f"AWS ClientError for '{secret_name}': {error_code}");
        API_KEY_CACHE[cache_key] = None;
        return None
    except Exception as e:
        logger.exception(f"Unexpected error retrieving secret '{secret_name}'.");
        API_KEY_CACHE[cache_key] = None;
        return None


# --- Default Schema and Validation ---
# (Setup unchanged)
DEFAULT_AI_SUMMARY_STRUCTURE = {
    "overall_summary": "",
    "sections": [
        {"id": "category_context", "heading": "Current Category Context", "content": "", "points": []},
        {"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "", "points": []},
        {"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "", "points": []},
        {"id": "mega_trends", "heading": "Key Mega Trends", "content": "", "points": []},
        {"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "", "points": []},
        {"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "", "points": []},
        # --- NEW: Ensure "comparison" section ID exists in default ---
        {"id": "comparison", "heading": "Comparison Points", "content": "", "points": []},
        # Changed from comparison_points
        # --- END NEW ---
        {"id": "web_context", "heading": "Web Context & News", "content": "", "points": []},
        {"id": "recommendations", "heading": "Recommendations", "content": "", "points": []}
    ]
}


def get_default_summary_structure() -> Dict:
    return json.loads(json.dumps(DEFAULT_AI_SUMMARY_STRUCTURE))


def validate_structured_summary(summary_obj: Any) -> bool:
    if not isinstance(summary_obj, dict): return False
    if "overall_summary" not in summary_obj or not isinstance(summary_obj["overall_summary"], str): return False
    if "sections" not in summary_obj or not isinstance(summary_obj["sections"], list): return False
    for section in summary_obj["sections"]:
        if not isinstance(section, dict): return False
        if not all(key in section for key in ["id", "heading", "content", "points"]): return False
        if not isinstance(section.get("points"), list): return False
        for point in section["points"]:
            if not isinstance(point, dict) or "text" not in point: return False
    return True


# --- Persona Prompts ---
PERSONA_PROMPTS = {
    # (Existing prompts for TREND_DETAIL, MEGA_TREND, CATEGORY_OVERVIEW, FORECAST, BRAND_ANALYSIS, AMAZON_RADAR, WEB_SUMMARY unchanged - they already output JSON)
    INDICATOR_TREND_DETAIL: """You are a TrendForecast.io Expert Analyst providing insights on {specific_item_name} in {country_name}.
    Analyze the provided internal data (category summary, style/color details, specific item details) and any relevant 'External Web Context'.

    Instructions:
    1. Generate ONLY a valid JSON object adhering EXACTLY to the schema provided below.
    2. Populate the `overall_summary` field with a concise narrative summarizing the key trend findings for {specific_item_name}.
    3. Populate the `item_trend_detail` section:
        - Set the `content` field to describe the current trend status (e.g., growing, declining, stable) based on recent historical performance (avg volume, growth) and potentially supporting points from web context or category context. Mention the specific avg volume and growth % from the data.
        - Use the `points` list for specific supporting details or drivers if applicable (format as {{"text": "..."}}).
    4. If relevant 'Internal Category Context' data is provided, summarize how the specific item's trend relates to the broader category in the `category_context` section's `content` field.
    5. If 'External Web Context' is available and relevant, summarize key insights/news influencing the item's trend in the `web_context` section's `content` or `points`.
    6. For all other sections in the schema (e.g., `forecast_outlook`, `mega_trends`, etc.), provide empty values: `content: ""` and `points: []`.
    7. DO NOT include markdown formatting (like **, *) within the JSON string values unless part of the actual data.

    JSON Schema to follow:
    ```json
    {{
      "overall_summary": "string",
      "sections": [
        {{"id": "category_context", "heading": "Current Category Context", "content": "string", "points": [{{"text": "string"}}, ...]}},
        {{"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "string", "points": [{{"text": "string"}}, ...]}},
        {{"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "", "points": []}},
        {{"id": "mega_trends", "heading": "Key Mega Trends", "content": "", "points": []}},
        {{"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "", "points": []}},
        {{"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "", "points": []}},
        {{"id": "web_context", "heading": "Web Context & News", "content": "string", "points": [{{"text": "string"}}, ...]}},
        {{"id": "recommendations", "heading": "Recommendations", "content": "", "points": []}},
        {{"id": "comparison", "heading": "Comparison Points", "content": "", "points": []}}
      ]
    }}
    ```""",

        # --- Mega Trend ---
        INDICATOR_MEGA_TREND: """You are a TrendForecast.io Senior Strategist providing a synthesized analysis of the '{category_name}' category in '{country_name}'.
    Analyze the provided data sources: Internal Mega Trends (list of top trending search queries/topics), Internal Category Context (overall category performance, top styles/colors), and External Web Context (synthesized answer or top results).

    Instructions:
    1. Generate ONLY a valid JSON object adhering EXACTLY to the schema provided below.
    2. Populate the `overall_summary` field with a concise narrative synthesizing the key findings about mega trends and their relation to the category context and web insights.
    3. Populate the `mega_trends` section:
        - Set the `content` field to describe the main themes emerging from the top queries.
        - Use the `points` list to list the top 3-5 identified mega trends (format as {{"text": "Trend Name: Supporting query example"}}).
    4. Populate the `category_context` section:
        - Set the `content` field to discuss how the identified mega trends relate to or influence the observed category performance, top styles, and top colors. Note alignments or disconnects.
    5. Populate the `web_context` section:
        - Set the `content` or `points` field to summarize relevant confirmations or additional context from the web search data.
    6. For all other sections (e.g., `item_trend_detail`, `forecast_outlook`, etc.), provide empty values: `content: ""` and `points: []`.
    7. DO NOT include markdown formatting within the JSON string values.

    JSON Schema to follow:
    ```json
    {{
      "overall_summary": "string",
      "sections": [
        {{"id": "category_context", "heading": "Current Category Context", "content": "string", "points": [{{"text": "string"}}, ...]}},
        {{"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "", "points": []}},
        {{"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "", "points": []}},
        {{"id": "mega_trends", "heading": "Key Mega Trends", "content": "string", "points": [{{"text": "string"}}, ...]}},
        {{"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "", "points": []}},
        {{"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "", "points": []}},
        {{"id": "web_context", "heading": "Web Context & News", "content": "string", "points": [{{"text": "string"}}, ...]}},
        {{"id": "recommendations", "heading": "Recommendations", "content": "", "points": []}},
        {{"id": "comparison", "heading": "Comparison Points", "content": "", "points": []}}
      ]
    }}
    ```""",

        # --- Category Overview ---
        INDICATOR_CATEGORY_OVERVIEW: """You are a TrendForecast.io Market Analyst providing a high-level overview for the '{category_name}' category in '{country_name}'.
    Analyze the provided category summary data, top style/color details, and web context.

    Instructions:
    1. Generate ONLY a valid JSON object adhering EXACTLY to the schema provided below.
    2. Populate the `overall_summary` field with a concise narrative summarizing the category's current state.
    3. Populate the `category_context` section:
        - Set the `content` field to describe the overall performance (growth, volume).
        - Use the `points` list to highlight the top 2-3 performing styles and top 2-3 performing colors based on the data (format as {{"text": "Top Style: [Name] (Growth: X%, Vol: Y)"}} or {{"text": "Top Color: [Name] (Growth: X%, Vol: Y)"}}).
    4. If 'External Web Context' is available, summarize relevant insights in the `web_context` section's `content` or `points`.
    5. For all other sections (e.g., `item_trend_detail`, `forecast_outlook`, etc.), provide empty values: `content: ""` and `points: []`.
    6. DO NOT include markdown formatting within the JSON string values.

    JSON Schema to follow:
    ```json
    {{
      "overall_summary": "string",
      "sections": [
        {{"id": "category_context", "heading": "Current Category Context", "content": "string", "points": [{{"text": "string"}}, ...]}},
        {{"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "", "points": []}},
        {{"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "", "points": []}},
        {{"id": "mega_trends", "heading": "Key Mega Trends", "content": "", "points": []}},
        {{"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "", "points": []}},
        {{"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "", "points": []}},
        {{"id": "web_context", "heading": "Web Context & News", "content": "string", "points": [{{"text": "string"}}, ...]}},
        {{"id": "recommendations", "heading": "Recommendations", "content": "", "points": []}},
        {{"id": "comparison", "heading": "Comparison Points", "content": "", "points": []}}
      ]
    }}
    ```""",

        # --- Forecast --- (Already Updated in previous step)
        INDICATOR_FORECAST: """You are a TrendForecast.io Forecast Specialist providing predictions for {specific_item_name} in {country_name}.
    Analyze the provided specific item details (avg volume, growth, forecast data points f2, f3, f6, avg2, avg3, avg6) and any relevant 'External Web Context'.

    Instructions:
    1. Generate ONLY a valid JSON object adhering EXACTLY to the schema provided below.
    2. Populate the `overall_summary` field with a concise narrative summarizing the forecast outlook.
    3. Populate the `forecast_outlook` section:
        - Set the `content` field to explain the growth trajectory based on the forecast numbers.
        - Populate the `points` list with individual forecast metrics. Each point MUST be an object `{{"text": "..."}}`. Use the EXACT phrasing: "forecasted growth in search demand is X% for Y months" or "forecasted average search demand is Z for Y months". Include all 6 data points (f2, f3, f6, avg2, avg3, avg6).
    4. Populate the `item_trend_detail` section's `content` field with a brief mention of the item's recent historical performance (avg vol, growth) from the provided data.
    5. If 'External Web Context' is available and relevant, summarize key points influencing the forecast in the `web_context` section's `content` or `points`.
    6. For all other sections in the schema (e.g., `category_context`, `mega_trends`, etc.), provide empty values: `content: ""` and `points: []`.
    7. DO NOT include markdown formatting within the JSON string values unless part of the actual data.

    JSON Schema to follow:
    ```json
    {{
      "overall_summary": "string",
      "sections": [
        {{"id": "category_context", "heading": "Current Category Context", "content": "", "points": []}},
        {{"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "string", "points": []}},
        {{"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "string", "points": [{{"text": "string"}}, ...]}},
        {{"id": "mega_trends", "heading": "Key Mega Trends", "content": "", "points": []}},
        {{"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "", "points": []}},
        {{"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "", "points": []}},
        {{"id": "web_context", "heading": "Web Context & News", "content": "string", "points": [{{"text": "string"}}, ...]}},
        {{"id": "recommendations", "heading": "Recommendations", "content": "", "points": []}},
        {{"id": "comparison", "heading": "Comparison Points", "content": "", "points": []}}
      ]
    }}
    ```""",

        # --- Brand Analysis ---
        INDICATOR_BRAND_ANALYSIS: """You are a TrendForecast.io Brand Analyst for brand '{brand_domain}'.
    Analyze the provided 'Internal Brand Performance Data' (estimated visits/growth per country) and any relevant 'External Web Context' (summarized answer or top results).

    Instructions:
    1. Generate ONLY a valid JSON object adhering EXACTLY to the schema provided below.
    2. Populate the `overall_summary` field with a concise narrative summarizing the brand's key performance findings and web context insights.
    3. Populate the `brand_performance` section:
        - Set the `content` field to discuss overall performance trends across countries.
        - Use the `points` list to highlight specific country performance (e.g., {{"text": "USA: High volume (X visits), Strong growth (Y%)"}}). Include highlights for 2-3 key countries.
    4. Populate the `web_context` section:
        - Set the `content` or `points` to summarize key findings from the web search (news, competitor mentions, sentiment).
    5. For all other sections (e.g., `category_context`, `forecast_outlook`, etc.), provide empty values: `content: ""` and `points: []`.
    6. DO NOT include markdown formatting within the JSON string values.

    JSON Schema to follow:
    ```json
    {{
      "overall_summary": "string",
      "sections": [
        {{"id": "category_context", "heading": "Current Category Context", "content": "", "points": []}},
        {{"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "", "points": []}},
        {{"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "", "points": []}},
        {{"id": "mega_trends", "heading": "Key Mega Trends", "content": "", "points": []}},
        {{"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "string", "points": [{{"text": "string"}}, ...]}},
        {{"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "", "points": []}},
        {{"id": "web_context", "heading": "Web Context & News", "content": "string", "points": [{{"text": "string"}}, ...]}},
        {{"id": "recommendations", "heading": "Recommendations", "content": "", "points": []}},
        {{"id": "comparison", "heading": "Comparison Points", "content": "", "points": []}}
      ]
    }}
    ```""",

        # --- Amazon Radar ---
        INDICATOR_AMAZON_RADAR: """You are a TrendForecast.io Amazon Market Analyst for '{target_category}' ({target_department}) in {country_name}.
    Analyze the provided 'Top Amazon Products' list and 'Amazon Category Department Market Size Context'.

    Instructions:
    1. Generate ONLY a valid JSON object adhering EXACTLY to the schema provided below.
    2. Populate the `overall_summary` field with a concise narrative summarizing the key findings from the Amazon data.
    3. Populate the `amazon_radar` section:
        - Set the `content` field to discuss general observations (e.g., price ranges, saturation, common brands if apparent).
        - Use the `points` list to highlight the top 3 products, mentioning key metrics like ASIN, Price, Rating, Est. Revenue (format as {{"text": "Top Product: ASIN XXX, Price $Y, Rating Z, Revenue ~$W"}}).
    4. Populate the `category_context` section's `content` field *only* with the provided market share information (e.g., "This category represents approx. X% share of the department on Amazon.").
    5. For all other sections (e.g., `item_trend_detail`, `forecast_outlook`, etc.), provide empty values: `content: ""` and `points: []`.
    6. DO NOT include markdown formatting within the JSON string values.

    JSON Schema to follow:
    ```json
    {{
      "overall_summary": "string",
      "sections": [
        {{"id": "category_context", "heading": "Current Category Context", "content": "string", "points": []}}, // For market share
        {{"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "", "points": []}},
        {{"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "", "points": []}},
        {{"id": "mega_trends", "heading": "Key Mega Trends", "content": "", "points": []}},
        {{"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "", "points": []}},
        {{"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "string", "points": [{{"text": "string"}}, ...]}},
        {{"id": "web_context", "heading": "Web Context & News", "content": "", "points": []}},
        {{"id": "recommendations", "heading": "Recommendations", "content": "", "points": []}},
        {{"id": "comparison", "heading": "Comparison Points", "content": "", "points": []}}
      ]
    }}
    ```""",

        # --- Web Summary (Consolidated from QA_Web) ---
        INDICATOR_WEB_SUMMARY: """You are a helpful AI assistant answering the query '{user_query}' for {country_name}.
    Analyze ONLY the provided 'External Web Context' (Synthesized Answer or Top Results Snippets).

    Instructions:
    1. Generate ONLY a valid JSON object adhering EXACTLY to the schema provided below.
    2. Populate the `overall_summary` field with a concise answer to the user's query, synthesized *only* from the provided web context.
    3. Populate the `web_context` section:
        - Set the `content` field to elaborate on the answer or provide key supporting details from the web context.
        - Use the `points` list to list key findings or source snippets (format as {{"text": "..."}}).
    4. For all other sections (e.g., `category_context`, `item_trend_detail`, etc.), provide empty values: `content: ""` and `points: []`.
    5. DO NOT include markdown formatting within the JSON string values.

    JSON Schema to follow:
    ```json
    {{
      "overall_summary": "string",
      "sections": [
        {{"id": "category_context", "heading": "Current Category Context", "content": "", "points": []}},
        {{"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "", "points": []}},
        {{"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "", "points": []}},
        {{"id": "mega_trends", "heading": "Key Mega Trends", "content": "", "points": []}},
        {{"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "", "points": []}},
        {{"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "", "points": []}},
        {{"id": "web_context", "heading": "Web Context & News", "content": "string", "points": [{{"text": "string"}}, ...]}},
        {{"id": "recommendations", "heading": "Recommendations", "content": "", "points": []}},
        {{"id": "comparison", "heading": "Comparison Points", "content": "", "points": []}}
      ]
    }}
    ```""",

    # --- NEW: Prompt for Category Comparison ---
    INDICATOR_CATEGORY_COMPARISON: """You are a TrendForecast.io Comparative Analyst comparing the performance of multiple fashion categories in {country_name}.
The user wants to compare: {category_list_str}.
Analyze the provided 'Internal Trend Data' for each category and any relevant 'External Web Context'.

Instructions:
1. Generate ONLY a valid JSON object adhering EXACTLY to the schema provided below.
2. Populate the `overall_summary` field with a concise narrative summarizing the main comparison points and overall finding (e.g., which category is performing better or key differentiating factors).
3. Populate the `comparison` section (id: "comparison"):
    - Dynamically set the `heading` to reflect the items being compared (e.g., "Comparison: Jeans vs. Pants").
    - Use the `content` field for a brief overview of the comparison.
    - Use the `points` list to detail the performance of EACH category. Include key metrics like average volume and growth percentage. Format as {{"text": "Category Name: Growth X%, Volume Y"}}.
    - Add further points highlighting key similarities or differences discovered.
4. If 'External Web Context' provides relevant insights explaining the differences or similarities, summarize these in the `web_context` section's `content` or `points`.
5. For all other sections in the schema (e.g., `category_context`, `item_trend_detail`, etc.), provide empty values: `content: ""` and `points: []`.
6. DO NOT include markdown formatting within the JSON string values.

JSON Schema to follow:
```json
{{
  "overall_summary": "string",
  "sections": [
    {{"id": "category_context", "heading": "Current Category Context", "content": "", "points": []}},
    {{"id": "item_trend_detail", "heading": "Specific Item Trend Detail", "content": "", "points": []}},
    {{"id": "forecast_outlook", "heading": "Forecast & Outlook", "content": "", "points": []}},
    {{"id": "mega_trends", "heading": "Key Mega Trends", "content": "", "points": []}},
    {{"id": "brand_performance", "heading": "Brand Performance Highlights", "content": "", "points": []}},
    {{"id": "amazon_radar", "heading": "Amazon Product Highlights", "content": "", "points": []}},
    {{"id": "comparison", "heading": "Comparison: [Category A] vs. [Category B]", "content": "string", "points": [{{"text": "string"}}, ...]}},
    {{"id": "web_context", "heading": "Web Context & News", "content": "string", "points": [{{"text": "string"}}, ...]}},
    {{"id": "recommendations", "heading": "Recommendations", "content": "", "points": []}}
    // Note: Ensure the schema here includes all default section IDs
  ]
}}
```""",
    # --- END NEW ---

    # --- TODO: Update These Prompts for JSON Schema ---
    INDICATOR_RECOMMENDATION: """[TODO: Update to output JSON schema]""",
    INDICATOR_QA_INTERNAL: """[TODO: Update to output JSON schema]""",
    INDICATOR_QA_COMBINED: """[TODO: Update to output JSON schema]""",
    INDICATOR_UNKNOWN: """... (Keep existing or update if needed) ...""",
    INDICATOR_ERROR: "Error processing request.",  # Not sent to LLM
}
# Aliases/Removals if needed
PERSONA_PROMPTS[INDICATOR_QA_WEB] = PERSONA_PROMPTS[INDICATOR_WEB_SUMMARY]  # Alias QA_WEB


# Could remove QA_WEB key entirely later

# --- Update get_task_details ---
def get_task_details(primary_task: str | None) -> Tuple[str, str]:
    indicator = INDICATOR_UNKNOWN
    # Existing mappings...
    if primary_task == "get_trend":
        indicator = INDICATOR_TREND_DETAIL
    elif primary_task == "get_forecast":
        indicator = INDICATOR_FORECAST
    elif primary_task == "summarize_mega_trends":
        indicator = INDICATOR_MEGA_TREND
    elif primary_task == "summarize_category":
        indicator = INDICATOR_CATEGORY_OVERVIEW
    # --- NEW: Add mapping for category comparison ---
    elif primary_task == "compare_categories_task":  # Match task name from Interpreter
        indicator = INDICATOR_CATEGORY_COMPARISON
    # --- END NEW ---
    elif primary_task == "compare_items":
        indicator = INDICATOR_CATEGORY_COMPARISON  # Reuse for item comparison initially? Decide later.
    elif primary_task == "get_recommendation":
        indicator = INDICATOR_RECOMMENDATION
    elif primary_task == "analyze_brand_deep_dive":
        indicator = INDICATOR_BRAND_ANALYSIS
    elif primary_task == "summarize_amazon_radar":
        indicator = INDICATOR_AMAZON_RADAR
    elif primary_task == "summarize_web_trends":
        indicator = INDICATOR_WEB_SUMMARY
    elif primary_task == "qa_web_only":
        indicator = INDICATOR_WEB_SUMMARY  # Alias
    elif primary_task == "qa_internal_only":
        indicator = INDICATOR_QA_INTERNAL
    elif primary_task == "qa_combined":
        indicator = INDICATOR_QA_COMBINED
    elif primary_task == "unknown":
        indicator = INDICATOR_UNKNOWN
    elif primary_task == "error":
        indicator = INDICATOR_ERROR

    prompt_template = PERSONA_PROMPTS.get(indicator, PERSONA_PROMPTS[INDICATOR_UNKNOWN])
    logger.info(f"Mapped primary_task '{primary_task}' to indicator '{indicator}'.")
    return indicator, prompt_template


# --- Update format_data_for_prompt ---
def format_data_for_prompt(internal_data: Dict, external_data: Dict) -> str:
    prompt_parts = []
    internal_data = internal_data or {}
    external_data = external_data or {}
    interpretation = internal_data.get("interpretation") if isinstance(internal_data.get("interpretation"),
                                                                       dict) else {}
    original_context = interpretation.get("original_context") if isinstance(interpretation.get("original_context"),
                                                                            dict) else {}
    query_subjects = interpretation.get("query_subjects") if isinstance(interpretation.get("query_subjects"),
                                                                        dict) else {}
    specific_known = query_subjects.get("specific_known", []) if isinstance(query_subjects.get("specific_known"),
                                                                            list) else []
    primary_task = interpretation.get("primary_task")
    target_brand = query_subjects.get("target_brand")
    # --- NEW: Get comparison subjects if present ---
    comparison_subjects = query_subjects.get("comparison_subjects", [])
    category_list_str = ", ".join(
        [subj.get("subject", "N/A") for subj in comparison_subjects if isinstance(subj, dict)])
    # --- END NEW ---

    # Basic Context
    prompt_parts.append("CONTEXT:")
    prompt_parts.append(f"- User Query: {original_context.get('query', 'N/A')}")
    # --- NEW: Adjust context display for comparison ---
    if primary_task == COMPARE_CATEGORIES_TASK_NAME and category_list_str:

        prompt_parts.append(f"- Task: Compare Categories ({category_list_str})")
        prompt_parts.append(f"- Country: {original_context.get('country', 'N/A')}")
    # --- END NEW ---
    elif primary_task == "analyze_brand_deep_dive" and target_brand:
        prompt_parts.append(f"- Brand Focus: {target_brand}");
        prompt_parts.append(f"- Country Context: {original_context.get('country', 'N/A')}")
    elif primary_task == "summarize_amazon_radar":
        prompt_parts.append(
            f"- Amazon Radar For: Category '{original_context.get('target_category', 'N/A')}', Department '{original_context.get('target_department', 'N/A')}'");
        prompt_parts.append(f"- Country: {original_context.get('country', 'N/A')}")
    elif primary_task == "summarize_web_trends":
        prompt_parts.append(f"- Topic: General fashion trends");
        prompt_parts.append(f"- Country: {original_context.get('country', 'N/A')}")
    else:  # Standard single category/item context
        prompt_parts.append(f"- Category: {original_context.get('category', 'N/A')}");
        prompt_parts.append(f"- Country: {original_context.get('country', 'N/A')}")
        if specific_known and isinstance(specific_known[0], dict): specific_item_name = specific_known[0].get("subject",
                                                                                                              "N/A"); prompt_parts.append(
            f"- Specific Focus Item: {specific_item_name}")

    prompt_parts.append("\nAVAILABLE DATA:")
    data_found = False

    # --- NEW: Format Comparison Data ---
    trends_comparison_data = internal_data.get("trends_data_comparison")
    if isinstance(trends_comparison_data, list) and trends_comparison_data:
        data_found = True
        prompt_parts.append("\nInternal Trend Data (Comparison):")
        for comp_item in trends_comparison_data:
            if isinstance(comp_item, dict):
                cat_name = comp_item.get("category_name", "Unknown Category")
                cat_data = comp_item.get("data", {})  # This holds the structure from TREND_MAIN_LAMBDA_NAME

                prompt_parts.append(f"\n--- Data for {cat_name} ---")
                cs = cat_data.get("category_summary")
                if isinstance(cs, dict):
                    prompt_parts.append(
                        f"- Overall Category ({cs.get('category_name', cat_name)}): Avg Vol={cs.get('average_volume', 'N/A')}, Growth={cs.get('growth_recent', 'N/A'):.1f}%")
                else:
                    prompt_parts.append("- Overall Category: Data missing.")

                style_details = cat_data.get("style_details", []);
                color_details = cat_data.get("color_details", [])
                if isinstance(style_details, list): top_styles = sorted(
                    [s for s in style_details if isinstance(s, dict)], key=lambda x: x.get('average_volume', 0),
                    reverse=True)[:3];
                if top_styles:
                    prompt_parts.append(
                        f"- Top Styles in {cat_name}: {', '.join([s.get('style_name', 'N/A') for s in top_styles])}")
                else:
                    prompt_parts.append(f"- Top Styles in {cat_name}: None found.")

                if isinstance(color_details, list): top_colors = sorted(
                    [c for c in color_details if isinstance(c, dict)], key=lambda x: x.get('average_volume', 0),
                    reverse=True)[:3];
                if top_colors:
                    prompt_parts.append(
                        f"- Top Colors in {cat_name}: {', '.join([c.get('color_name', 'N/A') for c in top_colors])}")
                else:
                    prompt_parts.append(f"- Top Colors in {cat_name}: None found.")
    # --- END NEW ---

    # Standard Single Trend Data (Only if not comparison)
    trends_single_data = internal_data.get("trends_data")
    if not trends_comparison_data and isinstance(trends_single_data, dict):
        data_found = True;
        prompt_parts.append("\nInternal Category Context:")
        cs = trends_single_data.get("category_summary");
        if isinstance(cs, dict): prompt_parts.append(
            f"- Overall Category ({cs.get('category_name', 'N/A')}): Avg Vol={cs.get('average_volume', 'N/A')}, Growth={cs.get('growth_recent', 'N/A'):.1f}%")
        style_details = trends_single_data.get("style_details", []);
        color_details = trends_single_data.get("color_details", [])
        if isinstance(style_details, list): top_styles = sorted([s for s in style_details if isinstance(s, dict)],
                                                                key=lambda x: x.get('average_volume', 0), reverse=True)[
                                                         :3];
        if top_styles: prompt_parts.append(
            f"- Top Styles in Category: {', '.join([s.get('style_name', 'N/A') for s in top_styles])}")
        if isinstance(color_details, list): top_colors = sorted([c for c in color_details if isinstance(c, dict)],
                                                                key=lambda x: x.get('average_volume', 0), reverse=True)[
                                                         :3];
        if top_colors: prompt_parts.append(
            f"- Top Colors in Category: {', '.join([c.get('color_name', 'N/A') for c in top_colors])}")

    # Other data sections (Brand, Amazon, Item Details, Mega) - Keep existing logic
    # Brand Data
    brand_perf_data = internal_data.get("brand_performance_data")  # (Keep existing formatting)
    if isinstance(brand_perf_data, list) and brand_perf_data:  # ...
        data_found = True;
        prompt_parts.append(f"\nInternal Brand Performance Data ({target_brand or 'Requested Brand'}):")
        for country_data in brand_perf_data[:10]:  # ...
            if isinstance(country_data, dict):  # ...
                country = country_data.get('country', 'N/A');
                visits = country_data.get('estimated_monthly_visits', 'N/A');
                growth = country_data.get('estimated_growth_percentage', 'N/A')
                growth_str = f"{growth:.1f}%" if isinstance(growth, (int, float)) else (
                    "N/A" if growth is None else str(growth));
                visits_str = f"{visits:,}" if isinstance(visits, (int, float)) else "N/A"
                prompt_parts.append(f"- {country}: Est. Visits={visits_str}, Est. Growth={growth_str}")
    elif primary_task == "analyze_brand_deep_dive":
        prompt_parts.append("\nInternal Brand Performance Data: - Data not available.")

    # Amazon Data
    amazon_data = internal_data.get("amazon_radar_data")  # (Keep existing formatting)
    if isinstance(amazon_data, dict):  # ...
        products = amazon_data.get("country_department_category", []);
        market_info = amazon_data.get("category_dep_market_size")
        if isinstance(products, list) and products:  # ...
            data_found = True;
            prompt_parts.append("\nTop Amazon Products (by Revenue, Max 10 Provided):")
            for i, p_item in enumerate(products[:10]):  # ...
                if isinstance(p_item, dict):  # ...
                    asin = p_item.get("asin", "N/A");
                    price = p_item.get("product_price", "N/A");
                    currency = p_item.get("currency", "");
                    rating = p_item.get("product_star_rating", "N/A");
                    revenue = p_item.get("estimated_revenue", "N/A");
                    orders = p_item.get("estimated_orders", "N/A");
                    reviews = p_item.get("number_of_reviews", "N/A")
                    price_str = f"{price:,.2f}" if isinstance(price, (int, float)) else "N/A";
                    revenue_str = f"{revenue:,.0f}" if isinstance(revenue, (int, float)) else "N/A";
                    orders_str = f"{orders:,.0f}" if isinstance(orders, (int, float)) else "N/A";
                    reviews_str = f"{reviews:,.0f}" if isinstance(reviews, (int, float)) else "N/A"
                    prompt_parts.append(
                        f"- Item {i + 1} (ASIN: {asin}): Price ~{price_str} {currency}, Rating={rating}/5, Est. Revenue={revenue_str}, Est. Orders={orders_str}, Reviews={reviews_str}")
        elif primary_task == "summarize_amazon_radar":
            prompt_parts.append("\nTop Amazon Products: - Data not available.")
        if isinstance(market_info, dict):  # ...
            data_found = True;
            prompt_parts.append("\nAmazon Category Department Market Size Context:")
            share = market_info.get("department_in_country_share", "N/A");
            prompt_parts.append(f"- Category's Share in Department: {share}% (approx.)")
        elif primary_task == "summarize_amazon_radar":
            prompt_parts.append("\nAmazon Category Department Market Size Context: - Data not available.")
    elif primary_task == "summarize_amazon_radar":
        prompt_parts.append("\nAmazon Radar Data: - Data not available.")

    # Item Details
    details = internal_data.get("chart_details_data")  # (Keep existing formatting)
    if isinstance(details, dict):  # ...
        data_found = True;
        prompt_parts.append(f"\nInternal Specific Item Details ({details.get('category_subject', 'N/A')}):")
        prompt_parts.append(
            f"- Avg Vol={details.get('average_volume', 'N/A')}, Growth={details.get('growth_recent', 'N/A'):.1f}%")
        if primary_task == "get_forecast":  # ...
            prompt_parts.append(f"- Data Point f2 (2m Growth %): {details.get('f2', 'N/A')}");
            prompt_parts.append(f"- Data Point f3 (3m Growth %): {details.get('f3', 'N/A')}");
            prompt_parts.append(f"- Data Point f6 (6m Growth %): {details.get('f6', 'N/A')}")
            prompt_parts.append(f"- Data Point avg2 (2m Avg Volume): {details.get('avg2', 'N/A')}");
            prompt_parts.append(f"- Data Point avg3 (3m Avg Volume): {details.get('avg3', 'N/A')}");
            prompt_parts.append(f"- Data Point avg6 (6m Avg Volume): {details.get('avg6', 'N/A')}")
    elif primary_task == "get_forecast" or primary_task == "get_trend":
        logger.debug("chart_details_data is missing or not a dict.")

    # Mega Trends
    mega = internal_data.get("mega_trends_data")  # (Keep existing formatting)
    if isinstance(mega, list) and mega:  # ...
        data_found = True;
        prompt_parts.append("\nInternal Mega Trends (Top Queries):")
        top_mega = sorted([m for m in mega if isinstance(m, dict)], key=lambda x: x.get('growth_recent', 0),
                          reverse=True)[:5];
        if top_mega:  # ...
            for m in top_mega: prompt_parts.append(
                f"- Query: '{m.get('query_name', 'N/A')}', Related Category: {m.get('category_name', 'N/A')}, Growth: {m.get('growth_recent', 'N/A'):.1f}%")
        else:
            prompt_parts.append("- No specific mega trend query data found.")
    elif primary_task == "summarize_mega_trends":
        logger.debug("mega_trends_data is missing or invalid."); prompt_parts.append(
            "\nInternal Mega Trends (Top Queries): - Data not available.")

    # External Web Context (Check required_sources)
    if "web_search" in interpretation.get("required_sources", []):  # (Keep existing formatting)
        ext_answer = external_data.get("answer");
        ext_results = external_data.get("results", [])
        if not isinstance(ext_results, list): ext_results = []
        if ext_answer or ext_results:  # ...
            data_found = True;
            prompt_parts.append("\nExternal Web Context:")
            if ext_answer: prompt_parts.append(f"- Synthesized Answer: {ext_answer}")
            if ext_results:  # ...
                prompt_parts.append("- Top Results Snippets:")
                for i, res in enumerate(ext_results[:3]):  # ...
                    if isinstance(res, dict): title = res.get('title', 'N/A'); content_snippet = res.get('content', '')[
                                                                                                 :150]; prompt_parts.append(
                        f"  - [{i + 1}] {title}: {content_snippet}...")
        else:
            logger.debug("Web search requested, but no external data found."); prompt_parts.append(
                "\nExternal Web Context: - No relevant web context found or provided.")

    # Final check
    if not data_found:  # (Keep existing logic)
        # ... generate appropriate "No data" message ...
        logger.warning("No significant internal or external data blocks were available to format for prompt.")
        query = original_context.get('query', 'N/A')
        if primary_task == COMPARE_CATEGORIES_TASK_NAME:
            return f"No trend data available for comparison of {category_list_str}."
        elif primary_task == "analyze_brand_deep_dive":
            brand = target_brand or 'the brand'; return f"No specific data or web context available to analyze for '{brand}'."
        elif primary_task == "summarize_amazon_radar":
            cat = original_context.get('target_category', 'N/A'); dept = original_context.get('target_department',
                                                                                              'N/A'); return f"No Amazon product data available for category '{cat}' in department '{dept}'."
        else:
            cat = original_context.get('category', 'N/A'); country = original_context.get('country',
                                                                                          'N/A'); return f"No specific data available for query '{query}' on category '{cat}' in {country}."

    return "\n".join(prompt_parts)


# --- Update build_final_payload_for_bubble ---
# def build_final_payload_for_bubble(
#         ai_summary_structured: Dict[str, Any],
#         internal_data: Dict,
#         external_data: Dict,
#         task_indicator: str,
#         final_status: str,
#         error_message: Optional[str]
# ) -> Dict:
#     logger.debug("Building final payload object with structured summary for Bubble...")
#     payload = {
#         # (...) Initial keys remain the same, initialized empty
#         "ai_summary_structured": ai_summary_structured,
#         "result_type_indicator": task_indicator if final_status != INDICATOR_ERROR else INDICATOR_ERROR,
#         "status": final_status,
#         "error_message": error_message,
#         "category_trend": [], "top_styles": [], "top_colors": [], "item_trend": [],
#         "item_metrics": [], "mega_trends_top": [], "web_links": [], "web_answer": None,
#         "brand_performance_summary": [],
#         "amazon_radar_products": [],
#         "comparison_category_trends": []
#     }
#     internal_data = internal_data or {};
#     external_data = external_data or {}
#
#     # --- MODIFICATION START: Handle both single and comparison data ---
#     trends_single_data = internal_data.get("trends_data")
#     trends_comparison_data = internal_data.get("trends_data_comparison")
#
#     all_top_styles = []
#     all_top_colors = []
#
#     if trends_single_data:  # Handle single category case
#         logger.debug("Processing single trend data...")
#         # Populate payload["category_trend"]
#         cs = trends_single_data.get("category_summary");
#         if isinstance(cs, dict): chart = cs.get("chart_data"); payload["category_trend"] = chart if isinstance(chart,
#                                                                                                                list) and chart else []
#
#         # Extract top styles/colors for the single category
#         category_name_single = trends_single_data.get("category_summary", {}).get("category_name", "Unknown")
#
#         style_details = trends_single_data.get("style_details", [])
#         valid_styles = [s for s in style_details if isinstance(s, dict)]
#         if valid_styles:
#             top_styles_list = sorted(valid_styles, key=lambda x: x.get('average_volume', 0), reverse=True)[:5]
#             # Add category_name for consistency, even though it's single
#             all_top_styles.extend([{"category_name": category_name_single, "name": s.get("style_name"),
#                                     "growth": s.get("growth_recent"), "volume": s.get("average_volume")} for s in
#                                    top_styles_list])
#
#         color_details = trends_single_data.get("color_details", [])
#         valid_colors = [c for c in color_details if isinstance(c, dict)]
#         if valid_colors:
#             top_colors_list = sorted(valid_colors, key=lambda x: x.get('average_volume', 0), reverse=True)[:5]
#             # Add category_name for consistency
#             all_top_colors.extend([{"category_name": category_name_single, "name": c.get("color_name"),
#                                     "growth": c.get("growth_recent"), "volume": c.get("average_volume")} for c in
#                                    top_colors_list])
#
#     elif isinstance(trends_comparison_data, list) and trends_comparison_data:  # Handle comparison case
#         logger.debug("Processing comparison trend data...")
#         comp_trends_list_for_payload = []  # For comparison_category_trends key
#         for item in trends_comparison_data:
#             if isinstance(item, dict):
#                 cat_name = item.get("category_name")
#                 cat_data = item.get("data")
#                 if cat_name and isinstance(cat_data, dict):
#                     # Populate comparison_category_trends (only chart data needed)
#                     cs = cat_data.get("category_summary")
#                     chart_data = cs.get("chart_data") if isinstance(cs, dict) else None
#                     comp_trends_list_for_payload.append({
#                         "category_name": cat_name,
#                         "chart_data": chart_data if isinstance(chart_data, list) else []
#                     })
#
#                     # Extract top styles for this category and add to combined list
#                     style_details = cat_data.get("style_details", [])
#                     valid_styles = [s for s in style_details if isinstance(s, dict)]
#                     if valid_styles:
#                         top_styles_list = sorted(valid_styles, key=lambda x: x.get('average_volume', 0), reverse=True)[
#                                           :3]  # Top 3 per category for comparison
#                         all_top_styles.extend([{"category_name": cat_name, "name": s.get("style_name"),
#                                                 "growth": s.get("growth_recent"), "volume": s.get("average_volume")} for
#                                                s in top_styles_list])
#
#                     # Extract top colors for this category and add to combined list
#                     color_details = cat_data.get("color_details", [])
#                     valid_colors = [c for c in color_details if isinstance(c, dict)]
#                     if valid_colors:
#                         top_colors_list = sorted(valid_colors, key=lambda x: x.get('average_volume', 0), reverse=True)[
#                                           :3]  # Top 3 per category for comparison
#                         all_top_colors.extend([{"category_name": cat_name, "name": c.get("color_name"),
#                                                 "growth": c.get("growth_recent"), "volume": c.get("average_volume")} for
#                                                c in top_colors_list])
#
#         payload["comparison_category_trends"] = comp_trends_list_for_payload
#         if comp_trends_list_for_payload:
#             logger.debug(f"Populated comparison_category_trends with {len(comp_trends_list_for_payload)} items.")
#
#     # Assign the combined lists to the payload keys
#     payload["top_styles"] = all_top_styles
#     payload["top_colors"] = all_top_colors
#     if all_top_styles: logger.debug(
#         f"Populated top_styles with {len(all_top_styles)} items (from single or comparison).")
#     if all_top_colors: logger.debug(
#         f"Populated top_colors with {len(all_top_colors)} items (from single or comparison).")
#     # --- MODIFICATION END ---
#
#     # Populate other data lists (Item, Mega, Brand, Amazon, Web) - Unchanged logic
#     # ... (keep existing logic for chart_details_data, mega_trends_data, brand_performance_data, amazon_radar_products, web_links, web_answer) ...
#
#     logger.info(f"Built final bubble payload object with keys: {list(payload.keys())}")
#     return payload


def build_final_payload_for_bubble(
        ai_summary_structured: Dict[str, Any],
        internal_data: Dict,
        external_data: Dict,
        task_indicator: str,
        final_status: str,
        error_message: Optional[str]
) -> Dict:
    logger.debug("Building final payload object with structured summary for Bubble...")
    payload = {
        "ai_summary_structured": ai_summary_structured,
        "result_type_indicator": task_indicator if final_status != INDICATOR_ERROR else INDICATOR_ERROR,
        "status": final_status,
        "error_message": error_message,
        "category_trend": [],
        "top_styles": [],
        "top_colors": [],
        "item_trend": [],
        "item_metrics": [],
        "mega_trends_top": [],  # Initialize
        "web_links": [],
        "web_answer": None,
        "brand_performance_summary": [],
        "amazon_radar_products": [],
        "comparison_category_trends": []
    }
    internal_data = internal_data or {}
    external_data = external_data or {}

    # --- Populate Trend Data (Single or Comparison) ---
    trends_single_data = internal_data.get("trends_data")
    trends_comparison_data = internal_data.get("trends_data_comparison")
    all_top_styles = []
    all_top_colors = []

    if trends_single_data:  # Handle single category case
        logger.debug("Processing single trend data...")
        cs = trends_single_data.get("category_summary")
        if isinstance(cs, dict):
            chart = cs.get("chart_data")
            payload["category_trend"] = chart if isinstance(chart, list) and chart else []

        category_name_single = trends_single_data.get("category_summary", {}).get("category_name", "Unknown")
        style_details = trends_single_data.get("style_details", [])
        valid_styles = [s for s in style_details if isinstance(s, dict)]
        if valid_styles:
            top_styles_list = sorted(valid_styles, key=lambda x: x.get('average_volume', 0), reverse=True)[:5]
            all_top_styles.extend([{"category_name": category_name_single, "name": s.get("style_name"),
                                    "growth": s.get("growth_recent"), "volume": s.get("average_volume")} for s in
                                   top_styles_list])
        color_details = trends_single_data.get("color_details", [])
        valid_colors = [c for c in color_details if isinstance(c, dict)]
        if valid_colors:
            top_colors_list = sorted(valid_colors, key=lambda x: x.get('average_volume', 0), reverse=True)[:5]
            all_top_colors.extend([{"category_name": category_name_single, "name": c.get("color_name"),
                                    "growth": c.get("growth_recent"), "volume": c.get("average_volume")} for c in
                                   top_colors_list])

    elif isinstance(trends_comparison_data, list) and trends_comparison_data:
        logger.debug("Processing comparison trend data...")
        comp_trends_list_for_payload = []
        for item in trends_comparison_data:
            if isinstance(item, dict):
                cat_name = item.get("category_name")
                cat_data = item.get("data")
                if cat_name and isinstance(cat_data, dict):
                    cs = cat_data.get("category_summary")
                    chart_data = cs.get("chart_data") if isinstance(cs, dict) else None
                    comp_trends_list_for_payload.append({
                        "category_name": cat_name,
                        "chart_data": chart_data if isinstance(chart_data, list) else []
                    })
                    style_details = cat_data.get("style_details", [])
                    valid_styles = [s for s in style_details if isinstance(s, dict)]
                    if valid_styles:
                        top_styles_list = sorted(valid_styles, key=lambda x: x.get('average_volume', 0), reverse=True)[
                                          :3]
                        all_top_styles.extend([{"category_name": cat_name, "name": s.get("style_name"),
                                                "growth": s.get("growth_recent"), "volume": s.get("average_volume")} for
                                               s in top_styles_list])
                    color_details = cat_data.get("color_details", [])
                    valid_colors = [c for c in color_details if isinstance(c, dict)]
                    if valid_colors:
                        top_colors_list = sorted(valid_colors, key=lambda x: x.get('average_volume', 0), reverse=True)[
                                          :3]
                        all_top_colors.extend([{"category_name": cat_name, "name": c.get("color_name"),
                                                "growth": c.get("growth_recent"), "volume": c.get("average_volume")} for
                                               c in top_colors_list])
        payload["comparison_category_trends"] = comp_trends_list_for_payload
        if comp_trends_list_for_payload:
            logger.debug(f"Populated comparison_category_trends with {len(comp_trends_list_for_payload)} items.")

    payload["top_styles"] = all_top_styles
    payload["top_colors"] = all_top_colors
    if all_top_styles: logger.debug(f"Populated top_styles with {len(all_top_styles)} items.")
    if all_top_colors: logger.debug(f"Populated top_colors with {len(all_top_colors)} items.")

    # --- CORRECTED: Populate other data lists (Item, Mega, Brand, Amazon, Web) ---
    chart_details_data = internal_data.get("chart_details_data") if isinstance(internal_data.get("chart_details_data"),
                                                                               dict) else None
    if chart_details_data:
        item_metrics_obj = {
            "name": chart_details_data.get("category_subject"),
            "growth": chart_details_data.get("growth_recent"),
            "volume": chart_details_data.get("average_volume"),
            "forecast_growth": None,
            "forecast_volume": None
        }
        if task_indicator == INDICATOR_FORECAST:  # Ensure INDICATOR_FORECAST is defined
            item_metrics_obj["forecast_growth"] = {"f2": chart_details_data.get("f2"),
                                                   "f3": chart_details_data.get("f3"),
                                                   "f6": chart_details_data.get("f6")}
            item_metrics_obj["forecast_volume"] = {"avg2": chart_details_data.get("avg2"),
                                                   "avg3": chart_details_data.get("avg3"),
                                                   "avg6": chart_details_data.get("avg6")}
        payload["item_metrics"] = [item_metrics_obj]
        logger.debug("Populated item_metrics list.")
        item_chart = chart_details_data.get("chart_data")
        if isinstance(item_chart, list) and item_chart:
            payload["item_trend"] = item_chart
            logger.debug("Populated item_trend list.")

    mega_trends_data_raw = internal_data.get("mega_trends_data")  # This is the list from the Router
    if isinstance(mega_trends_data_raw, list):
        valid_mega_items = [m for m in mega_trends_data_raw if isinstance(m, dict)]
        if valid_mega_items:
            # Sort by growth_recent, which should be a number. Handle potential None values in sorting.
            top_mega_list = sorted(
                valid_mega_items,
                key=lambda x: x.get('growth_recent', float('-inf')) if isinstance(x.get('growth_recent'),
                                                                                  (int, float)) else float('-inf'),
                reverse=True
            )[:10]

            payload["mega_trends_top"] = [
                {"name": m.get("query_name"),  # Key from dev_mega_trends output
                 "growth": m.get("growth_recent"),  # Key from dev_mega_trends output
                 "volume": m.get("average_volume"),  # Key from dev_mega_trends output
                 # "category" field here might be redundant if mega_trends are for the input category.
                 # If dev_mega_trends provides a specific 'category_name' per item, use m.get("category_name")
                 } for m in top_mega_list
            ]
            logger.debug(f"Populated mega_trends_top list with {len(payload['mega_trends_top'])} items.")
        else:
            logger.debug("No valid mega trend items found in mega_trends_data_raw.")
    elif mega_trends_data_raw is not None:  # It exists but isn't a list
        logger.warning(f"mega_trends_data from internal_data was not a list: {type(mega_trends_data_raw)}")

    brand_performance_data = internal_data.get("brand_performance_data")
    if brand_performance_data and isinstance(brand_performance_data, list):
        payload["brand_performance_summary"] = brand_performance_data
        logger.debug(f"Populated brand_performance_summary list with {len(brand_performance_data)} items.")

    amazon_radar_data_from_internal = internal_data.get("amazon_radar_data")
    if task_indicator == INDICATOR_AMAZON_RADAR and isinstance(amazon_radar_data_from_internal,
                                                               dict):  # Ensure INDICATOR_AMAZON_RADAR is defined
        products_list = amazon_radar_data_from_internal.get("country_department_category", [])
        if isinstance(products_list, list):
            cleaned_products = []
            for p_item in products_list[:10]:
                if isinstance(p_item, dict):
                    cleaned_products.append({
                        "asin": p_item.get("asin"), "product_url": p_item.get("product_url"),
                        "product_photo": p_item.get("product_photo"), "product_price": p_item.get("product_price"),
                        "currency": p_item.get("currency"), "estimated_revenue": p_item.get("estimated_revenue"),
                        "estimated_orders": p_item.get("estimated_orders"),
                        "number_of_reviews": p_item.get("number_of_reviews"),
                        "product_star_rating": p_item.get("product_star_rating"), "saturation": p_item.get("saturation")
                    })
            payload["amazon_radar_products"] = cleaned_products
            logger.debug(f"Populated amazon_radar_products list with {len(cleaned_products)} items.")

    external_results = external_data.get("results", []) if isinstance(external_data.get("results", []), list) else []
    external_answer = external_data.get("answer") if isinstance(external_data.get("answer"), str) else None
    if external_results:
        payload["web_links"] = [{"title": r.get("title"), "url": r.get("url")} for r in external_results if
                                isinstance(r, dict) and r.get("title") and r.get("url")][:5]
        if payload["web_links"]: logger.debug("Populated web_links list.")
    if external_answer:
        payload["web_answer"] = external_answer
        logger.debug("Populated web_answer string.")
    # --- END CORRECTION ---

    logger.info(f"Built final bubble payload object with keys: {list(payload.keys())}")
    return payload


# --- Main Lambda Handler (Mostly unchanged, relies on helpers) ---
def lambda_handler(event, context):
    logger.info(f"Received combined event: {json.dumps(event)}")

    # Pre-checks (Unchanged)
    if not GEMINI_SDK_AVAILABLE:  # ... return error payload ...
        error_struct = get_default_summary_structure();
        error_struct["overall_summary"] = "Error: LLM SDK unavailable."
        error_payload = build_final_payload_for_bubble(ai_summary_structured=error_struct, internal_data={},
                                                       external_data={}, task_indicator=INDICATOR_ERROR,
                                                       final_status=INDICATOR_ERROR,
                                                       error_message="LLM SDK unavailable.")
        return {"statusCode": 500, "body": json.dumps(error_payload)}
    if BOTO3_CLIENT_ERROR:  # ... return error payload ...
        error_struct = get_default_summary_structure();
        error_struct["overall_summary"] = f"Error: {BOTO3_CLIENT_ERROR}"
        error_payload = build_final_payload_for_bubble(ai_summary_structured=error_struct, internal_data={},
                                                       external_data={}, task_indicator=INDICATOR_ERROR,
                                                       final_status=INDICATOR_ERROR, error_message=BOTO3_CLIENT_ERROR)
        return {"statusCode": 500, "body": json.dumps(error_payload)}

    # Data extraction (Unchanged, includes getting comparison subjects)
    internal_data = event.get("internal_data", {});
    external_data = event.get("external_data", {})
    interpretation = internal_data.get("interpretation", {}) if isinstance(internal_data, dict) else {}
    original_context = interpretation.get("original_context", {}) if isinstance(interpretation, dict) else {}
    primary_task = interpretation.get("primary_task");
    user_query = original_context.get("query", "the user query")
    query_subjects = interpretation.get("query_subjects", {}) if isinstance(interpretation.get("query_subjects"),
                                                                            dict) else {}
    target_brand = query_subjects.get("target_brand");
    amazon_target_category = original_context.get("target_category", "N/A")
    amazon_target_department = original_context.get("target_department", "N/A")
    comparison_subjects = query_subjects.get("comparison_subjects", [])  # Used for prompt formatting
    category_list_str = ", ".join(
        [subj.get("subject", "N/A") for subj in comparison_subjects if isinstance(subj, dict)])

    # Upstream Error collection (Unchanged)
    upstream_errors = []  # ... collect errors ...
    if isinstance(internal_data, dict) and internal_data.get("errors"): upstream_errors.extend(internal_data["errors"])
    if isinstance(internal_data, dict) and internal_data.get("errorType"): upstream_errors.append(
        {"source": "FetchInternalDataRouter", "error": internal_data.get("errorType"),
         "details": internal_data.get("cause", internal_data.get("errorMessage"))})
    if isinstance(external_data, dict) and external_data.get("error"): upstream_errors.append(
        {"source": "FetchExternalContext", "error": external_data["error"]})
    if isinstance(external_data, dict) and external_data.get("errorType"): upstream_errors.append(
        {"source": "FetchExternalContext", "error": external_data.get("errorType"),
         "details": external_data.get("cause", external_data.get("errorMessage"))})

    result_type_indicator, prompt_template = get_task_details(primary_task)
    logger.info(f"Using result indicator: {result_type_indicator}")

    specific_item_name = "N/A"  # Primarily for non-comparison tasks
    if isinstance(interpretation.get("query_subjects"), dict):
        specific_known = query_subjects.get("specific_known", [])
        if specific_known and isinstance(specific_known[0], dict): specific_item_name = specific_known[0].get("subject",
                                                                                                              "N/A")

    # Format prompt (uses updated format_data_for_prompt and new prompt template)
    formatted_data_context = ""
    try:
        formatted_data_context = format_data_for_prompt(internal_data, external_data)
        prompt_format_args = {
            "specific_item_name": specific_item_name,
            "category_name": original_context.get('category', 'N/A'),  # Less relevant for comparison task
            "country_name": original_context.get('country', 'N/A'),
            "user_query": user_query,
            "brand_domain": target_brand or "N/A",
            "target_category": amazon_target_category,
            "target_department": amazon_target_department,
            "category_list_str": category_list_str  # For comparison prompt
        }
        synthesis_prompt = prompt_template.format(**prompt_format_args)
        synthesis_prompt += "\n\n" + formatted_data_context
        logger.debug(f"Constructed Synthesis Prompt:\n{synthesis_prompt}")
    except KeyError as key_err:  # ... error handling unchanged ...
        logger.error(f"Missing key in prompt template formatting: {key_err}...", exc_info=True)
        error_struct = get_default_summary_structure();
        error_struct["overall_summary"] = f"Error: Could not prepare prompt. Missing key: {key_err}"
        error_payload = build_final_payload_for_bubble(ai_summary_structured=error_struct, internal_data=internal_data,
                                                       external_data=external_data, task_indicator=INDICATOR_ERROR,
                                                       final_status=INDICATOR_ERROR, error_message=str(key_err))
        return {"statusCode": 500, "body": json.dumps(error_payload)}
    except Exception as e:  # ... error handling unchanged ...
        logger.error(f"Error formatting data for synthesis prompt: {e}", exc_info=True)
        error_struct = get_default_summary_structure();
        error_struct["overall_summary"] = f"Error: Could not prepare data for AI synthesis: {e}"
        error_payload = build_final_payload_for_bubble(ai_summary_structured=error_struct, internal_data=internal_data,
                                                       external_data=external_data, task_indicator=INDICATOR_ERROR,
                                                       final_status=INDICATOR_ERROR, error_message=str(e))
        return {"statusCode": 500, "body": json.dumps(error_payload)}

    # Secret Retrieval (Unchanged)
    google_api_key = get_secret_value(SECRET_NAME, "GOOGLE_API_KEY")
    if not google_api_key:  # ... return error payload ...
        error_struct = get_default_summary_structure();
        error_struct["overall_summary"] = "Error: API key configuration error (Google)."
        error_payload = build_final_payload_for_bubble(ai_summary_structured=error_struct, internal_data=internal_data,
                                                       external_data=external_data, task_indicator=INDICATOR_ERROR,
                                                       final_status=INDICATOR_ERROR,
                                                       error_message="API key config error")
        return {"statusCode": 500, "body": json.dumps(error_payload)}

    # LLM Call (Logic unchanged - now requests JSON)
    ai_summary_structured = None;
    llm_error = None
    try:
        if not formatted_data_context or formatted_data_context.startswith("No specific data available"):
            logger.warning("Skipping LLM call as no significant data was formatted for prompt.")
            ai_summary_structured = get_default_summary_structure()
            # Try to make the unknown prompt format args safe
            unknown_format_args = {
                "user_query": user_query,
                "category_name": original_context.get('category', 'N/A'),
                "country_name": original_context.get('country', 'N/A'),
                # Add defaults for any other keys the UNKNOWN prompt might use
                "specific_item_name": "N/A", "brand_domain": "N/A", "target_category": "N/A",
                "target_department": "N/A", "category_list_str": "N/A"
            }
            try:
                # Assume UNKNOWN prompt is simple text for now, place in overall_summary
                ai_summary_structured["overall_summary"] = PERSONA_PROMPTS[INDICATOR_UNKNOWN].format(
                    **unknown_format_args)
            except KeyError as unknown_key_err:
                logger.error(f"Error formatting UNKNOWN prompt: {unknown_key_err}")
                ai_summary_structured[
                    "overall_summary"] = "Could not generate summary because required data was missing and fallback prompt failed."

            result_type_indicator = INDICATOR_UNKNOWN
        else:
            logger.info(f"Calling Synthesis LLM: {SYNTHESIS_LLM_MODEL} for {result_type_indicator}...")
            genai.configure(api_key=google_api_key)
            model = genai.GenerativeModel(SYNTHESIS_LLM_MODEL)
            generation_config = genai_types.GenerationConfig(response_mime_type="application/json")  # Request JSON
            response = model.generate_content(synthesis_prompt, generation_config=generation_config)
            logger.info("Synthesis LLM response received.")
            raw_llm_text = response.text
            logger.debug(f"LLM Raw Response Text:\n{raw_llm_text}")

            try:  # Parse and validate LLM JSON response
                cleaned_text = raw_llm_text.strip();  # ... (cleaning as before) ...
                if cleaned_text.startswith("```json"): cleaned_text = cleaned_text[7:]
                if cleaned_text.endswith("```"): cleaned_text = cleaned_text[:-3]
                cleaned_text = cleaned_text.strip()
                if not cleaned_text: raise ValueError("LLM returned empty JSON string.")
                llm_output_json = json.loads(cleaned_text)
                if validate_structured_summary(llm_output_json):
                    ai_summary_structured = llm_output_json
                    logger.info("Successfully parsed and validated structured summary from LLM.")
                else:
                    logger.error(
                        f"LLM JSON output failed validation. Structure received: {json.dumps(llm_output_json)}")
                    raise ValueError("LLM JSON output failed schema validation.")
            except (json.JSONDecodeError, ValueError) as parse_err:
                logger.error(f"Failed to parse or validate LLM JSON response: {parse_err}", exc_info=True)
                llm_error = f"LLM response parsing/validation error: {parse_err}. Raw text: {raw_llm_text}"
                ai_summary_structured = get_default_summary_structure()  # Fallback
                ai_summary_structured[
                    "overall_summary"] = f"Error: Could not process AI analysis results. Details: {parse_err}"

    except Exception as e:  # Catch errors during LLM call itself
        logger.error(f"Synthesis LLM call failed: {e}", exc_info=True)
        llm_error = f"Synthesis LLM call failed: {str(e)}"
        ai_summary_structured = get_default_summary_structure()  # Fallback
        ai_summary_structured["overall_summary"] = "An error occurred during the analysis synthesis."

    # Build Final Payload (Logic unchanged)
    final_bubble_payload = {}
    try:
        final_status = "success";
        user_error_message = None
        if llm_error:  # ... handle LLM error status ...
            final_status = INDICATOR_ERROR;
            user_error_message = llm_error;
            result_type_indicator = INDICATOR_ERROR
            if not ai_summary_structured or not validate_structured_summary(
                    ai_summary_structured):  # Ensure fallback is valid
                ai_summary_structured = get_default_summary_structure()
                ai_summary_structured[
                    "overall_summary"] = "An error occurred during AI synthesis or response processing."
        elif upstream_errors:  # ... handle partial success ...
            final_status = "partial_data_success";
            logger.warning(f"Upstream errors detected: {upstream_errors}")
            if isinstance(ai_summary_structured, dict) and "overall_summary" in ai_summary_structured:
                intro_prefix = "Note: Analysis may be incomplete due to errors fetching some data. "
                ai_summary_structured["overall_summary"] = intro_prefix + ai_summary_structured.get("overall_summary",
                                                                                                    "")

        # Build the payload using the potentially modified ai_summary_structured
        final_bubble_payload = build_final_payload_for_bubble(
            ai_summary_structured=ai_summary_structured,
            internal_data=internal_data, external_data=external_data,
            task_indicator=result_type_indicator, final_status=final_status, error_message=user_error_message
        )
        logger.info(f"Final payload status: {final_status}")

    except Exception as e:  # ... handle final build error ...
        logger.error(f"Error during final payload building: {e}", exc_info=True)
        error_struct = get_default_summary_structure();
        error_struct["overall_summary"] = f"An critical error occurred preparing the final response: {e}"
        final_bubble_payload = {  # Fallback payload structure
            "ai_summary_structured": error_struct, "result_type_indicator": INDICATOR_ERROR, "status": INDICATOR_ERROR,
            "error_message": f"Payload construction error: {str(e)}",
            "category_trend": [], "top_styles": [], "top_colors": [], "item_trend": [], "item_metrics": [],
            "mega_trends_top": [], "web_links": [], "web_answer": None, "brand_performance_summary": [],
            "amazon_radar_products": [], "comparison_category_trends": []  # Include new key in fallback
        }

    return {"statusCode": 200, "body": json.dumps(final_bubble_payload)}