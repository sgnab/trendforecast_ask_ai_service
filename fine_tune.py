import os
from openai import OpenAI

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY=")) # Use env var

# train_file = client.files.create(
#   file=open("/Users/ghavamnabavi/Documents/llm_dataset/fashion_llm.jsonl", "rb"),
#   purpose="fine-tune"
# )
# validation_file = client.files.create(
#   file=open("/Users/ghavamnabavi/Documents/llm_dataset/fashion_llm.jsonl", "rb"),
#   purpose="fine-tune"
# )
# print(f"Train File ID: {train_file.id}")
# print(f"Validation File ID: {validation_file.id}")
#
# job = client.fine_tuning.jobs.create(
#   training_file=train_file.id,
#   validation_file=validation_file.id,
#   model="gpt-4o-mini-2024-07-18" # Specify the base 4o mini model identifier
#   # Add optional hyperparameters if needed (epochs, learning_rate_multiplier)
#   # Add optional suffix for your custom model name
# )
# print(f"Fine-tuning Job ID: {job.id}")
#
# # # client.fine_tuning.jobs.list(limit=10)
# job_details=client.fine_tuning.jobs.list(limit=10)
# job_details=client.fine_tuning.jobs.retrieve("ftjob-C4fPwE9G28EANQw11pDdBnQx")
#
#
# print(job_details)

completion = client.chat.completions.create(
    model="ft:gpt-4o-mini-2024-07-18:personal::BRVEMBZL",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "what is mega demands feature?!"}
    ]
)

print(completion.choices[0].message)

