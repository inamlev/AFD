import json
from datetime import datetime

# Load the JSON file
input_file = r"D:\Mongodb files\yelp_dataset.json"  # Note the 'r' prefix for raw string
output_file = r"D:\Mongodb files\yelp_dataset_reformatted.json"

with open(input_file, "r", encoding="utf-8") as f:  # Specify encoding as utf-8
    data = json.load(f)

# Function to reformat date field to ISO 8601 format
def reformat_date(date_str):
    original_format = "%your_original_date_format%"
    new_format = "%Y-%m-%dT%H:%M:%SZ"  # ISO 8601 format
    date_obj = datetime.strptime(date_str, original_format)
    return date_obj.strftime(new_format)

# Iterate through the data and reformat date fields
for document in data:
    if "your_date_field" in document:
        document["your_date_field"] = reformat_date(document["your_date_field"])

# Save the reformatted data to a new file
with open(output_file, "w", encoding="utf-8") as f:  # Specify encoding as utf-8
    json.dump(data, f, indent=2)
