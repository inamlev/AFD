import json
from datetime import datetime

input_file = "D:\\Mongodb files\\yelp_dataset.json"
output_file = "D:\\Mongodb files\\yelp_dataset_reformatted.json"

def reformat_date(date_str):
    original_format = "%your_original_date_format%"
    new_format = "%Y-%m-%dT%H:%M:%SZ"  # ISO 8601 format
    date_obj = datetime.strptime(date_str, original_format)
    return date_obj.strftime(new_format)

with open(input_file, "r", encoding="ISO-8859-1") as f:
    lines = f.readlines()

data = []
for line in lines:
    try:
        parsed_line = json.loads(line)
        data.append(parsed_line)
    except json.JSONDecodeError:
        print("Invalid JSON on line:", line)


reformatted_data = []

for document in data:
    if "your_date_field" in document:
        document["your_date_field"] = reformat_date(document["your_date_field"])
    reformatted_data.append(document)

with open(output_file, "w", encoding="utf-8") as f:
    json.dump(reformatted_data, f, indent=2)

print("Date format changed and saved to", output_file)
