import ijson
import json
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def split_json(input_filename, chunk_size):
    with open(input_filename, 'r', encoding='utf-8') as infile:
        items = ijson.items(infile, 'item')
        for index, chunk in enumerate(zip(*[iter(items)] * chunk_size)):
            output_filename = f"output_{index}.json"
            with open(output_filename, 'w', encoding='utf-8') as outfile:
                json.dump(list(chunk), outfile, indent=4, ensure_ascii=False, cls=DecimalEncoder)

if __name__ == "__main__":
    input_filename = r"D:\Mongodb files\yelp_dataset_reformatted.json"
    chunk_size = int(9072435/4)  # Convert float division result to an integer
    split_json(input_filename, chunk_size)
