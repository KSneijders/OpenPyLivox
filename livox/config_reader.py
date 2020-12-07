import json

min_distance = float('-inf')
max_distance = float('inf')

if 'values' not in locals():
    try:
        # read file
        values = json.loads(open('config.json', 'r').read())
        for key, value in values.items():
            exec(f"{key} = {value}", locals())
    except (FileNotFoundError, json.decoder.JSONDecodeError) as e:
        pass
