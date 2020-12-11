import json

min_distance = -5000
max_distance = 25000
duration = 1

if 'values' not in locals():
    try:
        # read file
        values = json.loads(open('config.json', 'r').read())
        for key, value in values.items():
            exec(f"{key} = {value}", locals())
    except (FileNotFoundError, json.decoder.JSONDecodeError) as e:
        pass
