import json

min_distance = -5000
max_distance = 25000
duration = 1

if 'config_file_read' not in locals().keys():
    config_file_read = True
    try:
        # read file
        values = json.loads(open('config.json', 'r').read())
        for key, value in values.items():
            exec(f"{key} = {value}", locals())
            print(f"{key} = {value}")
    except (FileNotFoundError, json.decoder.JSONDecodeError) as e:
        pass
