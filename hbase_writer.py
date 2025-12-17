# hbase_writer.py
import sys
import json
import requests

for line in sys.stdin:
    try:
        data = json.loads(line.strip())
        row_key = f"{data['city']}_{data['timestamp']}".replace(':', '_')
        url = f"http://hbase:9095/weather_data/{row_key}/cf"
        
        # Envoyer via REST
        response = requests.put(
            url, 
            data=json.dumps(data),
            headers={'Content-Type': 'application/json'},
            timeout=5
        )
        
        if response.status_code in [200, 201]:
            print(f"OK: {data['city']}")
        else:
            print(f"ERROR {response.status_code}: {response.text}", file=sys.stderr)
            
    except Exception as e:
        print(f"EXCEPTION: {e}", file=sys.stderr)