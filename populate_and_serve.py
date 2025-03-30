from flask import Flask, jsonify
from threading import Thread
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import random, time, datetime

# CONFIGURA QUI
url = "https://us-east-1-1.aws.cloud2.influxdata.com"
token = "1lI4Tppp5IUHTEQuFw5dUmI3ljTJflRChcmcKdUOEH89JSIRcZoT6tXUE7IYo6U46JBmfNm8I13MxzKVKwwv1Q=="  # <-- copia dal tuo InfluxDB
org = "EsternTeam"      # <-- nome organizzazione
bucket = "TestSensori20250330"

client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()


# ========== SERVER FLASK ========== #
app = Flask(__name__)

@app.route("/api/latest")
def get_latest_data():
    query = f'''
        from(bucket: "{bucket}")
          |> range(start: -5m)
          |> filter(fn: (r) => r._measurement == "sensori")
          |> filter(fn: (r) => r._field == "temperatura" or r._field == "energia")
          |> last()
    '''
    tables = query_api.query(query=query, org=org)

    results = []
    for table in tables:
        for record in table.records:
            results.append({
                "field": record.get_field(),
                "value": record.get_value(),
                "time": record.get_time().isoformat()
            })

    return jsonify(results)

# ========== THREAD DI SCRITTURA DATI ========== #
def write_loop():
    while True:
        temperatura = round(random.uniform(18.0, 30.0), 2)
        energia = random.randint(100, 200)

        p = Point("sensori") \
            .tag("device", "sensor_01") \
            .field("temperatura", temperatura) \
            .field("energia", energia) \
            .time(datetime.datetime.utcnow(), WritePrecision.NS)

        write_api.write(bucket=bucket, org=org, record=p)
        print(f"[DATA] Temp={temperatura} | Energia={energia}")
        time.sleep(5)

# ========== AVVIO ========== #
if __name__ == "__main__":
    # Avvia il loop scrittura in un thread separato
    Thread(target=write_loop, daemon=True).start()

    # Avvia il server Flask
    app.run(debug=True, port=5000)