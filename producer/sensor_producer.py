import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer

# Configuration API OpenWeatherMap
API_KEY = "a2b4739a06db3f1c5fef13b14531e2c0"
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

# Liste de villes à surveiller
CITIES = [
    {"name": "Casablanca", "country": "MA"},
    {"name": "Rabat", "country": "MA"},
    {"name": "Marrakech", "country": "MA"},
    {"name": "Fes", "country": "MA"},
    {"name": "Tangier", "country": "MA"}
]

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_weather_data(city_name, country_code):
    """
    Récupère les données météo en temps réel depuis OpenWeatherMap
    """
    try:
        params = {
            'q': f"{city_name},{country_code}",
            'appid': API_KEY,
            'units': 'metric',
            'lang': 'fr'
        }
        
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        weather_data = {
            'city': city_name,
            'country': country_code,
            'timestamp': datetime.now().isoformat(),
            'temperature': round(data['main']['temp'], 2),
            'feels_like': round(data['main']['feels_like'], 2),
            'temp_min': round(data['main']['temp_min'], 2),
            'temp_max': round(data['main']['temp_max'], 2),
            'pressure': data['main']['pressure'],
            'humidity': data['main']['humidity'],
            'weather_main': data['weather'][0]['main'],
            'weather_description': data['weather'][0]['description'],
            'clouds': data['clouds']['all'],
            'wind_speed': data['wind']['speed'],
            'wind_deg': data['wind'].get('deg', 0),
            'visibility': data.get('visibility', 0),
            'sunrise': datetime.fromtimestamp(data['sys']['sunrise']).isoformat(),
            'sunset': datetime.fromtimestamp(data['sys']['sunset']).isoformat(),
            'latitude': data['coord']['lat'],
            'longitude': data['coord']['lon']
        }
        
        if 'rain' in data:
            weather_data['rain_1h'] = data['rain'].get('1h', 0)
            weather_data['rain_3h'] = data['rain'].get('3h', 0)
        else:
            weather_data['rain_1h'] = 0
            weather_data['rain_3h'] = 0
        
        if 'snow' in data:
            weather_data['snow_1h'] = data['snow'].get('1h', 0)
            weather_data['snow_3h'] = data['snow'].get('3h', 0)
        else:
            weather_data['snow_1h'] = 0
            weather_data['snow_3h'] = 0
        
        return weather_data, None
        
    except requests.exceptions.RequestException as e:
        return None, f"Erreur API: {e}"
    except KeyError as e:
        return None, f"Erreur données: {e}"
    except Exception as e:
        return None, f"Erreur: {e}"


def main():
    print("=" * 70)
    print("🌤️  PRODUCTEUR DE DONNÉES MÉTÉO - OpenWeatherMap")
    print("=" * 70)
    print(f"📡 Connexion à Kafka sur kafka:29092")
    print(f"📊 Topic : weather-data")
    print(f"🌍 Villes : {', '.join([c['name'] for c in CITIES])}")
    print(f"⏱️  Intervalle : 60 secondes")
    print("=" * 70)
    print()
    
    counter = 0
    
    try:
        while True:
            print(f"\n{'='*70}")
            print(f"🔄 Cycle #{counter + 1} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*70}\n")
            
            for city in CITIES:
                weather_data, error = fetch_weather_data(city['name'], city['country'])
                
                if error:
                    print(f"❌ {city['name']}: {error}")
                    continue
                
                producer.send('weather-data', value=weather_data)
                counter += 1
                
                print(f"✅ Message #{counter} envoyé - {weather_data['city']}")
                print(f"   🌡️  Temp: {weather_data['temperature']}°C "
                      f"(Ressenti: {weather_data['feels_like']}°C)")
                print(f"   💧 Humidité: {weather_data['humidity']}% | "
                      f"☁️  Nuages: {weather_data['clouds']}%")
                print(f"   🌬️  Vent: {weather_data['wind_speed']} m/s")
                print(f"   🌤️  Météo: {weather_data['weather_description']}")
                print(f"   {'-'*68}")
            
            print(f"\n💤 Attente de 60 secondes...")
            print(f"📊 Total messages : {counter}")
            time.sleep(60)
            
    except KeyboardInterrupt:
        print("\n⚠️  Arrêt demandé")
        producer.flush()
        producer.close()
        print(f"✅ Arrêté après {counter} messages")
    except Exception as e:
        print(f"\n❌ Erreur : {e}")
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()