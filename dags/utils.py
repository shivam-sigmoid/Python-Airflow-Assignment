import os
import requests
import pandas as pd


def get_weather_api_method():
    url = "https://community-open-weather-map.p.rapidapi.com/weather"
    # 10 States
    state_list = ['bihar', 'jharkhand', 'goa', 'uttar pradesh', 'karnataka', 'punjab', 'haryana', 'gujarat', 'kerala',
                  'assam']
    df = pd.DataFrame(columns=["State", "Description", "Temperature", "Feels_Like_Temperature", "Min_Temperature", "Max_Temperature",
                 "Humidity", "Clouds"])
    for state in state_list:
        querystring = {"q": state}
    # API Host and Key
    headers = {
        'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com",
        'x-rapidapi-key': "ecf7830d16msh64cf3478497ecdcp15bd97jsn0db09825b716"
    }

    response = requests.get(url, headers=headers, params=querystring)
    info = response.json()
    # time.sleep(10)
    try:
        df = df.append({'State': info['name'], "Description": info['weather'][0]['description'],
                        'Temperature': info['main']['temp'], "Feels_Like_Temperature": info['main']['feels_like'],
                        "Min_Temperature": info['main']['temp_min'], "Max_Temperature": info['main']['temp_max'],
                        "Humidity": info['main']['humidity'], "Clouds": info['clouds']['all']}, ignore_index=True)
    except:
        print("API Request limit exceeds")

    path = "/usr/local/airflow/store_files_airflow"
    if not os.path.isfile(os.path.join(path, '/weather_data.csv')):
        df.to_csv(path + '/weather_data.csv', index=False)
    else:
        os.remove(os.path.join(path, '/weather_data.csv'))
        df.to_csv(os.path.join(path, '/weather_data.csv'), index=False)
    print(df.head())
