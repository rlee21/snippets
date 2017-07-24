import json
import requests
from pprint import pprint


# TODO: use argparse
# TODO: get 5-day forecast
def get_weather(zip_code):
    mode = 'json'
    units = 'imperial'
    country_code = 'us'
    with open('secret.json', 'r') as f:
        secret = json.load(f)
        api_key = secret['api_key']

    url = 'http://api.openweathermap.org/data/2.5/weather?zip='+zip_code+','+country_code+'&units='+units+'&mode='+mode+ \
          '&appid='+api_key+''
    get = requests.get(url)
    response = get.json()
    #pprint(response)
    city = response['name']
    temp = response['main']['temp']
    desc = response['weather'][0]['main']

    return "\n\n\n\n\n The current weather in {0} is {1} °F {2} \n\n\n\n\n".format(city, temp, desc)


if __name__ == '__main__':
    zip_code = input('Enter Zip Code: ')
    #zip_code = '98106'
    print(get_weather(zip_code))
