import requests

def GetCoordinates(city, country, zip=None, state=None):
    """Takes location data (city, state, zip, country) as input. Sends a GET request to the open street maps/mapquest API and 
    pulls latitude/longitude information out of the Json that's returned."""

    url = 'http://open.mapquestapi.com/nominatim/v1/search.php'
    parameters = {'city': city, 'country': country, 'postalcode': None, 'state': state, 'format': 'json' }
#   if country == 'United States':
#       parameters['postalcode'] = zip
    request = requests.get(url, params=parameters)
    request_json = request.json()
    if len(request_json) > 0:
        return {'latitude': request_json[0]['lat'], 'longitude': request_json[0]['lon']}
    return None

def LocationToCoord(locations):
    if not isinstance(locations, list):
        locations = [locations]
    for location in locations:        
        coordinates = GetCoordinates(location['city'], location['country'], zip=str(location['zip']), state=location['state'])
        if coordinates:
            location['geodata'] = {
                'latitude': coordinates['latitude'],            
                'longitude': coordinates['longitude']
            }
            
    return locations

