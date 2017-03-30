__author__ = 'tfiez'

import numpy as np
import datetime
import csv
import requests
import grequests
import time
import os
from requests.auth import HTTPBasicAuth


def get_wait(times, name):
    """Get the estimated wait time for a product type at a location.

    In the API calls there is not always a wait time available for the product
    type. The price call will return all the product types though, so this
    function gets the time for the product type passed in and returns an
    empty string if no time estimate was available.

    :param times: The list of product types and their estimated wait times.
    :param name: The product name as a string.
    :return: The wait time if available or an empty string.
    """

    for i in range(len(times)):
        if times[i]['display_name'] == name:
            return times[i]['eta_seconds']
    return ''


def exception_handler(request, exception):
    """Print if a request failed and prevents the program from breaking.

    :param request: request that created the exception which is passed in
    by the causing call.
    :param exception: The exception that caused the function to be called.
    :return: This function does not return anything.
    """

    pass


def gather_loop(price_api_params, time_api_params, client_id, client_secret, path):
    """Called every 5 minutes to gather the surge multiplier and wait times.

    This function is triggered every ~5 minutes to create API calls that will
    give the surge multiplier and the
    wait time of all uber product types at the locations that we have specified.

    :param price_api_params: The list of price API paramaters to create a call
    for each location we have.
    :param time_api_params: The list of time API paramaters to create a call
    for each location we have.
    :param uber_server_tokens: The list of uber server tokens that we will use
    in the API calls.
    :return: This function does not return any values but will write to a file
    the information from API calls.
    """

    token_num = 0  # Counter for the token number the function is at.
    price_reqs = list() # List to hold the requests for price API calls.
    time_reqs = list() # List to hold the requests for timer API calls.

    # Create a session. This is done because in the function there are 2 API
    # calls made for each location and there are 578 locations. Without this
    # only 1024 sockets can be open, so some of the API calls would fail since
    # it would exceed the limit. Creating the session gets around this problem.
    session = requests.session()

    auth_token_response = requests.post('https://api.lyft.com/oauth/token',
                                        data={"grant_type": "client_credentials",
                                              "scope": "public"},
                                        auth=HTTPBasicAuth(client_id[0],
                                                           client_secret[0]))

    try:
        server_token = auth_token_response.json()['access_token']
    except:
    return 

    # Create the requests for price and time API calls for each of the locations.
    for i in range(len(price_api_params)):
        # Create the asynchronous requests for this location.
        price_response = grequests.get(price_api_params[i]['url'],
                                       params=price_api_params[i]['parameters'],
                                       headers = {'Authorization': 'Bearer '
                                       + server_token}, session=session)
        time_response = grequests.get(time_api_params[i]['url'],
                                      params=time_api_params[i]['parameters'],
                                      headers = {'Authorization': 'Bearer '
                                      + server_token}, session=session)

        # Add the requests to the lists of requests.
        price_reqs.append(price_response)
        time_reqs.append(time_response)

    # Price and Time lists to hold the data in the from the API responses.
    price_data = list()
    time_data = list()

    # This call allows for all the requests for price to be made at once.
    price_results = grequests.map(price_reqs, exception_handler=exception_handler)
    for result in price_results:
        try:
            price_data.append(result.json()) # Break down the json response.
        except:
            price_data.append(None)

    # This call allows for all the requests for time to be made at once.
    time_results = grequests.map(time_reqs, exception_handler=exception_handler)
    for result in time_results:
        try:
            time_data.append(result.json()) # Break down the json response.
        except:
            time_data.append(None)

    # Write the data to the file where the data will be stored. This outer
    # loop will loop through the results for each location.
    for i in range(len(price_data)):
        # Check that the response is good.
        if time_data[i] is None or price_data[i] is None:
            pass
        else:
            # This will loop through the product types for the location.
            try:
                for j in range(len(price_data[i]['cost_estimates'])):
                        # Get the wait time for the product type.
                        wait_time = get_wait(time_data[i]['eta_estimates'],
                                             str(price_data[i]['cost_estimates'][j]['display_name']))
                        with open(path, 'ab') as f:
                            fileWriter = csv.writer(f, delimiter=',')
                            fileWriter.writerow([str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                                                 price_data[i]['cost_estimates'][j]['primetime_percentage'],
                                                 wait_time,
                                                 price_data[i]['cost_estimates'][j]['estimated_duration_seconds'],
                                                 price_data[i]['cost_estimates'][j]['estimated_distance_miles'],
                                                 price_data[i]['cost_estimates'][j]['estimated_cost_cents_min'],
                                                 price_data[i]['cost_estimates'][j]['estimated_cost_cents_max'],
                                                 str(price_data[i]['cost_estimates'][j]['display_name']),
                                                 price_api_params[i]['location_id'],
                                                 price_api_params[i]['parameters']['start_lat'],
                                                 price_api_params[i]['parameters']['start_lng'],
                                                 price_api_params[i]['parameters']['end_lat'],
                                                 price_api_params[i]['parameters']['end_lng']])
            except:
                pass


def main():
    # Read in all the geographical information of the locations.
    location_file = np.genfromtxt('locations.csv', delimiter=',')

    # This list will contain dictionaries with each dictionary containing
    # the location id, latitude, and longitude for each location in locations.csv.
    locations = list()

    # Fill the list with the dictionaries.
    for i in range(len(location_file)):
        location_dict = dict()
        location_dict['location_id'] = i  
        location_dict['latitude1'] = location_file[i][0]  # Get the latitude.
        location_dict['longitude1'] = location_file[i][1]  # Get the longitude.
        location_dict['latitude2'] = location_file[i][2]  # Get the latitude.
        location_dict['longitude2'] = location_file[i][3]  # Get the longitude.
        locations.append(location_dict)  # Add to the dictionary.

    # API access information. Add client id and client secret here.
    client_id = []
    client_secret = []

    # The API end points for lyft that are used to make calls.
    price_url = 'https://api.lyft.com/v1/cost'
    time_url = 'https://api.lyft.com/v1/eta'

    # These api param objects are used to send requests to the API,
    # we create api_param objects for each location.
    price_api_params = []
    time_api_params = []

    # Create an api param object for each location so a
    # call to the api can be made for each one.
    for l in locations:
        location_id = l['location_id']
        price_parameters = {
            'start_lat': l['latitude1'],
            'end_lat': l['latitude2'],
            'start_lng': l['longitude1'],
            'end_lng': l['longitude2']
        }

        time_parameters = {
            'lat': l['latitude1'],
            'lng': l['longitude1'],
        }

        price_api_params.append({'url': price_url, 'location_id': location_id,
                                 'type': 'price', 'parameters': price_parameters})
        time_api_params.append({'url': time_url, 'location_id': location_id,
                                'type': 'time', 'parameters': time_parameters})

    curr_day = datetime.datetime.today().day

    path = os.getcwd() + '/data'
    output_file_name = str(time.strftime("%m_%d_%Y")) + '.csv'
    if not os.path.isfile(os.path.join(path, output_file_name)):
        with open(os.path.join(path, output_file_name), 'wb') as f:
            fileWriter = csv.writer(f, delimiter=',')
            fileWriter.writerow(['timestamp', 'primetime_percentage', 'expected_wait_time',
                                 'duration', 'distance', 'low_estimate', 'high_estimate',
                                 'product_type', 'start_geoid', 'start_latitude',
                                 'start_longitude', 'end_latitude', 'end_longitude'])

    # Call the function for the script to run continuously.
    while 1:
        gather_loop(price_api_params, time_api_params, client_id, client_secret, os.path.join(path, output_file_name))
        time.sleep(250)

        # Get day of the month as an integer.
        new_day = datetime.datetime.today().day

        # If the day has changed, close the previous file and open a new file.
        if new_day != curr_day:
            curr_day = new_day  
            # Create new name by the date for the file.
            output_file_name = str(time.strftime("%m_%d_%Y")) + '.csv'
            with open(os.path.join(path, output_file_name), 'wb') as f:
                fileWriter = csv.writer(f, delimiter=',')
                fileWriter.writerow(['timestamp', 'primetime_percentage', 'expected_wait_time',
                                     'duration', 'distance', 'low_estimate', 'high_estimate',
                                     'product_type', 'start_geoid', 'start_latitude',
                                     'start_longitude', 'end_latitude', 'end_longitude'])


if __name__ == '__main__':
    main()
    