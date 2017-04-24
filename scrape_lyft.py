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

    :param times: The list of product types and their estimated wait times.
    :param name: The product name as a string.
    :return: The wait time if available or an empty string.
    """

    for i in range(len(times)):
        if times[i]['display_name'] == name:
            return times[i]['eta_seconds']
    return ''


def gather_loop(price_api_params, time_api_params, client_id, client_secret, path):
    """Called to gather the surge multiplier and wait times.

    :param price_api_params: The list of price API parameters to create a call
    for each location we have.
    :param time_api_params: The list of time API parameters to create a call
    for each location we have.
    :param client_id: Lyft API key.
    :param client_secret: Lyft API key.
    :param uber_server_tokens: The list of  server tokens that we will use
    in the API calls.
    :param path: file path to write data to.
    :return: This function does not return any values but will write to a file
    the information from API calls.
    """

    token_num = 0  

    price_reqs = list() 

    time_reqs = list() 

    num_sessions = 10
    sessions = [requests.session() for i in range(num_sessions)]

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
                                       + server_token}, session=sessions[i % num_sessions])

        time_response = grequests.get(time_api_params[i]['url'],
                                      params=time_api_params[i]['parameters'],
                                      headers = {'Authorization': 'Bearer '
                                      + server_token}, session=sessions[i % num_sessions])

        price_reqs.append(price_response)
        time_reqs.append(time_response)

    # To hold price and time data from the API responses.
    price_data = list()
    time_data = list()

    # This call makes the price requests asynchronously.
    price_results = grequests.map(price_reqs, size=num_sessions*5, exception_handler=lambda x, y: None)

    for result in price_results:
        try:
            price_data.append(result.json()) 
        except:
            price_data.append(None)

    # This call makes the time requests asynchronously.
    time_results = grequests.map(time_reqs, size=num_sessions*5, exception_handler=lambda x, y: None)

    for result in time_results:
        try:
            time_data.append(result.json()) 
        except:
            time_data.append(None)


    # Looping through each location and writing the results.
    for i in range(len(price_data)):
        if time_data[i] is None or price_data[i] is None:
            pass
        else:

            # This will loop through the product types for the location.
            try:
                for j in range(len(price_data[i]['cost_estimates'])):

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
    location_file = np.genfromtxt('locations.csv', delimiter=',')

    locations = list()

    for i in range(len(location_file)):
        location_dict = dict()
        location_dict['location_id'] = location_file[i][0] 
        location_dict['latitude1'] = location_file[i][1]  
        location_dict['longitude1'] = location_file[i][2]  
        location_dict['latitude2'] = location_file[i][3]  
        location_dict['longitude2'] = location_file[i][4]  
        locations.append(location_dict)  

    # API information for key.
    client_id = []
    client_secret = []

    price_url = 'https://api.lyft.com/v1/cost'
    time_url = 'https://api.lyft.com/v1/eta'

    price_api_params = []
    time_api_params = []

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

    path = os.getcwd() + '/lyft_data'

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
        
        time.sleep(180)

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
    
