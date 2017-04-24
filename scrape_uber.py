__author__ = 'tfiez'

import numpy as np
import datetime
import csv
import requests
import grequests
import time
import os

    
def get_wait(times, name):
    """Get the estimated wait time for a product type at a location.

    :param times: The list of product types and their estimated wait times.
    :param name: The product name as a string.
    :return: The wait time if available or an empty string.
    """

    for i in range(len(times)):
        if times[i]['display_name'] == name:
            return times[i]['estimate']
    return ''


def gather_loop(price_api_params, time_api_params, uber_server_tokens, path):
    """Called to gather the surge multiplier, wait time, and other api information.

    :param price_api_params: The list of price API parameters to create a call
    for each location we have.
    :param time_api_params: The list of time API parameters to create a call for
    each location we have.
    :param uber_server_tokens: The list of uber server tokens that we will use
    in the API calls.
    :param path: file path to write data to.
    :return: This function does not return any values but will write to a file
    the information from API calls.
    """

    token_num = 0  

    price_reqs = list() 

    time_reqs = list() 

    session = requests.session()

    # Create the requests for price and time API calls for each location.
    for i in range(len(price_api_params)):

        # Get the server token for the request for this location.
        price_api_params[i]['parameters']['server_token'] = uber_server_tokens[token_num]
        time_api_params[i]['parameters']['server_token'] = uber_server_tokens[token_num]

        # Create the asynchronous requests for this location.
        price_response = grequests.get(price_api_params[i]['url'],
                                       params=price_api_params[i]['parameters'],
                                       session=session)
        time_response = grequests.get(time_api_params[i]['url'],
                                      params=time_api_params[i]['parameters'],
                                      session=session)

        price_reqs.append(price_response)
        time_reqs.append(time_response)

        """
        Increment the token number. The token number will let us change what
        token we are using to make the API calls. This is needed if we are making
        requests for many locations or very frequently because uber limits 
        the requests from a token to 2000 per hour.
        """
        token_num += 1
        if token_num == len(uber_server_tokens):
            token_num = 0

    # To hold price and time data from the API responses.
    price_data = list()
    time_data = list()

    # This call makes the price requests asynchronously.
    price_results = grequests.map(price_reqs, exception_handler=lambda x, y: None)
    
    for result in price_results:
        try:
            price_data.append(result.json()) 
        except:
            price_data.append(None)

    # This call makes the time requests asynchronously.
    time_results = grequests.map(time_reqs, exception_handler=lambda x, y: None)

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
                for j in range(len(price_data[i]['prices'])):
                    if price_data[i] == None:
                        pass
                    else:

                        wait_time = get_wait(time_data[i]['times'],
                                             str(price_data[i]['prices'][j]['display_name']))

                        with open(path, 'ab') as f:
                            fileWriter = csv.writer(f, delimiter=',')
                            fileWriter.writerow([str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                                                 price_data[i]['prices'][j]['surge_multiplier'],
                                                  wait_time,
                                                 price_data[i]['prices'][j]['duration'],
                                                 price_data[i]['prices'][j]['distance'],
                                                 price_data[i]['prices'][j]['estimate'],
                                                 price_data[i]['prices'][j]['low_estimate'],
                                                 price_data[i]['prices'][j]['high_estimate'],
                                                 str(price_data[i]['prices'][j]['display_name']),
                                                 price_api_params[i]['location_id'],
                                                 price_api_params[i]['parameters']['start_latitude'],
                                                 price_api_params[i]['parameters']['start_longitude'],
                                                 price_api_params[i]['parameters']['end_latitude'],
                                                 price_api_params[i]['parameters']['end_longitude']])
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

    # The tokens allow for calls to the uber API. 
    uber_server_tokens = ['QhNzzz2EsfS5K7YbbPf3du3APJlLx7gj_V7_KSzd',
             		      '5-XSjcsQ8VSrh1NDc7Q_4F7Giq0mXe0CdHsYViRt',
                          'XNSBZf0XlGXL8Vv0i_nwF9S6gU8mvMI8m_BdD1q1',
                          'rWuvx5J_v48zbUPCH1aS4qsU08P6QaxcLSQXywZq',
                          'ekYMqzXFV1vp68kev4swZVnshBs9nvSzBT62eWc8',
                          'eceF62G35rzOaG6xDxmy97ZyaS5G4J6lC1pa6ruR',
		                  'u8SO6N0DIbQnasMTo_e5F9bQHzsOtk0ILDriqwP1',
                          'MNybuzgYo_SzIxf8rL7qeNE8Z_GdAihaW0-sZjgs',
                          'MwRdAfu7wl34oJVJo5eJWcOAoCSqMfbBcAoCntid',
                          '8xtbiB-GJd2zxLgpRRLMe6ZTi9h85Oy-xacRm8Rn',
                          '-kNmZfr0M8RmkiM-Oiws6lCnT_aRrEZ61uc3bfRP',
                          'dxDcib8FerhNEvG_yHXsQsCuVx24h7rJNF0Zt3jV',
                          'hVAY0MGSgwyIKDMAOTjvMUloSM7oxo-MQJKXW7Nt']


    price_url = 'https://api.uber.com/v1/estimates/price'
    time_url = 'https://api.uber.com/v1/estimates/time'

    price_api_params = []
    time_api_params = []

    for l in locations:
        location_id = l['location_id']
        price_parameters = {
            'start_latitude': l['latitude1'],
            'end_latitude': l['latitude2'],
            'start_longitude': l['longitude1'],
            'end_longitude': l['longitude2']
        }

        time_parameters = {
            'start_latitude': l['latitude1'],
            'start_longitude': l['longitude1'],
        }

        price_api_params.append({'url': price_url, 'location_id': location_id,
                                 'type': 'price', 'parameters': price_parameters})
        time_api_params.append({'url': time_url, 'location_id': location_id,
                                'type': 'time', 'parameters': time_parameters})

    curr_day = datetime.datetime.today().day

    path = os.getcwd() + '/uber_data'

    output_file_name = str(time.strftime("%m_%d_%Y")) + '.csv'

    if not os.path.isfile(os.path.join(path, output_file_name)):
        with open(os.path.join(path, output_file_name), 'wb') as f:
            fileWriter = csv.writer(f, delimiter=',')
            fileWriter.writerow(['timestamp', 'surge_multiplier', 'expected_wait_time', 
                                 'duration', 'distance', 'estimate', 'low_estimate', 
                                 'high_estimate', 'product_type', 'start_geoid', 
                                 'start_latitude', 'start_longitude', 'end_latitude', 
                                 'end_longitude'])

    # Call the function for the script to run continuously.
    while 1:
        gather_loop(price_api_params, time_api_params, uber_server_tokens, os.path.join(path, output_file_name))
        
        time.sleep(180)
        new_day = datetime.datetime.today().day

        # If the day has changed, close the previous file and open a new file.
        if new_day != curr_day:
            curr_day = new_day  
            
        # Create new name by the date for the file.
        output_file_name = str(time.strftime("%m_%d_%Y")) + '.csv'
        
        with open(os.path.join(path, output_file_name), 'wb') as f:
            fileWriter = csv.writer(f, delimiter=',')
            fileWriter.writerow(['timestamp', 'surge_multiplier', 'expected_wait_time', 
                                 'duration', 'distance', 'estimate', 'low_estimate', 
                                 'high_estimate', 'product_type', 'start_geoid', 
                                 'start_latitude', 'start_longitude', 'end_latitude', 
                                 'end_longitude'])


if __name__ == '__main__':
    main()
