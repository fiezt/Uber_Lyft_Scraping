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

    In the API calls there is not always a wait time available for the
    product type. The price call will return all the product types though, so
    this function gets the time for the product type passed in and returns an
    empty string if no time estimate was available.

    :param times: The list of product types and their estimated wait times.
    :param name: The product name as a string.
    :return: The wait time if available or an empty string.
    """

    for i in range(len(times)):
        if times[i]['display_name'] == name:
            return times[i]['estimate']
    return ''


def exception_handler(request, exception):
    """Print if a request failed and prevents the program from breaking.

    :param request: request that created the exception which is passed in by
    the causing call.
    :param exception: The exception that caused the function to be called.
    :return: This function does not return anything.
    """

    pass

def gather_loop(price_api_params, time_api_params, uber_server_tokens, path):
    """Called to gather the surge multiplier, wait time, and other api information.

    This function is triggered to create API calls that will
    give the surge multiplier and the wait time of all uber product types at
    the locations that is have specified.

    :param price_api_params: The list of price API paramaters to create a call
    for each location we have.
    :param time_api_params: The list of time API paramaters to create a call for
    each location we have.
    :param uber_server_tokens: The list of uber server tokens that we will use
    in the API calls.
    :return: This function does not return any values but will write to a file
    the information from API calls.
    """

    token_num = 0  # Counter for the token number the function is at.
    price_reqs = list() # List to hold the requests for price API calls.
    time_reqs = list() # List to hold the requests for timer API calls.

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

        # Add the requests to the lists of requests.
        price_reqs.append(price_response)
        time_reqs.append(time_response)

        # Increment the token number. The token number will let us change what
        # token we are using to make the API calls. This is needed if we are making
		# requests for many locations or very frequently because uber limits 
        # the requests from a token to 2000 per hour.
        token_num += 1
        if token_num == len(uber_server_tokens):
            token_num = 0

    # Price and time lists to hold the data in the from the API responses.
    price_data = list()
    time_data = list()

    # This call allows for all the requests for price to be made at once.
    price_results = grequests.map(price_reqs,
                                  exception_handler=exception_handler)
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

    # Write the data to the file where the data will be stored. This outer loop
    # will loop through the results for each location.
    for i in range(len(price_data)):
        # Check that the response is good.
        if time_data[i] is None or price_data[i] is None:
            pass
        else:
            # This will loop through the product types for the location.
            try:
                for j in range(len(price_data[i]['prices'])):
                    if price_data[i] == None:
                        pass
                    else:
                        # Get the wait time for the product type.
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
    # Read in all the geographical information of the locations we will get the
    # price from.
    location_file = np.genfromtxt('locations.csv', delimiter=',')

    # This list will contain dictionaries with each dictionary containing the location
    # id, latitude, and longitude for each location in locations.csv.
    locations = list()

    for i in range(len(location_file)):
        location_dict = dict()
        location_dict['location_id'] = i  
        location_dict['latitude1'] = location_file[i][0]  # Get the start latitude.
        location_dict['longitude1'] = location_file[i][1]  # Get the start longitude.
        location_dict['latitude2'] = location_file[i][2]  # Get the end latitude.
        location_dict['longitude2'] = location_file[i][3]  # Get the end longitude.
        locations.append(location_dict)  # Add to the dictionary.

    # The tokens allow for calls to the uber API. Add in a list of your tokens
    # as strings here.
    uber_server_tokens = []

    # The API end points for uber that are used to make calls.
    price_url = 'https://api.uber.com/v1/estimates/price'
    time_url = 'https://api.uber.com/v1/estimates/time'

    # These api param objects are used to send requests to the API,
    # we create api_param objects for each location.
    price_api_params = []
    time_api_params = []

    # Create an api param object for each location so a call to the
    # api can be made for each one.
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

    path = os.getcwd() + '../uber_data'
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
        # Get and write the information.
        gather_loop(price_api_params, time_api_params, uber_server_tokens, os.path.join(path, output_file_name))
		
		# Time between API calls.
        time.sleep(250)
       	new_day = datetime.datetime.today().day

        # If the day has changed, close the previous file and open a new file.
        if new_day != curr_day:
            curr_day = new_day  
            
	    # Create new name by the date for the file.
            output_file_name = str(time.strftime("%m_%d_%Y")) + '.csv'
            with open(os.path.join(pth, output_file_name), 'wb') as f:
                fileWriter = csv.writer(f, delimiter=',')
                fileWriter.writerow(['timestamp', 'surge_multiplier', 'expected_wait_time', 
				     'duration', 'distance', 'estimate', 'low_estimate', 
				     'high_estimate', 'product_type', 'start_geoid', 
				     'start_latitude', 'start_longitude', 'end_latitude', 
				     'end_longitude'])


if __name__ == '__main__':
    main()
    
