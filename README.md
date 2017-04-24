# Uber_Lyft_Scraping
These scripts I wrote can be used to scrape pricing and ride information from the Uber and Lyft APIs. Each script makes an API call every ~3 minutes to get surge pricing and wait time data for the locations in locations.csv. The locations in locations.csv are the geo_ids in San Francisco and Seattle. The files we run until they are stopped. 

Both the Uber and Lyft APIs need keys. The Uber key and the Lyft keys can be added on line 151 of the respective scripts.

# Running Instructions

python lyft_scraping.py 

python uber_scraping.py

Add & after these command to run in the background.

# Data
Data will be stored in either lyft_data, or uber_data automatically. A new file is created for each day.
