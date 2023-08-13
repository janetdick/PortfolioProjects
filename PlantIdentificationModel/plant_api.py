import os
import psycopg2
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
from io import BytesIO
from PIL import Image

# Load environment variables from .env file
load_dotenv()

# Retrieve the database credentials from environment variables
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

# API endpoint URL
API_ENDPOINT = 'https://perenual.com/api/species-list'

# API key
headers = {
    'Authorization': 'sk-T0E964cff5b68e1c51782'
}

# Initialize variables
page_number = 1
requests_limit = 300


# Connecting to the PostgreSQL database
conn = psycopg2.connect(
    host="db_host",
    database="db_name",
    user="db_user",
    password="db_password")


# Create a cursor to execute SQL commands
cursor = conn.cursor()

# Loop until all plant species are inserted
while True:
    # Make the API request
    params = {'page': page_number}
    response = requests.get(API_ENDPOINT, headers=headers, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        # Process the response (assuming the API returns JSON data)
        data = response.json()

        # Extracting individual plant species data from the response
        plant_species_list = data['data']

    # Looping through each plant species and inserting data into the PostgreSQL database
        for plant_species in plant_species_list:
            plant_id = plant_species['id']
            common_name = plant_species['common_name']
            scientific_name = plant_species['scientific_name'][0]  # one scientific name
            cycle = plant_species['cycle']
            watering = plant_species['watering']
            sunlight = plant_species['sunlight']
        
        # Extract image information
        image_id = plant_species['default_image']['image_id']
        image_url = plant_species['default_image']['original_url']

        # Download the image and convert it to binary data
        image_response = requests.get(image_url)
        if image_response.status_code == 200:
            image_binary = BytesIO(image_response.content).getvalue()
        else:
            print(f"Failed to download image for plant_id: {plant_id}")
            continue

        # Insert plant data into the plant_species table
        insert_plant_query = "INSERT INTO plant_species (id, common_name, scientific_name, cycle, watering, sunlight) " \
                            "VALUES (%s, %s, %s, %s, %s, %s);"
        data_to_insert = (plant_id, common_name, scientific_name, cycle, watering, sunlight)
        cursor.execute(insert_plant_query, data_to_insert)

        # Insert image binary data into the plant_images table
        insert_image_query = "INSERT INTO plant_images (plant_id, image_id, image_data) " \
                            "VALUES (%s, %s, %s);"
        image_data_to_insert = (plant_id, image_id, psycopg2.Binary(image_binary))
        cursor.execute(insert_image_query, image_data_to_insert)

        # Commit the changes after each batch
        conn.commit()

        # Check if the daily API request limit is reached
        if page_number >= requests_limit:
            # Wait until the next day before continuing with the next batch
            tomorrow = datetime.now() + timedelta(days=1)
            reset_time = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
            time_to_wait = (reset_time - datetime.now()).total_seconds()
            print(f"Waiting until the next day... (Time remaining: {time_to_wait} seconds)")
            time.sleep(time_to_wait)

            # Reset the page number and continue with the next batch
            page_number = 1
        else:
            # Increment the page number for the next API request
            page_number += 1

    else:
        print(f"Error: {response.status_code} - {response.text}")

# Close the cursor and the connection
cursor.close()
conn.close()