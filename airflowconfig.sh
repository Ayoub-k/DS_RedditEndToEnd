#!/bin/bash

# Load environment variables from .env file
#!/bin/bash

# Load the variables from the .env file
source .env


# Initialize Airflow database if it doesn't exist
# if [[ ! -e airflow.db ]]; then
    
# fi

# # Create an Airflow user if it doesn't exist
# if [[ ! $(airflow users list) ]]; then
    
# fi

airflow db init && airflow users create \
        --username $AIRFLOW_USERNAME \
        --password $AIRFLOW_PASSWORD \
        --firstname $AIRFLOW_FIRSTNAME \
        --lastname $AIRFLOW_LASTNAME \
        --role $AIRFLOW_ROLE \
        --email $AIRFLOW_EMAIL


# Start the Airflow scheduler and webserver
airflow scheduler & exec airflow webserver
