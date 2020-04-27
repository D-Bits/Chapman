"""
File for storing various config info, such as
database credentials.
"""
from os import getenv
from psycopg2 import connect
from sqlalchemy import create_engine
from requests import get
from dotenv import load_dotenv


# Load environment vars from .env file
load_dotenv()

# Create a SQLAlchemy engine for the local Postgres instance
local_pg_string = getenv("LOCAL_DB_STRING") 
local_pg_engine = create_engine(local_pg_string)
# Create a SQLAlchemy engine for the AWS Postgres instance 
gcp_pg_string = getenv("GCP_DB_STRING") 
gcp_pg_engine = create_engine(gcp_pg_string)


# API endpoint(s) and key
api_key = getenv('MOCKAROO_API_KEY')

# For "super secret" API
secrets_endpoint = get(f'https://my.api.mockaroo.com/super_secret_info.json?key={api_key}')
secrets_api_json = secrets_endpoint.json()

# For "servers" API
server_listing_endpoint = get(f'https://my.api.mockaroo.com/servers.json?key=53c79190')
server_listing_json = server_listing_endpoint.json()

# For "stocks" API
stocks_endpoint = get(f'https://my.api.mockaroo.com/stocks.json?key={api_key}')
stocks_json = stocks_endpoint.json()