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

# Load local PG credentials
local_host = getenv("LOCAL_PG_HOST")
local_db = getenv("LOCAL_PG_DB")
local_user = getenv("LOCAL_PG_USER") 
pgpass = getenv("LOCAL_PG_PASS")

# For cursor.copy_from()
local_pg_conn = connect(database=db, user=local_user, password=pgpass, host=local_host)

# Create a SQLAlchemy engine for the local Postgres instance
local_pg_string = getenv("LOCAL_SQLA_STRING") 
local_pg_engine = create_engine(local_pg_string)
# Create a SQLAlchemy engine for the AWS Postgres instance 
gcp_pg_string = getenv("GCP_SQLA_STRING") 
gcp_pg_engine = create_engine(gcp_pg_string)


# For "super secret" API
secrets_endpoint = get(getenv("SECRETS_ENDPOINT"))
secrets_api_json = secrets_endpoint.json()

# For "servers" API
server_listing_endpoint = get(getenv("SERVERS_ENDPOINT"))
server_listing_json = server_listing_endpoint.json()

# For "stocks" API
stocks_endpoint = get(getenv("STOCKS_ENDPOINT"))
stocks_json = stocks_endpoint.json()
