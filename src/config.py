import os
from dotenv import load_dotenv

app_root = os.path.dirname(os.path.dirname((os.path.abspath(__file__))))
data_root = f'{app_root}/data'
results_root = f'{app_root}/results'

env_path = f'{app_root}/.env'
load_dotenv(dotenv_path=env_path)

snowflakeAccount    = os.getenv("SNOWFLAKE_ACCOUNT")
snowflakeUsername   = os.getenv("SNOWFLAKE_USERNAME")
snowflakePassword   = os.getenv("SNOWFLAKE_PASSWORD")
snowflakeWarehouse  = os.getenv("SNOWFLAKE_WAREHOUSE")
snowflakeDatabase   = os.getenv("SNOWFLAKE_DATABASE")
snowflakeSchema     = os.getenv("SNOWFLAKE_SCHEMA")
snowflakeRole       = os.getenv("SNOWFLAKE_ROLE")

jiraUsername = os.getenv("JIRA_USERNAME")
jiraPassword = os.getenv("JIRA_PASSWORD")
