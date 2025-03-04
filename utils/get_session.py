from utils.snowflake_connection import SnowflakeConnection
from snowflake.snowpark import Session
import os
from typing import Dict


def session() -> Session:

    # if running in snowflake
    if SnowflakeConnection().connection:
        print("Reinstating connections from active session")
        session: Session | None = SnowflakeConnection().connection

    # if running locally with a config file
    elif os.path.exists('../config.py') or os.path.exists('config.py'):
        print("Creating session from config file")
        from config import snowpark_config
        return Session.builder.configs(snowpark_config).create()
    
    else:
        print("Creating session from environment variables")
        connection_parameters: Dict[str, int | str] = {
            "account": os.environ["SNOWSQL_ACCOUNT"],
            "user": os.environ["SNOWSQL_USER"],
            "password": os.environ["SNOWSQL_PWD"],
            "role": os.environ["SNOWSQL_ROLE"],
            "warehouse": os.environ["SNOWSQL_WAREHOUSE"],
            "database": os.environ["SNOWSQL_DATABASE"],
            "schema": os.environ["SNOWSQL_SCHEMA"]
        }
        SnowflakeConnection().connection = Session.builder.configs(connection_parameters).create()
        session = SnowflakeConnection().connection
        
    if session is None:
        raise Exception("Unable to create a session")
    return session
