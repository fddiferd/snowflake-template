from utils.snowflake_connection import SnowflakeConnection
from snowflake.snowpark import Session
import os
from typing import Dict


def session() -> Session:
    print("Getting session")

    # if running in snowflake
    connection: Session | None = SnowflakeConnection().connection
    if connection is not None:
        print("Reinstating connections from active session")
        return connection

    # if running locally with a config file
    elif os.path.exists('../config.py') or os.path.exists('config.py'):
        print("Creating session from config file")
        from config import snowpark_config
        session: Session = Session.builder.configs(snowpark_config).create()

        # print(session.get_current_role())
        # print(session.sql("SELECT CURRENT_ACCOUNT(), CURRENT_REGION()").collect())
        # print(session.sql("SELECT CURRENT_USER(), CURRENT_ROLE()").collect())
        # print(str(snowpark_config["database"]))
        # session.use_warehouse(str(snowpark_config["warehouse"]))
        # session.use_database(str(snowpark_config["database"]))
        # session.use_schema(str(snowpark_config["schema"]))
        return session
    
    else:
        raise Exception("Unable to create a session")
    
    # else:
    #     print("Creating session from environment variables")
    #     connection_parameters: Dict[str, int | str] = {
    #         "account": os.environ["SNOWSQL_ACCOUNT"],
    #         "user": os.environ["SNOWSQL_USER"],
    #         "password": os.environ["SNOWSQL_PWD"],
    #         "role": os.environ["SNOWSQL_ROLE"],
    #         "warehouse": os.environ["SNOWSQL_WAREHOUSE"],
    #         "database": os.environ["SNOWSQL_DATABASE"],
    #         "schema": os.environ["SNOWSQL_SCHEMA"]
    #     }
    #     SnowflakeConnection().connection = Session.builder.configs(connection_parameters).create()
    #     session = SnowflakeConnection().connection

if __name__ == "__main__":
    s: Session = session()
    print(s.get_current_database())