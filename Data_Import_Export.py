
#****************************************************************************************************#
#                                   Data import and export from Database                             #
#****************************************************************************************************#
import pandas as pd
import os
from sqlalchemy import create_engine


#====================================================================================================#
#                                   Create database connection                                       #
#====================================================================================================#

os.chdir('F:\\GIT\\Git_KaleemMohammed')

DB = { "database": "postgres",\
       "host"    : "localhost",\
       "port"    : "5432",\
       "user"    : "postgres",\
       "password": "postgres",
       "schema"  : "public"
     }

def get_db_connection():
    """
    Description:
        Create the database connection. 

    Args:

    Returns:
        Connect to respective database

    """
    
    con ='postgresql://'+DB['user']+':'\
                        +DB['password']+'@'\
                        +DB['host']+':'\
                        +DB['port'] +'/'\
                        +DB['database']
    
    db_connection = create_engine(con)         
    return db_connection
        
#====================================================================================================#
#                                   To import the data from Database                                 #
#====================================================================================================#
    
def get_data(table):
    
    """
    Description:
        Import the data from given "table"

    Args:
        table name
        Enter table name as a string
        Ex: To import data from test_df, call get_attribute() method as get_attribute('test_df')

    Returns:
        All the rows and columns from "table"

    """
    try:
        db_conn = get_db_connection()
        sql_attribute = """
                           select	* from {}.{}
                        """.format(DB['schema'],table)

        df = pd.read_sql(sql_attribute, db_conn)
        db_conn.dispose()
    except Exception as e:
        raise Exception('get_data- ' + str(e))

    return df

# Demo
test = get_data('test_df')
print(test)
#====================================================================================================#
#                                   To Load the data to Database                                     #
#====================================================================================================#
    

# Method 1 : Dump dataframe into Database

def insertIntoBackupTable(tablename,DB_tablename,type_operation):
    
    """
    Description:
        Load dataframe to Database table

    Args:
        table name
        table name in DB
        type_operation = 'append/replace'
        Enter table name in DB as a string
        Ex: To export dataframe df into DB table test_df, 
            call insertIntoBackupTable() method as insertIntoBackupTable(df,'test_df','append')
            
    Returns:
        Export data into DB

    """
    try:
        engine = get_db_connection()
        tablename.to_sql(DB_tablename,\
                             engine,\
                             index=False,\
                             if_exists= type_operation,\
                             schema=DB['schema'])
    except Exception as e:
        print('Error at insertIntoBackupTable'+str(e))


# Demo
df = pd.read_csv('test_df.csv')
df.columns = map(str.lower, df.columns)
insertIntoBackupTable(df,'test_df','append')



