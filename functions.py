import pyodbc
from pymongo import MongoClient
import sys

driver = '{SQL Server}'
server = 'DESKTOP-959UNBS'  
database = 'Adventureworks'
schema = 'HumanResources'
table_name = 'Employee'

def connect_to_mongo():
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client.university
        collection = db.student
        return collection
    except Exception as e:
        print('Error occurred while connecting to MongoDB:', e)
        return None

# Function to connect to sql
def connect_to_sql():
    try:
        conn = pyodbc.connect(
            'Driver='+driver+';'
            'Server='+server+';'
            'Database='+database+';'
            'Trusted_Connection=yes'
        )
        print('Connected to SQL Server')
        return conn
    except Exception as e:
        print('Error occurred while connecting to SQL Server:', sys.exc_info()[1])
        return None