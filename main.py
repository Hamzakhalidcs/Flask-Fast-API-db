from flask import Flask, jsonify, request
from pymongo import MongoClient
from typing import Optional
import pyodbc
import pandas as pd
from functions import *
import base64


app = Flask(__name__)


# Geting Data from MongoDB
@app.route('/get_data', methods=['GET'])
def get_data():
    try:
        collection = connect_to_mongo()

        if collection:
            name = request.args.get('name')
            query = {}
            if name:
                query = {'name': {'$regex': name, '$options': 'i'}}

            data = collection.find(query)

            data_list = []
            for record in data:
                record['_id'] = str(record['_id'])
                data_list.append(record)
            return jsonify(data_list), 200
        
        else:
            return jsonify({
                'message': 'An error occurred while getting the data',
                'error': 'Failed to connect to MongoDB'
            }), 500
        
    except Exception as e:
        print('Error while getting the data:', e)
        return jsonify({'error': 'An error occurred while fetching data'}), 500


# Getting the Data from SQL Server
@app.route('/some_data', methods=['GET'])
def some_data():
    return jsonify('Hello from some')

@app.route('/get_data_from_sql', methods=['GET'])
def get_sql_data():
    conn = connect_to_sql()

    if conn:
        query = "SELECT top 5 * FROM {}.{}.{} ".format(database, schema, table_name)
        data = pd.read_sql(query, conn)
        data_dict = data.to_dict(orient="records")
        conn.close()
        return jsonify(data_dict), 200
  
        # return data.to_json(orient="records")
    else:
        return jsonify({'error': 'Failed to connect to SQL Server'}), 500
    
@app.route('/get_from_sql', methods=['GET'])
def sql_data():
    conn = connect_to_sql()
    if conn:
        try:
            query = "SELECT TOP 5 * FROM {}.{}.{}".format(database, schema, table_name)
            data = pd.read_sql(query, conn)
            
            # Decode any bytes columns with error handling
            for col in data.columns:
                if data[col].dtype == 'object':
                    data[col] = data[col].apply(lambda x: x.decode('utf-8', errors='ignore') if isinstance(x, bytes) else x)



            data_dict = data.to_dict(orient="records")
            return jsonify(data_dict), 200
        except Exception as e:
            return jsonify({'error': str(e)}), 500
        finally:
            conn.close()
    else:
        return jsonify({'error': 'Failed to connect to SQL Server'}), 500



@app.route('/insert_data', methods=['POST'])
def insert_data():
    try:
        # Get JSON data from request
        data = request.get_json()

        # Ensure you're not returning any sets
        if isinstance(data, dict):
            # Example: Convert any set within data to a list
            for key, value in data.items():
                if isinstance(value, set):
                    data[key] = list(value)

        # Simulate data insertion logic here
        # insert_to_database(data)
        query = """
        INSERT INTO HumanResources.EmployeePayHistory (BusinessEntityID, ModifiedDate, PayFrequency, Rate, RateChangeDate)
        VALUES (?, ?, ?, ?, ?)
        """
        
        # Extract values from JSON
        params = (
            data.get('BusinessEntityID'),
            data.get('ModifiedDate'),
            data.get('PayFrequency'),
            data.get('Rate'),
            data.get('RateChangeDate')
        )
        
        # Connect to the database
        conn = connect_to_sql()
        if not conn:
            return jsonify({"error": "Failed to connect to the database"}), 500
        
        # Execute the query
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()

        # Close the connection
        cursor.close()
        conn.close()

        return jsonify({"message": "Data inserted successfully!"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 400


if __name__ == '__main__':
    app.run(debug=True)
