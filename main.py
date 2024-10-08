from flask import Flask, jsonify, request
from pymongo import MongoClient
from typing import Optional
import pyodbc
import pandas as pd
from functions import *

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
@app.route('/get_data_from_sql', methods=['GET'])
def get_sql_data():
    conn = connect_to_sql()

    if conn:
        query = "SELECT TOP 5 * FROM {}.{}.{}".format(database, schema, table_name)
        data = pd.read_sql(query, conn)
        data_dict = data.to_dict(orient="records")
        conn.close()
        return jsonify(data_dict), 200
  
        # return data.to_json(orient="records")
    else:
        return jsonify({'error': 'Failed to connect to SQL Server'}), 500

if __name__ == '__main__':
    app.run(debug=True)
