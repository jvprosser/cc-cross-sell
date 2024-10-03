from flask import Flask, render_template, request
from impala.dbapi import connect
import mlflow
import pandas as pd
import json
import os

import sqlalchemy as db
from sqlalchemy import Table, MetaData, select, func, text
import pickle
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler

USER_PREFIX=''
path_root='/home/cdsw'

import configparser
config = configparser.ConfigParser()
config.read(f"{path_root}/parameters.conf")

database      =f"{USER_PREFIX}_{config.get('general','database')}"
database_host =f"{config.get('flask','database_host')}"
database_user =f"{config.get('flask','database_user')}"
database_pass =f"{config.get('flask','database_password')}"

app = Flask(__name__)

# Load the MLflow model along with the transformers we built in the model notebook

logged_model = '/home/cdsw/.experiments/b5se-9i4r-sv7y-h5me/vr22-6y1w-xne8-jyg3/artifacts/lgb_model'

# Load model as a PyFuncModel.
loaded_model = mlflow.pyfunc.load_model(logged_model)

with open('data/standardscaler.pkl', 'rb') as file:
    loaded_standardscaler = pickle.load(file)

#with open('data/labelencoder.pkl', 'rb') as file:
#    loaded_labelencoder = pickle.load(file)

with open('data/onehotenc.pkl','rb') as file:
    ohefit = pickle.load(file)
    
with open('data/data_num_cols.pkl', 'rb') as file:
    data_num_cols = pickle.load(file)

with open('data/data_cat_cols.pkl', 'rb') as file:
    data_cat_cols = pickle.load(file) 

def normalize_data(raw_data):
    print(f"top raw_data = {raw_data}\n")

    #Testing code
    #raw_data='{"Age": 27, "Vintage": 26, "Avg_Account_Balance": 707906, "Channel_Code": "X1", "Credit_Product": "No", "Gender": "Female", "Is_Active": "No", "Occupation": "Salaried", "Region_Code": "RG256"}'  
    #data = json.loads(raw_data)
    #print(f"json raw_data = {[raw_data]}")

    cc_vector_df = pd.DataFrame([raw_data])    
    print(f"raw pandas dataframe {cc_vector_df}\n")    
        
    data_num_data = cc_vector_df.loc[:, data_num_cols]
    data_cat_data = cc_vector_df.loc[:, data_cat_cols]
    data_num_data['Avg_Account_Balance'] = np.log(data_num_data['Avg_Account_Balance'])
    print(f"Shape of num data: {data_num_data.shape}")
    print(f"Shape of cat data: {data_cat_data.shape}")
    
    print(f"data_num_data: {data_num_data}\n")
    print(f"data_cat_data: {data_cat_data}\n")
         
    data_num_data_s = loaded_standardscaler.transform(data_num_data)
    data_num_data_df_s = pd.DataFrame(data_num_data_s, columns = data_num_cols)
    
    #data_cat_data_norm = loaded_labelencoder.transform(data_cat_data)
    data_cat_data_norm = ohefit.transform(data_cat_data)    

    #data_cat_data_df = pd.DataFrame(data_cat_data_norm, columns = data_cat_cols)
    
    data_new = pd.concat([data_num_data_df_s, data_cat_data_norm], axis = 1)
    
    #print(f"transformed data={data_new}\n")
    return data_new

PORT = os.getenv('CDSW_APP_PORT', '8090')
print(f"Port: {PORT}")
print(f"Listening on Port: {PORT}")

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
      id = request.form['id']
        #id='NNVBBKZB'

        # Query the database
        # ## jdbc:impala://dwarehouse-gateway.jqrhv9ch.l39n-29js.a8.cloudera.site:443/;ssl=1;transportMode=http;httpPath=dwarehouse/cdp-proxy-api/impala;AuthMech=3;

        #conn = connect(host='your_impala_host', port=21050)

      def conn():
        return connect(host=database_host, port=443, \
                              timeout=None, use_ssl=True, ca_cert=None, database=database,\
                              user=database_user, password=database_pass, kerberos_service_name='impala', \
                              auth_mechanism="LDAP", krb_host=None, use_http_transport=True, \
                              http_path='cliservice', http_cookie_names=None, retries=3, jwt=None,\
                              user_agent=None)

      engine = db.create_engine('impala://', creator=conn)


      with engine.connect() as conn:
        query = f"""
          SELECT Age, Vintage, Avg_Account_Balance, Channel_Code, Credit_Product, Gender, Is_Active ,Occupation, Region_Code 
          FROM {USER_PREFIX}_cc_lead_model.cc_lead_train  
          WHERE ID = '{id}'
          """
        print(f"query: {query}")

        result = conn.execute(text(query)).fetchone()
        print(f"back from query: {result}")
 
        conn.close()

        if result:
            # Store results in a dictionary
            data = {
                "Age": result[0],
                "Vintage": result[1],
                "Avg_Account_Balance": result[2],
                "Channel_Code": result[3],
                "Credit_Product": result[4],
                "Gender": result[5],
                "Is_Active": result[6],
                "Occupation": result[7],
                "Region_Code": result[8]
            }
            print(f"data dict: {data}")

            # Normalize the data
            normalized_data = normalize_data(data)
            print(f"Normalized_data: {[normalized_data]}")
            #print(f"Normalized_data: {normalized_data}")

            # Make prediction
            prediction = loaded_model.predict(normalized_data)
            print(f"prediction = {prediction[0]}")
            return render_template('result.html', prediction=prediction[0])
        else:
            return render_template('index.html', error="No data found for this ID")

    return render_template('index.html')


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=PORT)
    



