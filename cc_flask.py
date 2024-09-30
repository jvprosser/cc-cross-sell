from flask import Flask, render_template, request
from impala.dbapi import connect
import mlflow
import pandas as pd
import json
import os
from cc_normalize import normalize_data
import sqlalchemy as db
from sqlalchemy import Table, MetaData, select, func, text

app = Flask(__name__)

# Load the MLflow model
#model_path = "path/to/your/mlflow/model.pkl"
#loaded_model = mlflow.pyfunc.load_model(model_path)

logged_model = '/home/cdsw/.experiments/b0se-2xuo-w64u-04yn/atgg-nm4g-tqs7-1r4h/artifacts/lgb_model'

# Load model as a PyFuncModel.
loaded_model = mlflow.pyfunc.load_model(logged_model)

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
        return connect(host='dwarehouse-gateway.jqrhv9ch.l39n-29js.a8.cloudera.site', port=443, \
                              timeout=None, use_ssl=True, ca_cert=None, database='jvp_cc_lead_model',\
                              user="csso_jprosser", password="BadPass#1", kerberos_service_name='impala', \
                              auth_mechanism="LDAP", krb_host=None, use_http_transport=True, \
                              http_path='cliservice', http_cookie_names=None, retries=3, jwt=None,\
                              user_agent=None)

      engine = db.create_engine('impala://', creator=conn)


      with engine.connect() as conn:
        query = f"""
          SELECT Age, Vintage, Avg_Account_Balance, Channel_Code, Credit_Product, Gender, Is_Active ,Occupation, Region_Code 
          FROM jvp_cc_lead_model.cc_lead_train  
          WHERE ID = '{id}'
          """
        print(f"query: {query}")

        result = conn.execute(text(query)).fetchone()
        print(f"back from query: {result}")
 
        conn.close()

        if result:
            # Store results in a dictionary
            data = {
               
                'Age': result[0],
                'Vintage': result[1],
                'Avg_Account_Balance': result[2],
                'Channel_Code': result[3],
                'Credit_Product': result[4],
                'Gender': result[5],
                'Is_Active': result[6],
                'Occupation': result[7],
                'Region_Code': result[8]
            }
            print(f"data dict: {data}")

            # Normalize the data
            normalized_data = normalize_data(data)
            print(f"Normalized_data: {[normalized_data]}")
            cc_vector_df = pd.DataFrame([normalized_data])
            print(f"Vector df = {cc_vector_df}")
            #cc_vector='{"Age":0.6831522461,"Vintage":-0.8637453118,"Avg_Account_Balance":-0.1245877441,"Channel_Code":1.0,"Credit_Product":2.0,"Gender":1.0,"Is_Active":0.0,\
            #"Occupation":3.0,"Region_Code":33.0}'          
            #data = json.loads(cc_vector)
            #cc_vector_df = pd.DataFrame([data])        
            #print(f"Vector df = {cc_vector_df}")

            # Make prediction
            prediction = loaded_model.predict(cc_vector_df)
            print(f"prediction = {prediction[0]}")
            return render_template('result.html', prediction=prediction[0])
        else:
            return render_template('index.html', error="No data found for this ID")

    return render_template('index.html')


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=PORT)
    

