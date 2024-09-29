from flask import Flask, render_template, request
from impala.dbapi import connect
import mlflow
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

print(f"Listening on Port: {os.environ['CDSW_APP_PORT']}")

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
          SELECT Gender, Age, Region_Code, Occupation, Channel_Code, 
                 Credit_Product, Avg_Account_Balance, Is_Active 
          FROM jvp_cc_lead_model.cc_lead_train  
          WHERE ID = '{id}'
          """
        result = conn.execute(text(query)).fetchone()
          
        conn.close()

        if result:
            # Store results in a dictionary
            data = {
                'Gender': result[0],
                'Age': result[1],
                'Region_Code': result[2],
                'Occupation': result[3],
                'Channel_Code': result[4],
                'Credit_Product': result[5],
                'Avg_Account_Balance': result[6],
                'Is_Active': result[7]
            }

            # Normalize the data
            normalized_data = normalize_data(data)

            # Make prediction
            prediction = loaded_model.predict(normalized_data)

            return render_template('result.html', prediction=prediction)
        else:
            return render_template('index.html', error="No data found for this ID")

    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)