import mlflow

import pandas as pd
import json
from cc_normalize import normalize_data

raw_data="{'Age': 27, 'Vintage': 26, 'Avg_Account_Balance': 707906, 'Channel_Code': 'X1', 'Credit_Product': 'No', 'Gender': 'Female', 'Is_Active': 'No', 'Occupation': 'Salaried', 'Region_Code': 'RG256'}"
ndata = normalize_data(raw_data)

cc_vector='{"Age":0.6831522461,"Vintage":-0.8637453118,"Avg_Account_Balance":-0.1245877441,"Channel_Code":1.0,"Credit_Product":2.0,"Gender":1.0,"Is_Active":0.0,\
"Occupation":3.0,"Region_Code":33.0}'

data = json.loads(cc_vector)

cc_vector_df = pd.DataFrame([data])
#cc_vector_df = pd.DataFrame([0.6831522461, -0.8637453118, -0.1245877441, 1.0, 2.0, 1.0, 0.0, 3.0, 33.0]).T

experiment = mlflow.get_experiment_by_name("JVP_FNB_CC_Cross_sell")
experiment_id = experiment.experiment_id
#
#
## Load the model
model_uri = "runs:/atgg-nm4g-tqs7-1r4h/lgb_model"  # Replace <run_id> with the actual run ID
print("Create the pip requirements.txt file for this model. Store it with the artifacts.")
mlflow.pyfunc.get_model_dependencies(model_uri)
 
# pip install -r /home/cdsw/.experiments/b0se-2xuo-w64u-04yn/atgg-nm4g-tqs7-1r4h/artifacts/lgb_model/requirements.txt

## Or if it's a registered model:
## model_uri = "models:/<model_name>/<version_or_stage>"
#
#loaded_model = mlflow.pyfunc.load_model(model_uri)
#
## Assuming you have input data in a pandas DataFrame called 'input_data'
#val = loaded_model.predict(cc_vector_df)
#
#print(f"val={val[0]}")

print(f"vector df = {cc_vector_df}")

logged_model = '/home/cdsw/.experiments/b0se-2xuo-w64u-04yn/atgg-nm4g-tqs7-1r4h/artifacts/lgb_model'

# Load model as a PyFuncModel.
loaded_model = mlflow.pyfunc.load_model(logged_model)

# Predict on a Pandas DataFrame.
val = loaded_model.predict(cc_vector_df)
print(f"val={val[0]}")