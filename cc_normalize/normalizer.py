import pickle
import pandas as pd
import numpy
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler

def normalize_data(data):
  print("hi")
  return normalized

raw_data='{"Age": 27, "Vintage": 26, "Avg_Account_Balance": 707906, "Channel_Code": "X1", "Credit_Product": "No", "Gender": "Female", "Is_Active": "No", "Occupation": "Salaried", "Region_Code": "RG256"}'  
normalized = {}

# Later, to load the scaler:
with open('data/standardscaler.pkl', 'rb') as file:
    loaded_standardscaler = pickle.load(file)

with open('data/labelencoder.pkl', 'rb') as file:
    loaded_labelencoder = pickle.load(file)

with open('data/data_num_cols.pkl', 'rb') as file:
    data_num_cols = pickle.load(file)

with open('data/data_cat_cols.pkl', 'rb') as file:
    data_cat_cols = pickle.load(file)        

data = json.loads(raw_data)

cc_vector_df = pd.DataFrame([data])    
    
    
data_num_data = cc_vector_df.loc[:, data_num_cols]
data_cat_data = cc_vector_df.loc[:, data_cat_cols]

print("Shape of num data:", data_num_data.shape)
print("Shape of cat data:", data_cat_data.shape)

#data_num_data_s = loaded_standardscaler.fit_transform(data_num_data)

data_num_data_norm = loaded_standardscaler.fit_transform(data_num_data)
data_num_data_s = pd.DataFrame(data_num_data_norm, columns = data_num_cols)

data_cat_data_norm = data_cat_data.apply(loaded_labelencoder.fit_transform)
data_cat_data_s = pd.DataFrame(data_cat_data_norm, columns = data_cat_cols)

data_new = pd.concat([data_num_data_s, data_cat_data_s], axis = 1)

print(f"data new={data_new}")

#for key, value in data.items():
#    if key == 'Gender':
#        normalized[key] = 1 if value.lower() == 'male' else 0
#    elif key == 'Age':
#        normalized[key] = (value - 18) / (100 - 18)  # Assuming age range 18-100
#    elif key == 'Avg_Account_Balance':
#        normalized[key] = value / 1000000  # Assuming max balance of 1,000,000
#    elif key in ['Region_Code', 'Occupation', 'Channel_Code', 'Credit_Product']:
#        # For categorical variables, you might want to use one-hot encoding
#        # This is a simplified version
#        normalized[key] = hash(value) % 10 / 10
#    elif key == 'Is_Active':
#        normalized[key] = 1 if value else 0
#    else:
#        normalized[key] = value  # Keep other values as is


raw_data="{'Age': 27, 'Vintage': 26, 'Avg_Account_Balance': 707906, 'Channel_Code': 'X1', 'Credit_Product': 'No', 'Gender': 'Female', 'Is_Active': 'No', 'Occupation': 'Salaried', 'Region_Code': 'RG256'}"
