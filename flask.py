from flask import Flask, request, jsonify
import xgboost as xgb
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
import joblib
import pickle

app = Flask(__name__)

# Load the pre-trained model
with open('model_filename.pkl', 'rb') as file:
    model = pickle.load(file)

# Initialize label encoders for categorical variables
le_gender = LabelEncoder()
le_region = LabelEncoder()
le_occupation = LabelEncoder()
le_channel = LabelEncoder()
le_credit_product = LabelEncoder()

# Fit label encoders (you should do this with your training data)
le_gender.fit(['Male', 'Female'])
le_region.fit(['RG268', 'RG270', 'RG271', 'RG272', 'RG273', 'RG274', 'RG275', 'RG276', 'RG277', 'RG278', 'RG279', 'RG280', 'RG281', 'RG282', 'RG283', 'RG284', 'RG285'])
le_occupation.fit(['Salaried', 'Self_Employed', 'Other'])
le_channel.fit(['X1', 'X2', 'X3', 'X4'])
le_credit_product.fit(['Yes', 'No'])

@app.route('/predict', methods=['POST'])
def predict():
    # Get data from POST request
    data = request.json
    
    # Convert to DataFrame
    df = pd.DataFrame([data])
    
    # Preprocess the data
    df['Gender'] = le_gender.transform(df['Gender'])
    df['Region_Code'] = le_region.transform(df['Region_Code'])
    df['Occupation'] = le_occupation.transform(df['Occupation'])
    df['Channel_Code'] = le_channel.transform(df['Channel_Code'])
    df['Credit_Product'] = le_credit_product.transform(df['Credit_Product'])
    
    # Ensure correct order of features
    features = ['Gender', 'Age', 'Region_Code', 'Occupation', 'Channel_Code', 
                'Vintage', 'Credit_Product', 'Avg_Account_Balance', 'Is_Active']
    X = df[features]
    
    # Make prediction
    dmatrix = xgb.DMatrix(X)
    prediction = model.predict(dmatrix)
    
    # Convert prediction to binary (assuming 0.5 threshold)
    is_lead = bool(prediction[0] > 0.5)
    
    # Return prediction as JSON
    return jsonify({'is_lead': is_lead})

if __name__ == '__main__':
    app.run(debug=True)