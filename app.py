import streamlit as st
import joblib
import pandas as pd
import xgboost as xgb

# Load the trained model
model = joblib.load('xgboost_model.pkl')

# Define a function to get predictions
def predict(features):
    dmatrix = xgb.DMatrix(features)
    prediction = model.predict(dmatrix)
    return prediction

# Streamlit app layout
st.title("Horse Racing Time Prediction App")
st.write("Enter the features below to predict the race time.")

# User input for features
cinsiyet_binary = st.selectbox("Cinsiyet (1=Erkek, 0=Dişi)", [0, 1])
irk_binary = st.selectbox("Irk (1=İngiliz, 0=Other)", [0, 1])
# make mesafe float input not slider but box
mesafe_normalized = st.number_input("Mesafe (normalized)", min_value=0.0, max_value=1.0, value=0.5)
age_normalized = st.number_input("Age (normalized)", min_value=0.0, max_value=1.0, value=0.5)
city_label = st.number_input("City Label", min_value=0, max_value=16, value=14)

# Dynamic Condition feature input
condition_columns = ['Condition_aa', 'Condition_ad', 'Condition_ae', 'Condition_ag', 
                    'Condition_ak', 'Condition_da', 'Condition_dd', 'Condition_de', 
                    'Condition_dg', 'Condition_dk', 'Condition_ka', 'Condition_kd', 
                    'Condition_ke', 'Condition_kg', 'Condition_kk', 'Condition_ya', 'Condition_yk']

condition_values = {}
for cond in condition_columns:
    condition_values[cond] = st.selectbox(f"{cond}", [0, 1])

# Prepare the input data in the same format as training data
input_data = {
    'Cinsiyet_binary': [cinsiyet_binary],
    'Irk_binary': [irk_binary]
}

# Add the Condition features to the input data
input_data.update(condition_values)

# add mesafe, age and city_label
input_data['Mesafe_normalized'] = [mesafe_normalized]
input_data['Age_normalized'] = [age_normalized]
input_data['City_label'] = [city_label]

# Convert input data to DataFrame
input_df = pd.DataFrame(input_data)

# Predict button
if st.button("Predict"):
    prediction = predict(input_df)
    st.write(f"Predicted Race Time (normalized): {prediction[0]}")
