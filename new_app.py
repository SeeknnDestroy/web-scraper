import streamlit as st
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import joblib
import xgboost as xgb
import os
import json

# Function to load encoders
def load_label_encoders(encoder_dir, categorical_columns):
    loaded_encoders = {}
    for col in categorical_columns:
        encoder_path = os.path.join(encoder_dir, f'{col}_label_encoder.joblib')
        if os.path.exists(encoder_path):
            loaded_encoders[col] = joblib.load(encoder_path)
        else:
            st.error(f"Encoder for {col} not found at {encoder_path}")
    return loaded_encoders

def load_scalers(scaler_dir, numeric_columns):
    loaded_scalers = {}
    for col in numeric_columns:
        scaler_path = os.path.join(scaler_dir, f'{col}_scaler.joblib')
        if os.path.exists(scaler_path):
            loaded_scalers[col] = joblib.load(scaler_path)
        else:
            st.error(f"Scaler for {col} not found at {scaler_path}")
    return loaded_scalers

# Function to preprocess user input
def preprocess_input(input_data, encoders, numeric_scalers):
    # Handle categorical variables
    for col, le in encoders.items():
        input_data[f'{col}_label'] = le.transform(input_data[col]).astype('int32')

    # Handle numeric variables (apply normalization)
    for col, scaler in numeric_scalers.items():
        input_data[f'{col}_normalized'] = scaler.transform(input_data[col].values.reshape(-1, 1)).flatten()

    return input_data



# Load the trained model
def load_model():
    model = xgb.Booster()
    model.load_model('xgboost_final_model.json')
    return model

# Load the dataset and extract unique values
def load_data():
    data_summary = json.load(open('data_summary.json', 'r'))

    return data_summary['unique_values'], data_summary['numeric_ranges']


# Main function to run the app
def main():
    st.title("Horse Racing Time Prediction App")

    # Load data and extract unique values
    unique_values, numeric_ranges = load_data()

    # Categorical variables
    şehir = st.selectbox("Select City", sorted(unique_values['Şehir']))
    mesafe = st.number_input(
        "Enter Race Distance (meters)",
        min_value=int(numeric_ranges['Mesafe']['min']),
        max_value=int(numeric_ranges['Mesafe']['max']),
        value=int(numeric_ranges['Mesafe']['min'])
    )
    pist_type = st.selectbox("Select Pist Type", sorted(unique_values['Pist_type']), help="Ç: Çim, K: Kum, S: Sentetik")
    pist_condition = st.selectbox("Select Pist Condition", sorted(unique_values['Pist_condition']))
    kcins = st.selectbox("Select Koşu Cinsi", sorted(unique_values['Kcins']))
    horse_name = st.selectbox("Select Horse Name", sorted(unique_values['Horse_name']))
    age = st.number_input(
        "Enter Horse Age (years)",
        min_value=int(numeric_ranges['Age']['min']),
        max_value=int(numeric_ranges['Age']['max']),
        value=int(numeric_ranges['Age']['min'])
    )
    irk = st.selectbox("Select Breed", ['İngiliz', 'Arap'])
    cinsiyet = st.selectbox("Select Gender", ['Erkek', 'Dişi'])
    jokey = st.selectbox("Select Jokey", sorted(unique_values['Jokey']))

    handikap = st.number_input(
        "Enter Handikap Score (0 if not applicable)",
        min_value=int(numeric_ranges['Handikap']['min']),
        max_value=int(numeric_ranges['Handikap']['max']),
        value=int(numeric_ranges['Handikap']['min'])
    )

    if st.button("Predict Race Time"):
        # Collect input data into a DataFrame
        input_data = {
            'Horse_name': horse_name,
            'Şehir': şehir,
            'Jokey': jokey,
            'Kcins': kcins,
            'Pist_type': pist_type,
            'Pist_condition': pist_condition,
            'Age': age,
            'Cinsiyet': cinsiyet,
            'Irk': irk,
            'Mesafe': mesafe,
            'Handikap': handikap
        }
        input_df = pd.DataFrame([input_data])

        # Load encoders
        categorical_columns = ['Horse_name', 'Şehir', 'Jokey', 'Kcins', 'Pist_type', 'Pist_condition']
        encoders = load_label_encoders('encoders', categorical_columns)

        # Encode binary variables
        input_df['Cinsiyet_binary'] = input_df['Cinsiyet'].map({'Erkek': 1, 'Dişi': 0})
        input_df['Irk_binary'] = input_df['Irk'].map({'İngiliz': 1, 'Arap': 0})  # Update mapping as needed

        # Load scalers for numeric features
        numeric_columns = ['Age', 'Mesafe', 'Handikap']
        numeric_scalers = load_scalers('scalers', numeric_columns)

        # Preprocess input data
        preprocessed_input = preprocess_input(input_df.copy(), encoders, numeric_scalers)

        # Prepare data for prediction
        feature_columns = [
            'Horse_name_label', 'Age_normalized', 'Cinsiyet_binary', 'Irk_binary', 'Mesafe_normalized',
            'Handikap_normalized', 'Pist_type_label', 'Pist_condition_label', 'Kcins_label', 'Şehir_label', 'Jokey_label'
        ]
        X = preprocessed_input[feature_columns]

        # Convert to DMatrix
        dmatrix = xgb.DMatrix(X, enable_categorical=True)

        # Load model
        model = load_model()

        # turn ms into minutes:seconds:milliseconds
        def convert_to_time(derece):
            minutes = int(derece / (60 * 1000))
            seconds = int((derece % (60 * 1000)) / 1000)
            milliseconds = int(derece % 1000)
            return f'{minutes}:{seconds:02d}.{milliseconds:03d}'

        # Make prediction
        prediction = model.predict(dmatrix)[0]

        # Convert the normalized prediction back to the original scale
        derece_ms_scaler: MinMaxScaler = joblib.load('scalers/Derece_ms_scaler.joblib')

        # Create a DataFrame with the same feature name
        prediction_df = pd.DataFrame({'Derece_ms_normalized': [prediction]})

        # Inverse transform to get the original scale
        predicted_derece = derece_ms_scaler.inverse_transform(prediction_df)[0][0]

        # Convert to human-readable format
        predicted_derece = convert_to_time(predicted_derece)

        st.write(f'The predicted race time (Derece) is: {predicted_derece}')


if __name__ == "__main__":
    main()
