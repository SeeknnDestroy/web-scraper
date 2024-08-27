import streamlit as st
import pandas as pd
import xgboost as xgb
import joblib
from sklearn.preprocessing import MinMaxScaler, LabelEncoder

# Load the trained model and scaler
model = joblib.load('xgboost_model.pkl')

scaler: MinMaxScaler = joblib.load('scaler.pkl')

# Define a manual mapping of cities to labels
city_mapping = {
    'ABD': 0,
    'Abu Dhabi Birleşik Arap Emirlikleri': 1,
    'Adana': 2,
    'Ankara': 3,
    'Antalya': 4,
    'Bursa': 5,
    'Churchill Downs ABD': 6,
    'Deauville Fransa': 7,
    'Diyarbakır': 8,
    'Elazığ': 9,
    'Kempton Park Birleşik Krallık': 10,
    'Kocaeli': 11,
    'Meydan Dubai': 12,
    'Santa Anita Park ABD': 13,
    'İstanbul': 14,
    'İzmir': 15,
    'Şanlıurfa': 16
}

# Function to preprocess the inputs
def preprocess_input(city, mesafe, age, irk, cinsiyet, condition):
    # Manually map the city to its encoded value
    city_encoded = city_mapping[city]
    
    # Normalize Mesafe and Age
    normalized_features = scaler.transform([[mesafe, age, 0]])  # We don't use Derece_ms for scaling, so set to 0
    mesafe_normalized = normalized_features[0][0]
    age_normalized = normalized_features[0][1]

    # Encode Irk and Cinsiyet
    irk_binary = 1 if irk == "İngiliz" else 0
    cinsiyet_binary = 1 if cinsiyet == "Erkek" else 0

    # Encode Conditions
    condition_cols = [f'Condition_{c}' for c in ['aa', 'ad', 'ae', 'ag', 'ak', 'da', 'dd', 'de', 'dg', 'dk', 'ka', 'kd', 'ke', 'kg', 'kk', 'ya', 'yk']]
    condition_encoded = {col: 0 for col in condition_cols}
    condition_encoded[f'Condition_{condition}'] = 1

    # Combine all inputs into a single DataFrame
    input_data = {
        'Age_normalized': age_normalized,
        'Cinsiyet_binary': cinsiyet_binary,
        'Irk_binary': irk_binary,
        'City_label': city_encoded,
        'Mesafe_normalized': mesafe_normalized,
        **condition_encoded
    }

    input_df = pd.DataFrame([input_data])
    return input_df

# Streamlit interface
st.title("Horse Racing Time Prediction App")

# User inputs
city = st.selectbox("Select City", list(city_mapping.keys()))
mesafe = st.number_input("Enter Race Distance (meters)", min_value=800, max_value=3400, value=1400)
age = st.number_input("Enter Horse Age (years)", min_value=2, max_value=57, value=4)
irk = st.selectbox("Select Breed", options=["Arap", "İngiliz"])
cinsiyet = st.selectbox("Select Gender", options=["Dişi", "Erkek"])
condition = st.selectbox("Select Condition", options=['aa', 'ad', 'ae', 'ag', 'ak', 'da', 'dd', 'de', 'dg', 'dk', 'ka', 'kd', 'ke', 'kg', 'kk', 'ya', 'yk'])


# Display the prediction
if st.button("Predict"):
    # Process the input
    processed_input = preprocess_input(city, mesafe, age, irk, cinsiyet, condition)

    # Convert the input to DMatrix for prediction
    dmat_input = xgb.DMatrix(processed_input)

    # Predict the race time in milliseconds
    predicted_derece_ms_normalized = model.predict(dmat_input)[0]

    # Inverse transform to original scale
    normalized_values = [[mesafe, age, predicted_derece_ms_normalized]]
    predicted_derece_ms = scaler.inverse_transform(normalized_values)[0][2]


    # Convert to minutes:seconds:milliseconds format
    def convert_to_time(derece):
        minutes = int(derece / (60 * 1000))
        seconds = int((derece % (60 * 1000)) / 1000)
        milliseconds = int(derece % 1000)
        return f'{minutes}:{seconds:02d}.{milliseconds:03d}'

    predicted_time = convert_to_time(predicted_derece_ms)
    st.write(f'The predicted race time (Derece) is: {predicted_time}')