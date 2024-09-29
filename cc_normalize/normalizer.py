
def normalize_data(data):
    normalized = {}
    for key, value in data.items():
        if key == 'Gender':
            normalized[key] = 1 if value.lower() == 'male' else 0
        elif key == 'Age':
            normalized[key] = (value - 18) / (100 - 18)  # Assuming age range 18-100
        elif key == 'Avg_Account_Balance':
            normalized[key] = value / 1000000  # Assuming max balance of 1,000,000
        elif key in ['Region_Code', 'Occupation', 'Channel_Code', 'Credit_Product']:
            # For categorical variables, you might want to use one-hot encoding
            # This is a simplified version
            normalized[key] = hash(value) % 10 / 10
        elif key == 'Is_Active':
            normalized[key] = 1 if value else 0
        else:
            normalized[key] = value  # Keep other values as is
    return normalized