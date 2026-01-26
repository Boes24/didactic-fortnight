from sklearn.preprocessing import MinMaxScaler
import pandas as pd

# Initialize the scaler globally (outside the function)
scaler = MinMaxScaler()

def scale_training_data(data_set):
    """
    Scale the training dataset using MinMaxScaler.
    Fit the scaler on the training data and transform it.
    """
    global scaler  # Use the global scaler
    scaled_features = scaler.fit_transform(data_set)  # Fit and transform training data
    scaled_data = pd.DataFrame(scaled_features, columns=data_set.columns)
    return scaled_data

def scale_prediction_data(data_set):
    """
    Scale the prediction dataset using the previously fitted scaler.
    Only transform the data.
    """
    global scaler  # Use the global scaler
    scaled_features = scaler.transform(data_set)  # Transform only
    scaled_data = pd.DataFrame(scaled_features, columns=data_set.columns)
    return scaled_data