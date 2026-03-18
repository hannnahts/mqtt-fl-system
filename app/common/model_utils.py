import pandas as pd
import numpy as np

FEATURE_COLUMNS = ["PM10", "NO", "NO2", "O3"]
TARGET_COLUMN = "PM25"


def train_local_model(
    csv_file,
    initial_model=None,
    local_epochs=5,
    learning_rate=0.0002,
):
    df = pd.read_csv(csv_file)
    X = df[FEATURE_COLUMNS].to_numpy(dtype=float)
    y = df[TARGET_COLUMN].to_numpy(dtype=float)
    num_samples, num_features = X.shape

    if initial_model is None:
        weights = np.zeros(num_features, dtype=float)
        bias = 0.0
        initial_round = 0
    else:
        weights = np.array(initial_model["weights"], dtype=float)
        bias = float(initial_model["bias"])
        initial_round = int(initial_model.get("round", 0))

    for _ in range(local_epochs):
        predictions = X @ weights + bias
        errors = predictions - y

        gradient_w = (2.0 / num_samples) * (X.T @ errors)
        gradient_b = float((2.0 / num_samples) * errors.sum())

        weights -= learning_rate * gradient_w
        bias -= learning_rate * gradient_b

    return weights.tolist(), float(bias), initial_round


def build_initial_model(feature_count):
    return {
        "weights": [0.0] * feature_count,
        "bias": 0.0,
        "round": 0,
    }
