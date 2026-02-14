
import numpy as np

def detect_zscore_anomaly(series):
    mean = series.mean()
    std = series.std()
    if std == 0:
        return [False] * len(series)
    z = (series - mean) / std
    return abs(z) > 2
