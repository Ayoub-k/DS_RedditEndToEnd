"""For transfrom data"""

import numpy as np
from scipy.stats import skew

def best_outlier_method(data):
    """
    Finds the best method for handling outliers in a pandas Series based on the skewness of the distribution.
    Returns the name of the best method and any additional parameters required by the method.
    """
    skewness = skew(data)
    
    # If the distribution is approximately symmetric, use Z-score method
    if abs(skewness) < 0.5:
        return 'zscore', 3
    
    # If the distribution is highly skewed, use Winsorization
    elif abs(skewness) >= 0.5 and abs(skewness) < 1:
        q1, q3 = np.percentile(data, [25, 75])
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        return 'winsor', (lower_bound, upper_bound)
    
    # If the distribution is very highly skewed, use log transformation
    else:
        return 'log', None
