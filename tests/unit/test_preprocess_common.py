import pandas as pd
import numpy as np
from genre_classifier.preprocess_common import fix_outliers


class TestPreprocessCommon:
    def test_fix_outliers(self):
        # Create a sample DataFrame with outliers
        data = {"year": [2000, 0, 1999, 2020], "tempo": [60, 400, 90, 50]}
        df = pd.DataFrame(data)

        # Expected DataFrame after fixing outliers
        expected_data = {
            "year": [2000, np.nan, 1999, 2020],
            "tempo": [120, np.nan, 90, 100],
        }
        expected_df = pd.DataFrame(expected_data)

        # Call the function
        result_df = fix_outliers(df)

        # Verify the results
        pd.testing.assert_frame_equal(result_df, expected_df)
