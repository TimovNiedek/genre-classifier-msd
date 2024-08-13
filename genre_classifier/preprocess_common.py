import pandas as pd
import numpy as np


def fix_outliers(
    df: pd.DataFrame, valid_tempo_min: float = 70, valid_tempo_max: float = 180
) -> pd.DataFrame:
    df["year"] = df["year"].replace(0, np.nan)
    df.loc[df["tempo"] < valid_tempo_min / 2, "tempo"] = np.nan
    df.loc[df["tempo"] > valid_tempo_max * 2, "tempo"] = np.nan
    df.loc[df["tempo"] < valid_tempo_min, "tempo"] *= 2
    df.loc[df["tempo"] > valid_tempo_max, "tempo"] /= 2
    return df
