import pandas as pd


def format_sep_data(sep_data, actions_data):
    adj_factor = sep_data["closeadj"] / sep_data["close"]

    # Calculate the adjusted values
    sep_data["adj_open"] = sep_data["open"] * adj_factor
    sep_data["adj_high"] = sep_data["high"] * adj_factor
    sep_data["adj_low"] = sep_data["low"] * adj_factor
    sep_data["adj_close"] = sep_data["closeadj"]

    # Filter dividends and splits
    dividends = actions_data.loc[actions_data["action"] == "dividend", "value"]
    splits = actions_data.loc[actions_data["action"] == "split", "value"]

    # Merge the dividends and splits into the SEP data
    sep_data = sep_data.join(dividends.rename("ex-dividend"), on='ticker', how="left", rsuffix='_div')
    sep_data = sep_data.join(splits.rename("split_ratio"), on='ticker', how="left", rsuffix='_split')

    # Fill missing values for new columns
    sep_data["ex-dividend"].fillna(0, inplace=True)
    sep_data["split_ratio"].fillna(1, inplace=True) 
    sep_data["adj_volume"] = sep_data["volume"]

    # Select only required columns
    update_final = sep_data.reset_index()[
        [
            "ticker",
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "ex-dividend",
            "split_ratio",
            "adj_open",
            "adj_high",
            "adj_low",
            "adj_close",
            "adj_volume",
        ]
    ]

    update_final.dropna(subset=["ticker"], inplace=True)
    update_final = update_final.set_index(['date', 'ticker']).sort_index()

    return update_final