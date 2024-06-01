import pandas as pd

def format_stock_metadata(tickers_data, sep_data, daily_data):
    tickers_data["last_sale"] = tickers_data["ticker"].apply(
        lambda ticker: sep_data.loc[ticker, 'close'] if ticker in sep_data.index else None
    )
    tickers_data['marketcap'] = tickers_data['ticker'].apply(
        lambda x: daily_data.loc[x, 'marketcap'] if x in daily_data.index else None
    )
    # tickers_data["marketcap"] = daily_data["marketcap"].astype(float)
    tickers_data["ipoyear"] = tickers_data["firstpricedate"].dt.year.astype(float)



    stock_data = tickers_data[
        [
            "ticker",
            "name",
            "last_sale",
            "marketcap",
            "ipoyear",
            "sector",
            "industry"
        ]
    ]

    # stock_data = stock_data.dropna(subset=['marketcap'])
    # stock_data.dropna(subset=["ticker"], inplace=True)
    # stock_data.reset_index(drop=True, inplace=True)

    stock_data = stock_data.dropna(subset=['marketcap'])
    stock_data = stock_data.reset_index(drop=True)
    stock_data = stock_data.set_index('ticker').sort_index()

    print(stock_data.info())