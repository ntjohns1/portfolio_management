> #### Data Ingestion Service
> - **Function**: Ingests new market data from Sharadar, Alpaca's API and yfinance, processes the data, and stores it in HDF5 and Parquet formats. Also ingests any other > ML training data like fundamentals or news.
> - **Technologies**: Python, Pandas, HDF5, Parquet, REST APIs

Todo: 

 - automate collecting minute data from 
 - automate compression and storage of .h5 files (unload to drive)