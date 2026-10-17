import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta

def main():
    print(f"--- Starting Bulk NSE Data Download: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    os.makedirs("data", exist_ok=True)
    
    try:
        print("Reading EQUITY_L.csv...")
        df = pd.read_csv("EQUITY_L.csv")
        df.columns = df.columns.str.strip()
        if 'SERIES' in df.columns:
            df = df[df['SERIES'] == 'EQ']
        symbols = df['SYMBOL'].tolist()
        tickers = [f"{sym}.NS" for sym in symbols]
        print(f"Found {len(tickers)} regular equity (EQ) stocks to track.")
    except FileNotFoundError:
        print("Error: EQUITY_L.csv not found! Please place it in the same folder as this script.")
        return

    today = datetime.today()
    three_years_ago = today - timedelta(days=3*365)
    
    existing_files = os.listdir("data")
    if len(existing_files) > 100:  
        print("Existing data detected. Performing fast incremental update (last 7 days)...")
        start_date = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    else:
        print("No significant existing data found. Performing initial full 3-year download...")
        start_date = three_years_ago.strftime('%Y-%m-%d')
        
    end_date = today.strftime('%Y-%m-%d')
    batch_size = 100 
    
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1} of {(len(tickers)//batch_size) + 1}...")
        
        data = yf.download(batch, start=start_date, end=end_date, group_by="ticker", threads=True, progress=False)
        
        for ticker in batch:
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    if ticker not in data.columns.get_level_values(0):
                        continue
                    df_ticker = data[ticker].dropna(how='all')
                else:
                    df_ticker = data.dropna(how='all')
                
                if df_ticker.empty:
                    continue
                    
                df_ticker.index.name = "Date"
                
                if all(col in df_ticker.columns for col in ['Open', 'High', 'Low', 'Close', 'Volume']):
                    df_ticker = df_ticker[['Open', 'High', 'Low', 'Close', 'Volume']]
                
                file_path = f"data/{ticker.replace('.NS', '')}_data.csv"
                
                if os.path.exists(file_path):
                    existing_df = pd.read_csv(file_path, index_col="Date", parse_dates=True)
                    merged_df = pd.concat([existing_df, df_ticker])
                    merged_df = merged_df[~merged_df.index.duplicated(keep='last')]
                else:
                    merged_df = df_ticker
                    
                merged_df = merged_df[merged_df.index >= pd.to_datetime(three_years_ago)]
                merged_df.to_csv(file_path)
                
            except Exception as e:
                pass
                
    print("--- Bulk Update Complete ---")

if __name__ == "__main__":
    main()
