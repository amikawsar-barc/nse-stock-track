import yfinance as yf
import pandas as pd
import os
import json
from datetime import datetime, timedelta


def main():
    print(
        f"--- Starting Bulk NSE Data Download: "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---"
    )

    os.makedirs("data", exist_ok=True)

    try:
        print("Reading EQUITY_L.csv...")
        df = pd.read_csv("EQUITY_L.csv")
        df.columns = df.columns.str.strip()

        if "SERIES" in df.columns:
            df = df[df["SERIES"] == "EQ"]

        symbols = df["SYMBOL"].dropna().astype(str).str.strip().tolist()
        tickers = [f"{sym}.NS" for sym in symbols]
        print(f"Found {len(tickers)} regular equity (EQ) stocks to track.")
    except FileNotFoundError:
        print(
            "Error: EQUITY_L.csv not found! "
            "Please place it in the same folder as this script."
        )
        return
    except (KeyError, pd.errors.ParserError) as exc:
        print(f"Error reading EQUITY_L.csv: {exc}")
        return

    today = datetime.today()
    three_years_ago = today - timedelta(days=5 * 365)

    existing_files = [
        name for name in os.listdir("data")
        if name.endswith("_data.csv")
    ]

    if len(existing_files) > 100:
        print("Existing data detected. Performing fast incremental update (last 7 days)...")
        start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    else:
        print("No significant existing data found. Performing initial full 3-year download...")
        start_date = three_years_ago.strftime("%Y-%m-%d")

    end_date = (today + timedelta(days=1)).strftime('%Y-%m-%d')  #today.strftime("%Y-%m-%d")
    batch_size = 100
    market_caps = {}

    total_batches = (len(tickers) + batch_size - 1) // batch_size

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        print(f"Processing batch {i // batch_size + 1} of {total_batches}...")

        try:
            data = yf.download(
                batch,
                start=start_date,
                end=end_date,
                group_by="ticker",
                threads=True,
                progress=False,
                auto_adjust=False,
            )
        except Exception as exc:
            print(f"Batch download failed: {exc}")
            continue

        for ticker in batch:
            clean_sym = ticker.removesuffix(".NS")

            # Fetch market capitalization.
            try:
                info = yf.Ticker(ticker).fast_info
                market_caps[clean_sym] = info.get("marketCap")
            except Exception:
                market_caps[clean_sym] = 0

            # Process historical price and volume data.
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    if ticker not in data.columns.get_level_values(0):
                        continue
                    df_ticker = data[ticker].dropna(how="all")
                else:
                    df_ticker = data.dropna(how="all")

                if df_ticker.empty:
                    print(f"No historical data returned for {ticker}.")
                    continue

                df_ticker.index.name = "Date"
                expected_columns = ["Open", "High", "Low", "Close", "Volume"]
                if all(col in df_ticker.columns for col in expected_columns):
                    df_ticker = df_ticker[expected_columns]

                file_path = os.path.join("data", f"{clean_sym}_data.csv")

                if os.path.exists(file_path):
                    existing_df = pd.read_csv(
                        file_path, index_col="Date", parse_dates=True
                    )
                    existing_df.index = pd.to_datetime(existing_df.index)
                    merged_df = pd.concat([existing_df, df_ticker])
                    merged_df = merged_df[
                        ~merged_df.index.duplicated(keep="last")
                    ].sort_index()
                else:
                    merged_df = df_ticker.sort_index()

                merged_df = merged_df[
                    merged_df.index >= pd.to_datetime(three_years_ago)
                ]
                merged_df.to_csv(file_path)
            except Exception as exc:
                print(f"Could not process {ticker}: {exc}")

    # Save market caps to one JSON file.
    mcap_file = os.path.join("data", "market_caps.json")
    with open(mcap_file, "w", encoding="utf-8") as f:
        json.dump(market_caps, f, indent=4)

    print(f"Saved market caps to {mcap_file}")
    print("--- Bulk Update Complete ---")


if __name__ == "__main__":
    main()
