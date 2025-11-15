# Import required libraries
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
import os

class FinancialDataFetcher:
    """
    A class to fetch and manage financial data for tickers
    """
    
    def __init__(self, equity_file='yf_data.csv'):
        self.equity_file = equity_file
        self.numeric_columns = [
            'Current Price', 'Net Income', 'Shares Outstanding', 'Market Cap', 
            'P/E Ratio (TTM)', 'Earnings Growth', 'Dividend Yield', 
            '5 Yrs Avg Dividend Yield', 'Profit Margin', 'Return on Equity', 
            'Debt to Equity', 'Free Cash Flow', 'Total Revenue', 'Gross Profits',
            'ROA', 'ROE', 'EPS'
        ]

    # Function to parse user input and extract ticker symbols
    def get_tickers(self, user_input):
        """
        Parse user input to extract ticker symbols.
        Supports single ticker or CSV file with tickers.
        """
        if user_input.endswith('.csv'):
            try:
                df = pd.read_csv(user_input)
                if 'Ticker' in df.columns:
                    return df['Ticker'].tolist(), user_input
                else:
                    return df.iloc[:, 0].tolist(), user_input
            except Exception as e:
                print(f"Error reading CSV file: {e}")
                return [], user_input
        else:
            # Handle multiple tickers separated by commas
            tickers = [ticker.strip().upper() for ticker in user_input.split(',')]
            return tickers, user_input

    # Function to fetch info financial_data
    def get_ticker_info(self, ticker_symbol):
        try:
            ticker = yf.Ticker(ticker_symbol)
            info_data = ticker.info
            
            # Extract key financial metrics
            financial_data = {
                'Ticker': ticker_symbol,
                'Security Type': info_data.get('quoteType', 'N/A'),
                'Exchange': info_data.get('exchange', 'N/A'),
                'Country': info_data.get('country', 'N/A'),
                'Sector': info_data.get('sector', 'N/A'),
                'Current Price': info_data.get('currentPrice'),
                'Shares Outstanding': info_data.get('sharesOutstanding'),
                'Market Cap': info_data.get('marketCap'),
                'Net Income': info_data.get('netIncomeToCommon'),
                'EPS': info_data.get('epsTrailingTwelveMonths'),
                'P/E Ratio (TTM)': info_data.get('trailingPE'),
                'BVPS': info_data.get('bookValue'),
                'P/B Ratio': info_data.get('priceToBook'),
                'Earnings Growth': info_data.get('earningsGrowth'),
                'Revenue Growth': info_data.get('revenueGrowth'),
                'Dividend Yield': info_data.get('dividendYield'),
                'Gross Margin': info_data.get('grossMargins'),
                'Profit Margin': info_data.get('profitMargins'),
                'Debt to Equity': info_data.get('debtToEquity'),
                'Free Cash Flow': info_data.get('freeCashflow'),
                'Current Ratio': info_data.get('currentRatio'),
                'Quick Ratio': info_data.get('quickRatio'),
                'ROE': info_data.get('returnOnEquity'),
                'ROA': info_data.get('returnOnAssets'),
            }
            
            # Convert to Series
            info_series = pd.Series(financial_data)
            return info_series
            
        except Exception as e:
            print(f"Error fetching data for {ticker_symbol}: {e}")
            return pd.Series({'Ticker': ticker_symbol, 'Error': str(e)})

    def _convert_to_numeric(self, df):
        """Convert numeric columns to appropriate data types"""
        for col in self.numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Handle infinite values by replacing them with NaN
                # Then you can choose to fill with a specific value if needed
                df[col] = df[col].replace([np.inf, -np.inf], np.nan)
                
        return df

    def _update_csv_file(self, new_data, filename):
        """Helper function to update CSV files with new data"""
        if os.path.exists(filename):
            existing_df = pd.read_csv(filename, index_col='Ticker')
            existing_df = self._convert_to_numeric(existing_df)
            
            # Remove existing entries for the tickers we're updating
            existing_df = existing_df[~existing_df.index.isin(new_data.index)]
            
            # Concatenate the old data (without the updated tickers) with the new data
            combined_df = pd.concat([existing_df, new_data])
            combined_df.to_csv(filename, index=True)
            print(f"Updated {filename} with {len(new_data)} equity records")
        else:
            new_data.to_csv(filename, index=True)
            print(f"Created new {filename} file with {len(new_data)} equity records")

    # Function to fetch and save data for all tickers
    def fetch_and_save_ticker_data(self, tickers, input_filename):
        equity_data = []
        non_equity_tickers = []  # Just store non-equity ticker names for display
        failed_tickers = []
        
        for ticker in tickers:
            data = self.get_ticker_info(ticker)
            
            # Check if there was an error fetching data
            if 'Error' in data:
                failed_tickers.append(ticker)
                print(f"Failed to process {ticker}: {data['Error']}")
                continue
                
            security_type = data.get('Security Type', 'N/A')
            
            if security_type == 'EQUITY':
                equity_data.append(data)
            else:
                non_equity_tickers.append(ticker)
                print(f"  - {ticker} is a {security_type} (non-equity), skipping save")
        
        # Process Equity data only
        if equity_data:
            equity_df = pd.DataFrame(equity_data)
            equity_df.set_index('Ticker', inplace=True)
            equity_df = self._convert_to_numeric(equity_df)
            self._update_csv_file(equity_df, self.equity_file)
        
        # Display non-equity tickers (for display only, not returned)
        if non_equity_tickers:
            print(f"\nNon-equity securities found ({len(non_equity_tickers)} tickers):")
            for ticker in non_equity_tickers:
                print(f"  - {ticker}")
            print("Note: Non-equity securities are displayed but not saved to the database.")
        
        # Print summary of failed tickers
        if failed_tickers:
            print(f"\nFailed to process {len(failed_tickers)} tickers:")
            for ticker in failed_tickers:
                print(f"  - {ticker}")
        else:
            print("\nAll tickers processed successfully!")
        
        return equity_data, failed_tickers

    def get_financial_data(self, user_input=None):
        """
        Main function to get financial data for tickers
        Can be called with user_input or will prompt for input
        """
        if user_input is None:
            user_input = input("Enter ticker symbol or CSV filename containing tickers: ").strip()
        
        tickers, input_filename = self.get_tickers(user_input)
        if not tickers:
            print("No valid tickers found. Please check your input.")
            return None, []
        
        print(f"\nProcessing {len(tickers)} tickers: {', '.join(tickers)}")
        financial_data, failed_tickers = self.fetch_and_save_ticker_data(tickers, input_filename)
        
        # Print final summary
        successful_equity_count = len(tickers) - len(failed_tickers)
        print(f"\nProcessing complete!")
        print(f"Successfully processed equity: {successful_equity_count} tickers")
        print(f"Failed to process: {len(failed_tickers)} tickers")
        
        return financial_data, failed_tickers

    def get_single_ticker_data(self, ticker_symbol):
        """Get data for a single ticker without saving to CSV"""
        return self.get_ticker_info(ticker_symbol)

    def get_multiple_tickers_data(self, ticker_list):
        """Get data for multiple tickers without saving to CSV"""
        results = []
        failed_tickers = []
        
        for ticker in ticker_list:
            data = self.get_ticker_info(ticker)
            if 'Error' in data:
                failed_tickers.append(ticker)
            else:
                results.append(data)
        
        return results, failed_tickers

# Standalone functions for backward compatibility
def get_tickers(user_input):
    fetcher = FinancialDataFetcher()
    return fetcher.get_tickers(user_input)

def get_ticker_info(ticker_symbol):
    fetcher = FinancialDataFetcher()
    return fetcher.get_ticker_info(ticker_symbol)

def fetch_and_save_ticker_data(tickers, input_filename):
    fetcher = FinancialDataFetcher()
    return fetcher.fetch_and_save_ticker_data(tickers, input_filename)

def main(csv_filename=None):
    """
    Main function that can be called directly
    """
    fetcher = FinancialDataFetcher()
    
    if csv_filename:
        user_input = csv_filename
    else:
        user_input = None
    
    return fetcher.get_financial_data(user_input)

# Make the module callable by adding this function
def get_yf_data(csv_filename=None):
    """
    Top-level function that makes the module callable
    """
    return main(csv_filename)

# Execute the code
if __name__ == "__main__":
    financial_data, failed_tickers = main()