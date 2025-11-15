import os
import time
import pandas as pd
from yf_to_db import main  # Import the main function from your script

def process_multiple_csv_files(csv_folder_path, delay_minutes=2):
    """
    Process multiple CSV files containing tickers with delays between each file
    
    Args:
        csv_folder_path (str): Path to the folder containing CSV files
        delay_minutes (int): Delay in minutes between processing each CSV file
    """
    # Get all CSV files in the specified folder
    csv_files = [f for f in os.listdir(csv_folder_path) if f.endswith('.csv')]
    
    if not csv_files:
        print(f"No CSV files found in {csv_folder_path}")
        return
    
    print(f"Found {len(csv_files)} CSV files to process")
    
    # Process each CSV file
    for i, csv_file in enumerate(csv_files):
        csv_path = os.path.join(csv_folder_path, csv_file)
        
        print(f"\n{'='*50}")
        print(f"Processing file {i+1}/{len(csv_files)}: {csv_file}")
        print(f"{'='*50}")
        
        # Process the current CSV file
        try:
            financial_data, failed_tickers = main(csv_path)
            
            # Print summary for this file
            print(f"\nSummary for {csv_file}:")
            print(f"Successfully processed: {len(financial_data) - len(failed_tickers)} tickers")
            print(f"Failed to process: {len(failed_tickers)} tickers")
            
        except Exception as e:
            print(f"Error processing {csv_file}: {e}")
        
        # Add delay between files (except after the last file)
        if i < len(csv_files) - 1:
            delay_seconds = delay_minutes * 60
            print(f"\nWaiting {delay_minutes} minutes before processing next file...")
            time.sleep(delay_seconds)

if __name__ == "__main__":
    # Specify the path to your folder containing CSV files
    csv_folder = "input_csv"  # Update this path as needed
    
    # Process all CSV files with 5-minute delays
    process_multiple_csv_files(csv_folder, delay_minutes=5)