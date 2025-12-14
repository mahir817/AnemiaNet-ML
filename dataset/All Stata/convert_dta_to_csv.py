"""
Convert all .dta (Stata) files in a folder to CSV format.
Handles large files efficiently and shows progress messages.
"""

import os
import pandas as pd
from pathlib import Path
import time


def convert_dta_to_csv(input_folder, output_folder=None):
    """
    Convert all .dta files in a folder (and subfolders) to CSV format.
    
    Parameters:
    -----------
    input_folder : str
        Path to the folder containing .dta files
    output_folder : str, optional
        Path to save CSV files. If None, saves in the same location as .dta files
    """
    input_path = Path(input_folder)
    
    # Find all .dta files recursively
    dta_files = list(input_path.rglob("*.dta")) + list(input_path.rglob("*.DTA"))
    
    if not dta_files:
        print(f"No .dta files found in {input_folder}")
        return
    
    print(f"Found {len(dta_files)} .dta file(s) to convert.\n")
    
    successful = 0
    failed = 0
    
    for dta_file in dta_files:
        try:
            # Get file size for info
            file_size_mb = dta_file.stat().st_size / (1024 * 1024)
            print(f"Processing: {dta_file.name} ({file_size_mb:.2f} MB)")
            
            # Read Stata file efficiently
            # Using chunksize=None for full read, but pandas handles memory efficiently
            start_time = time.time()
            df = pd.read_stata(dta_file, convert_categoricals=False, preserve_dtypes=False)
            read_time = time.time() - start_time
            
            # Determine output path
            if output_folder:
                output_path = Path(output_folder) / dta_file.relative_to(input_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)
            else:
                output_path = dta_file.parent / dta_file.stem
            
            # Save as CSV
            csv_file = output_path.with_suffix('.csv')
            start_time = time.time()
            
            # For large files, use chunking when writing CSV
            # pandas to_csv is already efficient, but we can optimize with index=False
            df.to_csv(csv_file, index=False, encoding='utf-8')
            write_time = time.time() - start_time
            
            # Success message
            csv_size_mb = csv_file.stat().st_size / (1024 * 1024)
            print(f"  [SUCCESS] Converted to: {csv_file.name}")
            print(f"    Rows: {len(df):,}, Columns: {len(df.columns)}")
            print(f"    CSV size: {csv_size_mb:.2f} MB")
            print(f"    Read time: {read_time:.2f}s, Write time: {write_time:.2f}s\n")
            
            successful += 1
            
        except Exception as e:
            print(f"  [FAILED] Could not convert {dta_file.name}")
            print(f"    Error: {str(e)}\n")
            failed += 1
    
    # Summary
    print("=" * 60)
    print(f"Conversion complete!")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Total: {len(dta_files)}")


if __name__ == "__main__":
    # Get the current directory or specify a folder
    import sys
    
    if len(sys.argv) > 1:
        folder_path = sys.argv[1]
    else:
        # Default to current directory
        folder_path = os.getcwd()
    
    # Optional: specify output folder as second argument
    output_path = sys.argv[2] if len(sys.argv) > 2 else None
    
    print(f"Converting .dta files in: {folder_path}")
    if output_path:
        print(f"Output folder: {output_path}")
    print("=" * 60 + "\n")
    
    convert_dta_to_csv(folder_path, output_path)

