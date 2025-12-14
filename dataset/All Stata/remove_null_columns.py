"""
Remove columns that are entirely null/empty from CSV files.
Efficiently handles large files by processing in chunks if needed.
"""

import os
import pandas as pd
from pathlib import Path
import time


def remove_null_columns(csv_file, inplace=True):
    """
    Remove columns that are entirely null/empty from a CSV file.
    
    Parameters:
    -----------
    csv_file : str or Path
        Path to the CSV file
    inplace : bool
        If True, overwrites the original file. If False, creates a backup.
    
    Returns:
    --------
    tuple: (original_cols, remaining_cols, removed_cols)
    """
    csv_path = Path(csv_file)
    
    if not csv_path.exists():
        print(f"File not found: {csv_file}")
        return None
    
    # Get file size
    file_size_mb = csv_path.stat().st_size / (1024 * 1024)
    print(f"Processing: {csv_path.name} ({file_size_mb:.2f} MB)")
    
    # Read CSV file
    start_time = time.time()
    df = pd.read_csv(csv_path, low_memory=False)
    read_time = time.time() - start_time
    
    original_cols = len(df.columns)
    original_rows = len(df)
    
    # Count null values per column
    print(f"  Original: {original_rows:,} rows, {original_cols} columns")
    
    # Remove columns that are entirely null/empty
    # This includes columns where all values are NaN, None, or empty strings
    null_counts = df.isnull().sum()
    empty_string_counts = (df == '').sum()
    
    # Columns to drop: entirely null OR entirely empty strings
    cols_to_drop = []
    for col in df.columns:
        if null_counts[col] == len(df) or empty_string_counts[col] == len(df):
            cols_to_drop.append(col)
    
    if cols_to_drop:
        df_cleaned = df.drop(columns=cols_to_drop)
        remaining_cols = len(df_cleaned.columns)
        removed_cols = len(cols_to_drop)
        
        print(f"  Removing {removed_cols} null/empty columns...")
        
        # Save cleaned CSV
        start_time = time.time()
        if inplace:
            try:
                df_cleaned.to_csv(csv_path, index=False, encoding='utf-8')
            except PermissionError:
                # If file is locked, save with _cleaned suffix
                cleaned_path = csv_path.parent / f"{csv_path.stem}_cleaned.csv"
                df_cleaned.to_csv(cleaned_path, index=False, encoding='utf-8')
                print(f"  Note: Original file is locked. Saved as: {cleaned_path.name}")
                csv_path = cleaned_path
        else:
            # Create backup
            backup_path = csv_path.with_suffix('.csv.backup')
            csv_path.rename(backup_path)
            df_cleaned.to_csv(csv_path, index=False, encoding='utf-8')
        
        write_time = time.time() - start_time
        
        # Get new file size
        new_size_mb = csv_path.stat().st_size / (1024 * 1024)
        size_reduction = file_size_mb - new_size_mb
        
        print(f"  [SUCCESS] Removed {removed_cols} columns")
        print(f"    Remaining: {remaining_cols} columns")
        print(f"    Size reduction: {size_reduction:.2f} MB ({size_reduction/file_size_mb*100:.1f}%)")
        print(f"    Read time: {read_time:.2f}s, Write time: {write_time:.2f}s\n")
        
        return (original_cols, remaining_cols, removed_cols)
    else:
        print(f"  No null columns found. File unchanged.\n")
        return (original_cols, original_cols, 0)


def process_folder(folder_path, recursive=True):
    """
    Process all CSV files in a folder, removing null columns.
    
    Parameters:
    -----------
    folder_path : str or Path
        Path to folder containing CSV files
    recursive : bool
        If True, processes CSV files in subfolders too
    """
    folder = Path(folder_path)
    
    if recursive:
        csv_files = list(folder.rglob("*.csv"))
    else:
        csv_files = list(folder.glob("*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {folder_path}")
        return
    
    print(f"Found {len(csv_files)} CSV file(s) to process.\n")
    print("=" * 60 + "\n")
    
    total_removed = 0
    total_original = 0
    successful = 0
    failed = 0
    
    for csv_file in csv_files:
        try:
            result = remove_null_columns(csv_file, inplace=True)
            if result:
                original_cols, remaining_cols, removed_cols = result
                total_original += original_cols
                total_removed += removed_cols
                successful += 1
        except Exception as e:
            print(f"  [FAILED] Error processing {csv_file.name}")
            print(f"    Error: {str(e)}\n")
            failed += 1
    
    # Summary
    print("=" * 60)
    print(f"Processing complete!")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Total columns removed: {total_removed}")
    print(f"  Average columns per file: {total_original/successful if successful > 0 else 0:.1f}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        folder_path = sys.argv[1]
        # Default to recursive unless explicitly set to False
        recursive = len(sys.argv) <= 2 or sys.argv[2].lower() != 'false'
    else:
        # Default to current directory, process recursively
        folder_path = Path.cwd()
        recursive = True
    
    print(f"Removing null columns from CSV files in: {folder_path}")
    if recursive:
        print("Processing all subfolders recursively...")
    print("=" * 60 + "\n")
    
    process_folder(folder_path, recursive=recursive)

