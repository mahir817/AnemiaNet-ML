"""
Extract column names from a CSV file and save them to a text file.
"""

import pandas as pd
from pathlib import Path


def extract_column_names(csv_file, output_file=None):
    """
    Extract column names from a CSV file and save to a text file.
    
    Parameters:
    -----------
    csv_file : str or Path
        Path to the CSV file
    output_file : str or Path, optional
        Path to output text file. If None, uses CSV filename with .txt extension
    """
    csv_path = Path(csv_file)
    
    if not csv_path.exists():
        print(f"File not found: {csv_file}")
        return
    
    # Read just the header to get column names
    print(f"Reading column names from: {csv_path.name}")
    df = pd.read_csv(csv_path, nrows=0)  # Read only header
    
    column_names = df.columns.tolist()
    
    # Determine output file path
    if output_file:
        output_path = Path(output_file)
    else:
        output_path = csv_path.parent / f"{csv_path.stem}_columns.txt"
    
    # Write column names to text file
    with open(output_path, 'w', encoding='utf-8') as f:
        for i, col in enumerate(column_names, 1):
            f.write(f"{i}. {col}\n")
    
    print(f"  Extracted {len(column_names)} column names")
    print(f"  Saved to: {output_path.name}")
    print(f"  Full path: {output_path}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        # Default to Individual Recode CSV
        csv_file = Path("Individual Recode") / "BDIR61FL.csv"
    
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    extract_column_names(csv_file, output_file)


