"""
Script to analyze CSV columns and find potential primary key candidates
"""
import pandas as pd
from pathlib import Path

CSV_PATH = "./data/raw/covid-data.csv"

def analyze_columns_for_primary_key(csv_path, sample_size=None):
    """
    Analyze CSV columns to find potential primary key candidates
    
    Args:
        csv_path: Path to CSV file
        sample_size: Number of rows to analyze (None = all rows)
    """
    print(f"Analyzing CSV: {csv_path}")
    print("=" * 80)
    
    # Read CSV (sample or full)
    if sample_size:
        df = pd.read_csv(csv_path, nrows=sample_size)
        print(f"Analyzing sample of {sample_size} rows\n")
    else:
        df = pd.read_csv(csv_path)
        print(f"Analyzing all {len(df)} rows\n")
    
    total_rows = len(df)
    
    # Column analysis
    print(f"{'Column Name':<40} {'Nulls':<10} {'Null %':<10} {'Unique':<12} {'Can be PK?'}")
    print("-" * 80)
    
    results = []
    
    for col in df.columns:
        null_count = df[col].isna().sum()
        null_pct = (null_count / total_rows) * 100
        unique_count = df[col].nunique()
        
        # Check if can be primary key (no nulls)
        can_be_pk = "✓ YES" if null_count == 0 else "✗ NO"
        
        results.append({
            'column': col,
            'null_count': null_count,
            'null_pct': null_pct,
            'unique_count': unique_count,
            'can_be_pk': null_count == 0
        })
        
        print(f"{col:<40} {null_count:<10} {null_pct:<10.2f} {unique_count:<12} {can_be_pk}")
    
    # Find potential composite keys
    print("\n" + "=" * 80)
    print("COMPOSITE KEY ANALYSIS")
    print("=" * 80)
    
    # Common composite key combinations
    composite_candidates = [
        ["REPORT_DATE", "COUNTY_FIPS_NUMBER"],
        ["REPORT_DATE", "PROVINCE_STATE_NAME"],
        ["REPORT_DATE", "COUNTY_NAME", "PROVINCE_STATE_NAME"],
        ["REPORT_DATE", "COUNTY_FIPS_NUMBER", "PROVINCE_STATE_NAME"],
    ]
    
    for combo in composite_candidates:
        # Check if all columns exist
        if all(col in df.columns for col in combo):
            # Count nulls in any of the columns
            null_in_combo = df[combo].isna().any(axis=1).sum()
            null_pct = (null_in_combo / total_rows) * 100
            
            # Count unique combinations
            unique_combos = df[combo].drop_duplicates().shape[0]
            
            # Check for duplicates after removing nulls
            df_clean = df[combo].dropna()
            duplicates = df_clean.duplicated().sum()
            
            print(f"\nComposite: {' + '.join(combo)}")
            print(f"  - Rows with nulls: {null_in_combo} ({null_pct:.2f}%)")
            print(f"  - Unique combinations: {unique_combos}")
            print(f"  - Duplicates (after removing nulls): {duplicates}")
            
            if null_in_combo == 0 and duplicates == 0:
                print(f"  ✓ CAN BE PRIMARY KEY (no nulls, no duplicates)")
            elif duplicates == 0:
                print(f"  ⚠ CAN BE PRIMARY KEY if nulls are filtered ({null_in_combo} rows will be dropped)")
            else:
                print(f"  ✗ CANNOT BE PRIMARY KEY (has duplicates)")
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    no_null_cols = [r['column'] for r in results if r['can_be_pk']]
    
    print(f"\nColumns with NO nulls (can be part of primary key):")
    for col in no_null_cols:
        print(f"  ✓ {col}")
    
    print(f"\nColumns with nulls (CANNOT be primary key alone):")
    for r in results:
        if not r['can_be_pk']:
            print(f"  ✗ {r['column']}: {r['null_count']} nulls ({r['null_pct']:.2f}%)")
    
    # Recommendation
    print("\n" + "=" * 80)
    print("RECOMMENDATION")
    print("=" * 80)
    
    # Check REPORT_DATE + COUNTY_FIPS_NUMBER
    if all(col in df.columns for col in ["REPORT_DATE", "COUNTY_FIPS_NUMBER"]):
        combo_df = df[["REPORT_DATE", "COUNTY_FIPS_NUMBER"]]
        null_count = combo_df.isna().any(axis=1).sum()
        
        if null_count > 0:
            print(f"\n⚠ PRIMARY KEY: (REPORT_DATE, COUNTY_FIPS_NUMBER)")
            print(f"  - This combination has {null_count} rows with nulls")
            print(f"  - Recommendation: Filter these rows before insert:")
            print(f"    chunk = chunk.dropna(subset=['REPORT_DATE', 'COUNTY_FIPS_NUMBER'])")
            print(f"  - This will DROP {null_count} rows ({(null_count/total_rows)*100:.2f}%)")
        else:
            print(f"\n✓ PRIMARY KEY: (REPORT_DATE, COUNTY_FIPS_NUMBER)")
            print(f"  - Perfect! No nulls found.")
    
    return results


if __name__ == "__main__":
    # Check if file exists
    csv_path = CSV_PATH
    
    if not Path(csv_path).exists():
        print(f"ERROR: File not found: {csv_path}")
        print("\nPlease update CSV_PATH in this script to point to your COVID data file.")
        exit(1)
    
    # You can change sample_size to analyze faster
    # Use None to analyze entire file
    results = analyze_columns_for_primary_key(csv_path, sample_size=None)
    
    print("\n" + "=" * 80)
    print("Analysis complete!")
    print("=" * 80)