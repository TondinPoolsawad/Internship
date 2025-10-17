"""This Code reads all the fetched csv, filters them and combined them into 1 combined csv"""
import os
import pandas as pd
import glob

csv_files = glob.glob('oae_output/*.csv')
results = []

# Column mapping for standardization
YEAR_COLUMNS = ['year_th', 'crop_year', 'year_crop', 'ปี']
COMMODITY_COLUMNS = ['commod', 'commodity', 'สินค้า']
VALUE_COLUMNS = ['values', 'value', 'มูลค่า', 'ปริมาณ']
UNIT_COLUMNS = ['unit', 'หน่วย']

def clean_year(year_val):
    try:
        year_str = str(year_val).strip()
        if year_str.lower() in ['nan', 'none', '', '#nan', 'n/a']:
            return 'N/A'

        year_num = pd.to_numeric(year_str, errors='coerce')
        if pd.notna(year_num):
            return str(int(year_num))

        return year_str
    except:
        return str(year_val)

def clean_value_and_unit(val, unit):
    try:
        # Convert to string and clean up
        val_str = str(val).strip()
        unit_str = str(unit).strip()
        
        # Handle NaN, blank, or invalid values
        if val_str.lower() in ['nan', 'none', '', '#nan', 'n/a']:
            return '0.00', 'ตัน'
        
        # Remove any commas and try to convert to float
        val_num = float(val_str.replace(',', ''))
        
        # If unit is 'พันตัน', multiply by 1000 and change unit to 'ตัน'
        if 'พันตัน' in unit_str:
            val_num *= 1000
            return f"{val_num:,.2f}", 'ตัน'
        
        return f"{val_num:,.2f}", 'ตัน' if 'ตัน' in unit_str else unit_str
    except:
        return val_str, unit_str

def find_matching_column(df, possible_names):
    for col in possible_names:
        matches = [c for c in df.columns if col.lower() in c.lower()]
        if matches:
            return matches[0]
    return None

# Process each CSV file
for file in csv_files:
    try:
        # Read CSV with UTF-8 encoding
        df = pd.read_csv(file, encoding='utf-8')
        
        # Convert all columns to string type for consistent searching
        df = df.astype(str)
        
        # Filter rows that contain 'ตัน' in any column
        mask = df.apply(lambda x: x.str.contains('ตัน', na=False)).any(axis=1)
        rows_with_ton = df[mask]
        
        if not rows_with_ton.empty:
            # Create a new dataframe with only required columns
            selected_data = pd.DataFrame()
            
            # Find and rename columns
            year_col = find_matching_column(rows_with_ton, YEAR_COLUMNS)
            commod_col = find_matching_column(rows_with_ton, COMMODITY_COLUMNS)
            value_col = find_matching_column(rows_with_ton, VALUE_COLUMNS)
            unit_col = find_matching_column(rows_with_ton, UNIT_COLUMNS)
            
            # Add found columns with standardized names
            if year_col:
                selected_data['Year'] = rows_with_ton[year_col].apply(clean_year)
            else:
                selected_data['Year'] = 'N/A'
                
            if commod_col:
                selected_data['Commodity'] = rows_with_ton[commod_col].fillna('').astype(str).str.strip()
            else:
                selected_data['Commodity'] = os.path.splitext(os.path.basename(file))[0]
                
            if value_col:
                if unit_col:
                    # Apply cleaning to both value and unit together
                    value_unit_pair = rows_with_ton.apply(
                        lambda row: clean_value_and_unit(row[value_col], row[unit_col]), 
                        axis=1
                    )
                    selected_data['Value'] = value_unit_pair.apply(lambda x: x[0])
                    selected_data['Unit'] = value_unit_pair.apply(lambda x: x[1])
                else:
                    # If no unit column, assume 'ตัน' since we filtered for it
                    value_unit_pair = rows_with_ton[value_col].apply(
                        lambda x: clean_value_and_unit(x, 'ตัน')
                    )
                    selected_data['Value'] = value_unit_pair.apply(lambda x: x[0])
                    selected_data['Unit'] = value_unit_pair.apply(lambda x: x[1])
            else:
                selected_data['Value'] = '0.00'
                selected_data['Unit'] = 'ตัน'
            
            # Add source file information
            selected_data['Source_File'] = os.path.basename(file)
            
            results.append(selected_data)
            
    except Exception as e:
        print(f"Error processing {file}: {str(e)}")

if results:
    # Combine all results
    final_df = pd.concat(results, ignore_index=True)

    # Remove any duplicate rows
    final_df = final_df.drop_duplicates()

    # Final cleanup of values
    final_df['Year'] = final_df['Year'].apply(clean_year)
    value_unit_pair = final_df.apply(
        lambda row: clean_value_and_unit(row['Value'], row['Unit']), 
        axis=1
    )
    final_df['Value'] = value_unit_pair.apply(lambda x: x[0])
    final_df['Unit'] = value_unit_pair.apply(lambda x: x[1])
    
    final_df['Commodity'] = final_df['Commodity'].fillna('').astype(str).str.strip()

    cols = ['Year', 'Commodity', 'Value', 'Unit', 'Source_File']
    final_df = final_df[cols]

    output_file = 'oae_output/combined_ton_data_cleaned.csv'
    try:
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    except PermissionError:
        # If file is locked, try with a timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'oae_output/combined_ton_data_cleaned_{timestamp}.csv'
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"\nProcessing complete!")
    print(f"Combined data saved to: {output_file}")
    print(f"\nSummary:")
    print(f"Total rows found: {len(final_df)}")
    print(f"Number of source files with matches: {len(results)}")
    print(f"\nColumns in output:")
    for col in cols:
        print(f"- {col}")
    print(f"\nFiles processed:")
    for file in csv_files:
        print(f"- {os.path.basename(file)}")
else:
    print("No rows containing 'ตัน' were found in any of the CSV files.")