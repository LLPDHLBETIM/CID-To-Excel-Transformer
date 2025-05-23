import pandas as pd
import pdfplumber
import os
import re


def process_columns_for_excel(all_columns):
    """
    Process columns to ensure complete information is saved to Excel
    """
    # Create a new dictionary to store the processed data
    processed_data = {}
    
    # Find the maximum length of any column
    max_length = max([len(values) for values in all_columns.values()], default=0)
    
    # Get all keys that appear to be related
    key_groups = {
        'PLANTE': ['SAP/COFOR', 'SUPPLIER NAME'],
        'PURCHASING': ['SQD']
    }
    
    # Process each key group
    for main_key, related_keys in key_groups.items():
        if main_key in all_columns:
            # Create a new column for the complete information
            complete_info = []
            
            # Get the values for the main key
            main_values = all_columns[main_key]
            
            # For each value in the main key
            for i in range(max_length):
                if i < len(main_values) and main_values[i]:
                    info = f"{main_key}: {main_values[i]}"
                    
                    # Add related information if available
                    for related_key in related_keys:
                        if related_key in all_columns and i < len(all_columns[related_key]) and all_columns[related_key][i]:
                            info += f", {related_key}: {all_columns[related_key][i]}"
                    
                    complete_info.append(info)
                else:
                    complete_info.append(None)
            
            # Add the complete information to the processed data
            processed_data[f"{main_key}_COMPLETE"] = complete_info
    
    # Add all original columns, ensuring they all have the same length
    for key, values in all_columns.items():
        # Pad with None to ensure all columns have the same length
        padded_values = values + [None] * (max_length - len(values))
        processed_data[key] = padded_values
    return processed_data


def save_tables_to_excel(tables, df=None):
    """Save all tables to an Excel file with one sheet per table and the column-wise data"""
    output_path = "pdf_data_complete.xlsx"
    
    with pd.ExcelWriter(output_path) as writer:
        # First, save the column-wise data if available
        if df is not None:
            # Process the data to ensure complete information
            processed_data = process_columns_for_excel(df.to_dict('list'))
            
            # Create DataFrame from processed data
            try:
                processed_df = pd.DataFrame(processed_data)
                processed_df.to_excel(writer, sheet_name="Column_Wise_Data", index=False)
                print(f"Column-wise data saved to sheet 'Column_Wise_Data'")
            except ValueError as e:
                print(f"Error creating DataFrame from processed data: {e}")
                # Save the original DataFrame as fallback
                df.to_excel(writer, sheet_name="Column_Wise_Data", index=False)
                print(f"Original column-wise data saved to sheet 'Column_Wise_Data'")
       
        # Then save each table
        
        
        for table_name, table_data in tables.items():
            print("Tables here   : ", tables)
            # Convert table to DataFrame
            if table_data and len(table_data) > 0:
                headers = table_data[0]
                data = table_data[1:]
                
                # Filter out None values from headers
                headers = [str(h) if h is not None else f"Column_{i}" for i, h in enumerate(headers)]
                
                try:
                    # Create DataFrame
                    table_df = pd.DataFrame(data, columns=headers)
                    
                    # Write to Excel
                    sheet_name = table_name[:31]  # Excel sheet names limited to 31 chars
                    table_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"Table saved to sheet '{sheet_name}'")
                except Exception as e:
                    print(f"Error saving table {table_name}: {e}")
    
    print(f"All data saved to {output_path}")

def process_columns(text):
    """Process text to extract data in a column-wise manner"""
    # Split by newlines first
    lines = text.split('\n')
    
    # Initialize dictionary to store column data
    columns = {}
    current_column = None
    
    for line in lines:
        # Skip empty lines
        if not line.strip():
            continue
        
        # Split by double spaces to identify potential columns
        parts = [part.strip() for part in line.split('  ') if part.strip()]
        
        # Process each part as a potential column header or value
        for i, part in enumerate(parts):
            # Check if this part looks like a column header
            if part in ["PLANTE", "SAP/COFOR", "SUPPLIER NAME"] or re.match(r'^[A-Z_/]+$', part):
                current_column = part
                columns[current_column] = []
                # If there's a value right after the header, add it
                if i + 1 < len(parts):
                    columns[current_column].append(parts[i + 1])
            # If we have an active column and this isn't a header, it's a value
            elif current_column and i % 2 == 1:  # Odd indices are values in a double-space split
                if i - 1 >= 0 and parts[i-1] not in columns:
                    # This is a value for a new column
                    columns[parts[i-1]] = [part]
                    current_column = parts[i-1]
                else:
                    # This is a value for the current column
                    columns[current_column].append(part)
    
    return columns



def fix_none_values_in_table(table, table_num):
    """
    Fix None values in specific tables (1, 10, 11) by replacing them with
    the appropriate label from the previous cell's newline split
    """
    if table_num not in [1, 10, 11]:
        return table
    
    fixed_table = []
    
    # First, extract header information from row 0
    header_labels = {}
    if table and len(table) > 0:
        for col_idx, cell in enumerate(table[0]):
            if cell and isinstance(cell, str) and '\n' in cell:
                parts = cell.split('\n')
                if len(parts) > 1:
                    # Store the part after newline as the label for this column
                    header_labels[col_idx] = parts[1].strip()
    
    # Now process each row
    for row_idx, row in enumerate(table):
        fixed_row = list(row)  # Create a copy of the row to modify
        
        for col_idx, cell in enumerate(row):
            # For header row, fix cells with newlines to only show the first part
            if row_idx == 0 and cell and isinstance(cell, str) and '\n' in cell:
                parts = cell.split('\n')
                fixed_row[col_idx] = parts[0].strip()  # Keep only the left part
            
            # Check if this cell is None
            elif cell is None or cell == "None":
                # If we're in the header row and have a previous cell with newline
                if row_idx == 0 and col_idx > 0:
                    prev_cell = row[col_idx-1]
                    if prev_cell and isinstance(prev_cell, str) and '\n' in prev_cell:
                        parts = prev_cell.split('\n')
                        if len(parts) > 1:
                            fixed_row[col_idx] = parts[1].strip()
                # If we're in a data row, use the header label for this column
                elif col_idx in header_labels:
                    fixed_row[col_idx] = header_labels[col_idx]
        
        fixed_table.append(fixed_row)
    
    return fixed_table



def process_split_header_tables(table, table_num):
    """
    Process tables with split headers (like tables 7 and 8)
    where the header contains multiple values separated by spaces
    and the data rows need to be aligned with these header parts.
    """
    if table_num not in [6,7, 8]:
        return table
    
    if not table or len(table) < 2:
        return table
    
    processed_table = []
    
    # Get the header row
    header_row_lable = table[0]
    header_row = table[1]
    
    # Process the header to split it into parts
    split_headers = []
    for cell in header_row:
        if cell and isinstance(cell, str):
            # Split by spaces but keep meaningful parts
            parts = [p.strip() for p in cell.split(' ') if p.strip()]
            split_headers.extend(parts)
        else:
            # Don't add None values to the header
            continue
    
    # Add the processed header row (without None values)
    processed_table.append(split_headers)
    
    # Process data rows
    for row_idx in range(2, len(table)):
        row = table[row_idx]
        
        # Skip empty rows
        if not row or all(cell is None or str(cell).strip() == '' for cell in row):
            continue
        
        processed_row = []
        
        # Align data values with split headers
        header_idx = 0
        for cell in row:
            if cell and isinstance(cell, str):
                # Split the cell by spaces
                parts = [p.strip() for p in cell.split('  ') if p.strip()]
                
                # Add each part, aligning with headers
                for part in parts:
                    if header_idx < len(split_headers):
                        processed_row.append(part)
                        header_idx += 1
            else:
                # Skip None values in data rows
                continue
        
        # Pad the row if needed
        while len(processed_row) < len(split_headers):
            processed_row.append(None)
        
        # Add the processed row
        processed_table.append(processed_row)
    
    return processed_table


def read_pdf_column_wise():
    # Static path to the PDF file
    pdf_path = "pdf_reader/Entrega de capacidade Marelli (55).pdf"
    
    # Check if file exists
    if not os.path.exists(pdf_path):
        print(f"Error: File not found at {pdf_path}")
        return None
    
    all_columns = {}
    all_tables = {}
    
    try:
        # Open the PDF file
        with pdfplumber.open(pdf_path) as pdf:
            print(f"PDF loaded successfully. Total pages: {len(pdf.pages)}")
            
            # Process each page
            for page_num, page in enumerate(pdf.pages, 1):
                print(f"\n--- Processing Page {page_num} ---")
                
                # Extract text
                text = page.extract_text()
                if text:
                    print(f"Text content from page {page_num}:")
                    print(text[:500] + "..." if len(text) > 500 else text)
                    
                    # Process the text to extract column data
                    columns = process_columns(text)
                    
                    # Merge with existing columns
                    for col_name, col_values in columns.items():
                        if col_name in all_columns:
                            all_columns[col_name].extend(col_values)
                        else:
                            all_columns[col_name] = col_values
                
                # Extract tables
                tables = page.extract_tables()
                if tables:
                    print(f"Found {len(tables)} tables on page {page_num}")
                    
                    for table_num, table in enumerate(tables, 1):
                        print(f"Original Table {table_num} content:")
                        for row in table:
                            print(row)
                        
                        # Fix None values in specific tables
                        fixed_table = fix_none_values_in_table(table, table_num)
                        
                        # Then process split header tables
                        if table_num in [7, 8]:
                            fixed_table = process_split_header_tables(fixed_table, table_num)
                        
                        print(f"Transformed Table {table_num} content:")
                        for row in fixed_table:
                            print(row)
                        
                        # Store the fixed table
                        all_tables[f"Table_{page_num}_{table_num}"] = fixed_table
                        
                        # Convert table to DataFrame
                        if fixed_table and len(fixed_table) > 0:
                            # Use first row as header if it looks like a header
                            headers = fixed_table[0]
                            
                            # Filter out empty rows
                            data = []
                            for row in fixed_table[1:]:
                                # Check if row is empty or contains only empty strings
                                if row and any(cell is not None and str(cell).strip() != '' for cell in row):
                                    data.append(row)
                                else:
                                    # print("Skipping empty row:", row)
                                    None
                            
                            # Only create DataFrame if we have data
                            if data and headers:
                                # Make sure headers are strings
                                headers = [str(h) if h is not None else f"Column_{i}" for i, h in enumerate(headers)]
                                
                                # Create DataFrame
                                df = pd.DataFrame(data, columns=headers)
                                print(f"DataFrame for Table {table_num}:")
                                print(df)
                                
                                # Process each column and add to all_columns
                                for col in df.columns:
                                    col_name = col.strip()
                                    if not col_name:
                                        continue
                                        
                                    if col_name not in all_columns:
                                        all_columns[col_name] = []
                                    
                                    # Add non-empty values to the column
                                    values = [val for val in df[col] if val and str(val).strip()]
                                    all_columns[col_name].extend(values)
                                    
                                    # print(f"Added {len(values)} values to column '{col_name}'")
                            else:
                                # print(f"No non-empty data rows found in Table {table_num}")
                                None
                           
    
    except Exception as e:
        print(f"Error processing PDF: {e}")
        import traceback
        traceback.print_exc()
        return None, None
    
    # Create DataFrame from column data
    if all_columns:
        # First, determine the maximum length of any column
        max_length = max([len(values) for values in all_columns.values()], default=0)
        
        # Pad shorter columns with None
        for col_name in all_columns:
            all_columns[col_name] = all_columns[col_name] + [None] * (max_length - len(all_columns[col_name]))
           
        # Create DataFrame
        df = pd.DataFrame(all_columns)
        # print("Data Frame : ",df)
        # Clean up the DataFrame - remove rows that are all None
        df = df.dropna(how='all')
                
        return df, all_tables
    else:
        print("No columns extracted from the PDF")
        return None, all_tables


def split_after_two_spaces(text):
    """Split text after sequences of exactly two spaces"""
    pattern = r'(?<=  )'
    parts = re.split(pattern, text)
    
    # Remove any empty strings and strip whitespace
    parts = [part.strip() for part in parts if part.strip()]
    
    return parts

def extract_column_data_from_text(text):
    """Extract column data from text using the split_after_two_spaces function"""
    # Split text into lines
    lines = text.split('\n')
    
    # Initialize columns dictionary
    columns = {}
    
    for line in lines:
        # Skip empty lines
        if not line.strip():
            continue
        
        # Split line by double spaces
        parts = split_after_two_spaces(line)
        
        # Process parts in pairs (label, value)
        for i in range(0, len(parts) - 1, 2):
            label = parts[i]
            value = parts[i + 1] if i + 1 < len(parts) else ""
            
            if label not in columns:
                columns[label] = []
            
            columns[label].append(value)
    
    return columns

def save_tables_to_excel(tables, df=None):
    output_path = "marelli_data_complete.xlsx"
    
    header_info = {}
    dataframes = []

    # 1. Extract header_info from key-value and protocol tables
    for table_name, table_data in tables.items():
        if not table_data or len(table_data) < 1:
            continue
        
        # Key-value pairs, usually 2 columns per row
        if all(isinstance(row, list) and len(row) == 2 for row in table_data):
            for row in table_data:
                key = str(row[0]).strip() if row[0] else ""
                value = str(row[1]).strip() if row[1] else ""
                if key:
                    header_info[key] = value
            continue
        
        # Protocol or summary tables (2 rows, 1 column each)
        if len(table_data) == 2 and all(len(row) == 1 for row in table_data):
            key = str(table_data[0][0]).strip() if table_data[0][0] else ""
            value = str(table_data[1][0]).strip() if table_data[1][0] else ""
            if key:
                header_info[key] = value
            continue

    # 2. For all other tables (with multiple columns), create DataFrames and add header_info columns as constants
    for table_name, table_data in tables.items():
        if not table_data or len(table_data) < 1:
            continue
        
        # Skip tables already processed as header_info
        if all(isinstance(row, list) and len(row) == 1 for row in table_data):
            continue
        if len(table_data) == 1 and all(len(row) == 1 for row in table_data):
            continue
        
        # Assume first row is header
        headers = [str(h) if h is not None else f"Column_{i}" for i, h in enumerate(table_data[0])]
        rows = table_data[1:]

        
        # Filter empty rows (optional)
        filtered_rows = [row for row in rows if any(cell is not None and str(cell).strip() != '' for cell in row)]
        if not filtered_rows:
            continue
        print(f"Processing rows '{filtered_rows}'")
        
        df_table = pd.DataFrame(filtered_rows, columns=headers)
        

        # Add header_info as columns with constant value per row
        for k, v in header_info.items():
            df_table[k] = v
        
        print(f"Processing rows '{df_table}'")

        dataframes.append(df_table[2:])

        print(f"Table '{df_table}' processed and added to dataframes list")
        df_final = pd.concat(dataframes, ignore_index=True)
        print(f"Final DataFrame: {df_final}")

    # 3. Concatenate all dataframes (columns will merge and missing values will be NaN)
    # if dataframes:
    #     df_final = pd.concat(dataframes, ignore_index=True)
    # else:
    #     # If no data tables, create one row from header_info only
    #     df_final = pd.DataFrame([header_info])
    
    # 4. Optional: reorder columns if you want header_info columns first (or any preferred order)
    header_cols = list(header_info.keys())
    other_cols = [col for col in df_final.columns if col not in header_cols]
    # df_final = df_fina]
    
    # 5. Save to Excel
    with pd.ExcelWriter(output_path) as writer:
        df_final.to_excel(writer, sheet_name="All_Data", index=False)

    print(f"All data saved to {output_path} with header_info merged and other columns appended")
    return df_final

if __name__ == "__main__":
    print("Starting PDF extraction process...")
    df, all_tables = read_pdf_column_wise()
    
    
    # Save all tables and column-wise data to Excel
    if all_tables:
        save_tables_to_excel(all_tables, df)
    else:
        print("No tables extracted from the PDF.")

if __name__ == "__main__":
    print("Starting PDF extraction process...")
    df, all_tables = read_pdf_column_wise()
    
    if df is not None:
        # print("\nFinal DataFrame (Column-wise):")
        # print(df)
        
        
        # Save all tables to Excel
        if all_tables:
            save_tables_to_excel(all_tables)
    else:
        print("Failed to extract structured data from the PDF.")
