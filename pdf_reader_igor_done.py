import pandas as pd
import pdfplumber
import os
import re


def process_columns_for_excel(all_columns):
    """
    Process columns to ensure complete information is saved to Excel
    """
    processed_data = {}
    max_length = max((len(vals) for vals in all_columns.values()), default=0)

    # Define groups of related columns to combine
    key_groups = {
        'PLANTE': ['SAP/COFOR', 'SUPPLIER NAME'],
        'PURCHASING': ['SQD']
    }

    # Build combined info columns
    for main_key, related_keys in key_groups.items():
        values = all_columns.get(main_key, [])
        combined = []
        for i in range(max_length):
            if i < len(values) and values[i]:
                entry = f"{main_key}: {values[i]}"
                for rk in related_keys:
                    rk_vals = all_columns.get(rk, [])
                    if i < len(rk_vals) and rk_vals[i]:
                        entry += f", {rk}: {rk_vals[i]}"
                combined.append(entry)
            else:
                combined.append(None)
        processed_data[f"{main_key}_COMPLETE"] = combined

    # Pad and include all original columns
    for key, vals in all_columns.items():
        padded = vals + [None] * (max_length - len(vals))
        processed_data[key] = padded

    return processed_data


def save_tables_to_excel(tables, df=None):
    """
    Build a compiled DataFrame from selected tables and save only the 'Compiled' sheet to Excel.
    """
    output_path = "marelli_data_complete.xlsx"

    # Helper to detect non-empty cells (removes zero-width spaces)
    def is_nonempty(cell):
        if cell is None:
            return False
        text = str(cell).replace('\u200b', '').strip()
        return bool(text)

    # Tables to include in the compiled sheet
    compile_keys = [
        'Table_1_1_transposed', 'Table_1_2_transposed',
        'Table_1_3', 'Table_1_4', 'Table_1_5', 'Table_1_6',
        'Table_1_7', 'Table_1_8', 'Table_1_9',
        'Table_1_10_transposed', 'Table_1_11_transposed', 'Table_1_12'
    ]

    compiled_dfs = []
    # Build list of DataFrames for each specified key
    for key in compile_keys:
        base = key.replace('_transposed', '')
        tbl = tables.get(base)
        if not tbl or len(tbl) < 2:
            continue

        if key.endswith('_transposed') and all(len(r) == 2 for r in tbl):
            df_vert = pd.DataFrame(tbl, columns=['field', 'value']).set_index('field')
            df_tab = df_vert.T.reset_index(drop=True)
        else:
            headers = [
                str(h) if h is not None else f"Column_{i}"
                for i, h in enumerate(tbl[0])
            ]
            # keep only truly non-empty rows
            data_rows = [
                row for row in tbl[1:]
                if any(is_nonempty(c) for c in row)
            ]
            df_tab = pd.DataFrame(data_rows, columns=headers)

        compiled_dfs.append(df_tab)

    # Align all DataFrames on the same number of rows
    if compiled_dfs:
        max_rows = max(df.shape[0] for df in compiled_dfs)
        aligned = []
        for df in compiled_dfs:
            df_aligned = df.reindex(index=range(max_rows)).ffill()
            aligned.append(df_aligned)
        df_compilado = pd.concat(aligned, axis=1)
    else:
        df_compilado = pd.DataFrame()

    # Write only the 'Compiled' sheet
    with pd.ExcelWriter(output_path) as writer:
        df_compilado.to_excel(writer, sheet_name='Compiled', index=False)
    print(f"Compiled sheet saved to {output_path}")


def process_columns(text):
    """
    Process plain text to extract columns by splitting on double spaces and detecting headers.
    """
    lines = text.splitlines()
    columns = {}
    current = None

    for line in lines:
        if not line.strip():
            continue
        parts = [p.strip() for p in line.split('  ') if p.strip()]
        for idx, val in enumerate(parts):
            # identify header
            if val in ["PLANTE", "SAP/COFOR", "SUPPLIER NAME"] or re.match(r'^[A-Z_/]+$', val):
                current = val
                columns.setdefault(current, [])
                # if next part exists, treat as first value
                if idx + 1 < len(parts):
                    columns[current].append(parts[idx + 1])
            # treat odd-indexed parts as values when a column is active
            elif current and idx % 2 == 1:
                prev = parts[idx - 1]
                if prev not in columns:
                    current = prev
                    columns[current] = [val]
                else:
                    columns[current].append(val)

    return columns


def fix_none_values_in_table(table, table_num):
    """
    For specific table indices, replace None or 'None' using header splits.
    """
    if table_num not in {1, 10, 11}:
        return table
    fixed = []
    # extract secondary header labels
    header_labels = {}
    if table:
        for ci, cell in enumerate(table[0]):
            if isinstance(cell, str) and '\n' in cell:
                parts = cell.split('\n', 1)
                if len(parts) > 1:
                    header_labels[ci] = parts[1].strip()
    # process rows
    for ri, row in enumerate(table):
        new_row = list(row)
        for ci, cell in enumerate(row):
            if ri == 0 and isinstance(cell, str) and '\n' in cell:
                new_row[ci] = cell.split('\n', 1)[0].strip()
            elif cell is None or cell == 'None':
                if ri == 0 and ci in header_labels:
                    new_row[ci] = header_labels[ci]
                elif ci in header_labels:
                    new_row[ci] = header_labels[ci]
        fixed.append(new_row)
    return fixed


def process_split_header_tables(table, table_num):
    """
    Align multi-part headers and their cells for given tables (6, 7, 8).
    """
    if table_num not in {6, 7, 8} or len(table) < 2:
        return table
    # second row has the actual header parts
    header_parts = []
    for cell in table[1]:
        if isinstance(cell, str):
            header_parts.extend([p.strip() for p in cell.split() if p.strip()])
    result = [header_parts]
    for row in table[2:]:
        if not any(cell and str(cell).strip() for cell in row):
            continue
        aligned = []
        ki = 0
        for cell in row:
            if isinstance(cell, str):
                for part in cell.split('  '):
                    if ki < len(header_parts):
                        aligned.append(part.strip())
                        ki += 1
        # pad
        while len(aligned) < len(header_parts):
            aligned.append(None)
        result.append(aligned)
    return result


def read_pdf_column_wise():
    """
    Read and extract both text-based columns and tables from the PDF.
    Returns a DataFrame of column-wise data plus a dict of tables.
    """
    path = "pdf_reader/Entrega de capacidade Marelli (55).pdf"
    if not os.path.isfile(path):
        print(f"File not found: {path}")
        return None, {}

    all_cols = {}
    all_tabs = {}
    with pdfplumber.open(path) as pdf:
        for pg, page in enumerate(pdf.pages, 1):
            txt = page.extract_text() or ''
            cols = process_columns(txt)
            for k, vs in cols.items():
                all_cols.setdefault(k, []).extend(vs)
            tabs = page.extract_tables() or []
            for ti, tab in enumerate(tabs, 1):
                ft = fix_none_values_in_table(tab, ti)
                if ti in {7, 8}:
                    ft = process_split_header_tables(ft, ti)
                name = f"Table_{pg}_{ti}"
                all_tabs[name] = ft
    # build DataFrame from columns
    if all_cols:
        ml = max(len(v) for v in all_cols.values())
        for k, v in all_cols.items():
            all_cols[k] = v + [None] * (ml - len(v))
        df = pd.DataFrame(all_cols).dropna(how='all')
    else:
        df = pd.DataFrame()
    return df, all_tabs


if __name__ == '__main__':
    df, tables = read_pdf_column_wise()
    if tables:
        save_tables_to_excel(tables, df)
    else:
        print("No tables extracted.")