import os
import glob
import re
import pandas as pd
import pdfplumber

def process_columns(text):
    lines = text.splitlines()
    columns = {}
    current = None
    for line in lines:
        if not line.strip():
            continue
        parts = [p.strip() for p in line.split('  ') if p.strip()]
        for idx, val in enumerate(parts):
            if val in ['PLANTE', 'SAP/COFOR', 'SUPPLIER NAME'] or re.match(r'^[A-Z_/]+$', val):
                current = val
                columns.setdefault(current, [])
                if idx + 1 < len(parts):
                    columns[current].append(parts[idx + 1])
            elif current and idx % 2 == 1:
                columns[current].append(val)
    return columns

def fix_none_values_in_table(table, table_num):
    if table_num not in {1, 10, 11}:
        return table
    fixed = []
    header_labels = {}
    if table and table[0]:
        for ci, cell in enumerate(table[0]):
            if isinstance(cell, str) and '\n' in cell:
                parts = cell.split('\n', 1)
                header_labels[ci] = parts[1].strip()
    for ri, row in enumerate(table):
        new_row = list(row)
        for ci, cell in enumerate(row):
            if (cell is None or cell == 'None') and ci in header_labels:
                new_row[ci] = header_labels[ci]
        fixed.append(new_row)
    return fixed

def process_split_header_tables(table, table_num):
    if table_num not in {6, 7, 8} or len(table) < 2:
        return table
    header_parts = [p for p in table[1] if isinstance(p, str) and p.strip()]
    header_parts = [hp.strip() for cell in header_parts for hp in cell.split() if hp.strip()]
    result = [header_parts]
    def is_real(cell):
        if cell is None: return False
        txt = str(cell).replace('\u200b', '').strip()
        return bool(txt)
    for row in table[2:]:
        if not any(is_real(cell) for cell in row):
            continue
        aligned = []
        ki = 0
        for cell in row:
            if isinstance(cell, str):
                for part in cell.split('  '):
                    if ki < len(header_parts):
                        aligned.append(part.strip())
                        ki += 1
        while len(aligned) < len(header_parts):
            aligned.append(None)
        result.append(aligned)
    return result

def deduplicate_columns(cols):
    seen = {}
    result = []
    for col in cols:
        base = str(col)
        if base not in seen:
            seen[base] = 0
            result.append(base)
        else:
            seen[base] += 1
            result.append(f"{base}.{seen[base]}")
    return result

def read_pdf(path):
    all_cols = {}
    all_tabs = {}
    with pdfplumber.open(path) as pdf:
        for pg, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ''
            cols = process_columns(text)
            for k, vs in cols.items():
                all_cols.setdefault(k, []).extend(vs)
            tables = page.extract_tables() or []
            for ti, tbl in enumerate(tables, start=1):
                ft = fix_none_values_in_table(tbl, ti)
                if ti in {7, 8}:
                    ft = process_split_header_tables(ft, ti)
                all_tabs[f'Table_{pg}_{ti}'] = ft
    max_len = max((len(v) for v in all_cols.values()), default=0)
    for k, v in all_cols.items():
        all_cols[k] = v + [None] * (max_len - len(v))
    df_cols = pd.DataFrame(all_cols).dropna(how='all')
    return df_cols, all_tabs

def normalize_capacity_header(header):
    """
    Normalize variants of capacity header to 'CAPACITY INCREASE DATE'.
    """
    if not isinstance(header, str):
        return header
    header_clean = header.strip().replace("  ", " ")
    # Accept both with and without colon
    if re.match(r'^CAPACITY INCREASE (DATA|DATE)$', header_clean):
        return "CAPACITY INCREASE DATE"
    if re.match(r'^CAPACITY INCREASE (DATA|DATE):', header_clean):
        return "CAPACITY INCREASE DATE" + header_clean[len("CAPACITY INCREASE DATA"):]
    return header

def compile_tables(tables):
    """
    - If table has headers like 'CAPACITY INCREASE DATA: 03/09/2024' and/or 'SOP DATE: 30/04/2025' (all headers in KEY: value format),
      make a single-row DataFrame with columns 'CAPACITY INCREASE DATE', 'SOP DATE', etc., and the dates as values.
    - For normal tables, standard handling, with normalization of CAPACITY INCREASE DATA/DATE to 'CAPACITY INCREASE DATE'.
    """
    dfs = []
    for key, tbl in tables.items():
        if not tbl or not tbl[0]:
            continue

        # Check if all headers are in "KEY: value" format
        all_kv_headers = True
        kv_pairs = {}
        for h in tbl[0]:
            if isinstance(h, str):
                m = re.match(r'^([A-Z/ _]+):\s*(.+)$', h.strip())
                if m:
                    norm_key = m.group(1).strip()
                    if norm_key in ['CAPACITY INCREASE DATA', 'CAPACITY INCREASE DATE']:
                        norm_key = 'CAPACITY INCREASE DATE'
                    kv_pairs[norm_key] = m.group(2).strip()
                else:
                    all_kv_headers = False
                    break
            else:
                all_kv_headers = False
                break

        if all_kv_headers and kv_pairs:
            df_tab = pd.DataFrame([kv_pairs])
            dfs.append(df_tab)
            continue

        # Standard table handling with header normalization
        new_header = []
        header_value_row = []
        header_modified = False
        for h in tbl[0]:
            if isinstance(h, str):
                m = re.match(r'^([A-Z/ _]+):\s*(.+)$', h.strip())
                if m:
                    norm_key = m.group(1).strip()
                    if norm_key in ['CAPACITY INCREASE DATA', 'CAPACITY INCREASE DATE']:
                        norm_key = 'CAPACITY INCREASE DATE'
                    new_header.append(norm_key)
                    header_value_row.append(m.group(2).strip())
                    header_modified = True
                else:
                    # Normalize header even if not KEY: value
                    if h.strip() in ['CAPACITY INCREASE DATA', 'CAPACITY INCREASE DATE']:
                        new_header.append('CAPACITY INCREASE DATE')
                    else:
                        new_header.append(h)
                    header_value_row.append(None)
            else:
                new_header.append(h)
                header_value_row.append(None)

        if header_modified:
            if len(tbl) == 1 or all((cell is None or str(cell).strip() == '') for cell in tbl[1]):
                tbl.insert(1, header_value_row)
            else:
                if len(tbl) > 1:
                    for i, val in enumerate(header_value_row):
                        if val and (tbl[1][i] is None or str(tbl[1][i]).strip() == ''):
                            tbl[1][i] = val
            tbl[0] = new_header

        if len(tbl) == 1:
            tbl.append([None] * len(tbl[0]))

        if all(len(r) == 2 for r in tbl):
            dfv = pd.DataFrame(tbl, columns=['field', 'value']).set_index('field')
            # Normalize columns in key-value tables as well
            dfv.index = [normalize_capacity_header(idx) for idx in dfv.index]
            df_tab = dfv.T.reset_index(drop=True)
        else:
            hdrs = [normalize_capacity_header(h) if h is not None else f'Col_{i}' for i, h in enumerate(tbl[0])]
            hdrs = deduplicate_columns(hdrs)
            rows = []
            for r in tbl[1:]:
                row = list(r) + [None] * (len(hdrs) - len(r))
                rows.append(row)
            df_tab = pd.DataFrame(rows, columns=hdrs)
        df_tab.columns = deduplicate_columns(df_tab.columns)
        dfs.append(df_tab)
    if not dfs:
        return pd.DataFrame()
    max_rows = max(df.shape[0] for df in dfs)
    for i, df in enumerate(dfs):
        df.columns = deduplicate_columns(df.columns)
    aligned = [df.reindex(range(max_rows)).ffill() for df in dfs]
    final_df = pd.concat(aligned, axis=1)
    final_df.columns = deduplicate_columns(final_df.columns)
    return final_df

def main():
    pdf_folder = 'pdf_reader'
    out = 'compiled_output.xlsx'
    pdfs = glob.glob(os.path.join(pdf_folder, '*.pdf'))
    all_dfs = []
    for p in pdfs:
        print(f'Processing {p}...')
        _, tables = read_pdf(p)
        dfc = compile_tables(tables)
        if not dfc.empty:
            dfc.insert(0, 'source_pdf', os.path.basename(p))
            all_dfs.append(dfc)
    if all_dfs:
        for i, df in enumerate(all_dfs):
            df.columns = deduplicate_columns(df.columns)
        final = pd.concat(all_dfs, ignore_index=True, sort=False)
        final.columns = deduplicate_columns(final.columns)
    else:
        final = pd.DataFrame()
    with pd.ExcelWriter(out) as writer:
        final.to_excel(writer, sheet_name='Compiled', index=False)
    print(f'Data saved to {out}')

if __name__ == '__main__':
    main()