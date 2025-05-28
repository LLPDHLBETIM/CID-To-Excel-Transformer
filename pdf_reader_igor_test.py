import os
import glob
import re
import pandas as pd
import pdfplumber


def process_columns(text):
    '''
    Extrai pares chave/valor de texto corrido.
    '''
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
    '''
    Substitui células None com base em cabeçalhos divididos.
    '''
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
    '''
    Alinha cabeçalhos multipartes para tabelas específicas (6,7,8).
    Filtra linhas vazias antes de alinhar.
    '''
    if table_num not in {6, 7, 8} or len(table) < 2:
        return table
    # Header parts from second row
    header_parts = [p for p in table[1] if isinstance(p, str) and p.strip()]
    # Clean header parts
    header_parts = [hp.strip() for cell in header_parts for hp in cell.split() if hp.strip()]
    result = [header_parts]
    # Function to check real non-empty cell (remove zero-width)
    def is_real(cell):
        if cell is None: return False
        txt = str(cell).replace('\u200b', '').strip()
        return bool(txt)
    for row in table[2:]:
        # drop row if all cells empty or zero-width
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
        # pad missing
        while len(aligned) < len(header_parts):
            aligned.append(None)
        result.append(aligned)
    return result


def read_pdf(path):
    '''
    Lê PDF, extrai colunas e tabelas.
    '''
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
    # normalize column-wise
    max_len = max((len(v) for v in all_cols.values()), default=0)
    for k, v in all_cols.items():
        all_cols[k] = v + [None] * (max_len - len(v))
    df_cols = pd.DataFrame(all_cols).dropna(how='all')
    return df_cols, all_tabs


def compile_tables(tables):
    '''
    Transforma e concatena tabelas num DataFrame.
    '''
    def is_nonempty(cell):
        if cell is None: return False
        return bool(str(cell).replace('\u200b','').strip())
    dfs = []
    for key, tbl in tables.items():
        if not tbl or len(tbl) < 2: continue
        # transpose key-value
        if all(len(r) == 2 for r in tbl):
            dfv = pd.DataFrame(tbl, columns=['field','value']).set_index('field')
            df_tab = dfv.T.reset_index(drop=True)
        else:
            hdrs = [str(h) if h is not None else f'Col_{i}' for i,h in enumerate(tbl[0])]
            rows = [r for r in tbl[1:] if any(is_nonempty(c) for c in r)]
            df_tab = pd.DataFrame(rows, columns=hdrs)
        dfs.append(df_tab)
    if not dfs: return pd.DataFrame()
    max_rows = max(df.shape[0] for df in dfs)
    aligned = [df.reindex(range(max_rows)).ffill() for df in dfs]
    return pd.concat(aligned, axis=1)


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
        final = pd.concat(all_dfs, ignore_index=True, sort=False)
    else:
        final = pd.DataFrame()
    with pd.ExcelWriter(out) as writer:
        final.to_excel(writer, sheet_name='Compiled', index=False)
    print(f'Data saved to {out}')

if __name__ == '__main__':
    main()
