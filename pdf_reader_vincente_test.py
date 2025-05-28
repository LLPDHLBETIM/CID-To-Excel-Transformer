import os
import re
import pandas as pd
import pdfplumber
import tkinter as tk
from tkinter import filedialog, messagebox

def process_columns(text):
    """
    Extrai pares chave/valor de texto corrido, conforme antes.
    """
    lines = text.splitlines()
    columns = {}
    current = None
    for line in lines:
        if not line.strip():
            continue
        parts = [p.strip() for p in line.split('  ') if p.strip()]
        for idx, val in enumerate(parts):
            if val in ["PLANTE","SAP/COFOR","SUPPLIER NAME"] or re.match(r'^[A-Z_/]+$', val):
                current = val
                columns.setdefault(current, [])
                if idx+1 < len(parts):
                    columns[current].append(parts[idx+1])
            elif current and idx % 2 == 1:
                prev = parts[idx-1]
                if prev not in columns:
                    current = prev
                    columns[current] = [val]
                else:
                    columns[current].append(val)
    return columns

def fix_none_values_in_table(table, table_num):
    """
    Ajusta células None para tables específicas (1,10,11).
    """
    if table_num not in {1,10,11}:
        return table
    header_labels = {}
    # primeiro linha: extrai rótulos secundários
    for ci, cell in enumerate(table[0]):
        if isinstance(cell, str) and '\n' in cell:
            parts = cell.split('\n',1)
            if len(parts)>1:
                header_labels[ci] = parts[1].strip()
    fixed = []
    for ri, row in enumerate(table):
        new_row = list(row)
        for ci, cell in enumerate(row):
            if ri==0 and isinstance(cell,str) and '\n' in cell:
                new_row[ci] = cell.split('\n',1)[0].strip()
            elif (cell is None or cell=='None') and ci in header_labels:
                new_row[ci] = header_labels[ci]
        fixed.append(new_row)
    return fixed

def process_split_header_tables(table, table_num):
    """
    Ajusta headers múltiplas partes (tabelas 6,7,8).
    """
    if table_num not in {6,7,8} or len(table)<2:
        return table
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
        while len(aligned) < len(header_parts):
            aligned.append(None)
        result.append(aligned)
    return result

def read_pdf_column_wise(path):
    """
    Lê cada PDF, extrai texto em colunas e tables, retorna (df, dict_of_tables).
    """
    if not os.path.isfile(path):
        print(f"Arquivo não encontrado: {path}")
        return pd.DataFrame(), {}

    all_cols = {}
    all_tabs = {}
    with pdfplumber.open(path) as pdf:
        for pg, page in enumerate(pdf.pages, 1):
            # texto → colunas
            txt = page.extract_text() or ''
            cols = process_columns(txt)
            for k, vs in cols.items():
                all_cols.setdefault(k, []).extend(vs)

            # tables
            for ti, tab in enumerate(page.extract_tables() or [], 1):
                ft = fix_none_values_in_table(tab, ti)
                if ti in {7,8}:
                    ft = process_split_header_tables(ft, ti)
                all_tabs[f"Table_{pg}_{ti}"] = ft

    # monta DataFrame de texto
    if all_cols:
        max_len = max(len(v) for v in all_cols.values())
        for k, v in all_cols.items():
            all_cols[k] = v + [None]*(max_len - len(v))
        df = pd.DataFrame(all_cols).dropna(how='all')
    else:
        df = pd.DataFrame()

    return df, all_tabs

def save_tables_to_excel(tables, df, output_path):
    """
    tables      – dict com tables extraídas de todos os PDFs
    df          – DataFrame único de todas as colunas de texto
    output_path – caminho completo do arquivo .xlsx
    """
    def is_nonempty(cell):
        return bool(str(cell).replace('\u200b','').strip()) if cell is not None else False

    # compile keys (ajuste conforme a necessidade real de tables)
    compile_keys = [
        'Table_1_1_transposed', 'Table_1_2_transposed',
        'Table_1_3', 'Table_1_4', 'Table_1_5', 'Table_1_6',
        'Table_1_7', 'Table_1_8', 'Table_1_9',
        'Table_1_10_transposed', 'Table_1_11_transposed', 'Table_1_12'
    ]

    compiled_dfs = []
    for key in compile_keys:
        for name, tbl in tables.items():
            if name.endswith(key):
                if key.endswith('_transposed') and all(len(r)==2 for r in tbl):
                    df_vert = pd.DataFrame(tbl, columns=['field','value']).set_index('field')
                    df_tab = df_vert.T.reset_index(drop=True)
                else:
                    headers = [str(h) if h else f"Col_{i}" for i,h in enumerate(tbl[0])]
                    rows = [r for r in tbl[1:] if any(is_nonempty(c) for c in r)]
                    df_tab = pd.DataFrame(rows, columns=headers)
                compiled_dfs.append(df_tab)

    if compiled_dfs:
        max_rows = max(df_.shape[0] for df_ in compiled_dfs)
        aligned = [df_.reindex(index=range(max_rows)).ffill() for df_ in compiled_dfs]
        df_compiled = pd.concat(aligned, axis=1)
    else:
        df_compiled = pd.DataFrame()

    # grava Excel com 2 sheets
    with pd.ExcelWriter(output_path) as writer:
        df_compiled.to_excel(writer, sheet_name='Compiled_Tables', index=False)
        if not df.empty:
            df.to_excel(writer, sheet_name='Extracted_Columns', index=False)

    print(f"Dados salvos em {output_path}")

def process_all_pdfs(input_folder, output_path):
    master_tables = {}
    df_list = []

    for fname in os.listdir(input_folder):
        if not fname.lower().endswith('.pdf'):
            continue
        full_path = os.path.join(input_folder, fname)
        path = full_path.replace('\\','/')
        print("Processando:", path)

        df_cur, tabs_cur = read_pdf_column_wise(path)
        if not df_cur.empty:
            # Adiciona uma coluna para identificar a origem dos dados (PDF)
            df_cur.insert(0, 'source_pdf', os.path.splitext(fname)[0])
            df_list.append(df_cur)

        for tbl_name, tbl_data in tabs_cur.items():
            master_tables[f"{os.path.splitext(fname)[0]}__{tbl_name}"] = tbl_data

        print("OK:", path)

    # Concatena corretamente TODOS os DataFrames numa única tabela final
    if df_list:
        master_df = pd.concat(df_list, ignore_index=True, sort=False)
    else:
        master_df = pd.DataFrame()

    save_tables_to_excel(master_tables, master_df, output_path)

# — GUI tkinter —
def select_input_folder():
    folder = filedialog.askdirectory(title="Selecione a pasta de PDFs")
    if folder:
        input_entry.delete(0, tk.END)
        input_entry.insert(0, folder)

def select_output_file():
    file = filedialog.asksaveasfilename(
        defaultextension=".xlsx",
        filetypes=[("Excel","*.xlsx")],
        title="Salvar Excel como..."
    )
    if file:
        output_entry.delete(0, tk.END)
        output_entry.insert(0, file)

def run_processing():
    in_folder = input_entry.get()
    out_file  = output_entry.get()
    if not os.path.isdir(in_folder):
        messagebox.showerror("Erro","Pasta de entrada inválida.")
        return
    if not out_file.lower().endswith('.xlsx'):
        messagebox.showerror("Erro","Arquivo de saída deve terminar em .xlsx")
        return
    btn_run.config(state=tk.DISABLED)
    try:
        process_all_pdfs(in_folder, out_file)
        messagebox.showinfo("Concluído", f"Dados salvos em:\n{out_file}")
    except Exception as e:
        messagebox.showerror("Erro de Processamento", str(e))
    finally:
        btn_run.config(state=tk.NORMAL)

if __name__ == '__main__':
    root = tk.Tk()
    root.title("Batch PDF → Excel")

    tk.Label(root, text="Pasta de PDFs:") \
      .grid(row=0, column=0, sticky="e", padx=5, pady=5)
    input_entry = tk.Entry(root, width=50)
    input_entry.grid(row=0, column=1, padx=5, pady=5)
    tk.Button(root, text="Browse…", command=select_input_folder) \
      .grid(row=0, column=2, padx=5)

    tk.Label(root, text="Arquivo Excel:") \
      .grid(row=1, column=0, sticky="e", padx=5, pady=5)
    output_entry = tk.Entry(root, width=50)
    output_entry.grid(row=1, column=1, padx=5, pady=5)
    tk.Button(root, text="Browse…", command=select_output_file) \
      .grid(row=1, column=2, padx=5)

    btn_run = tk.Button(root, text="Executar", width=20, command=run_processing)
    btn_run.grid(row=2, column=1, pady=10)

    root.mainloop()
