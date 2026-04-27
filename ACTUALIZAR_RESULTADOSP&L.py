import pandas as pd
import json
import os
import re

# CONFIGURACIÓN DE RUTAS
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
EXCEL_PATH    = os.path.join(BASE_DIR, 'RESULTADOS.xlsx')
HTML_PATH     = os.path.join(BASE_DIR, 'INFORME_RESULTADOS.html')
PARQUET_PATH  = os.path.join(BASE_DIR, 'resultados_data.parquet')
META_PATH     = os.path.join(BASE_DIR, 'resultados_meta.json')

TAX_RATES = {
    'COLOMBIA': 0.19, 'PANAMA': 0.07, 'EL SALVADOR': 0.13, 'GUATEMALA': 0.12,
    'HONDURAS': 0.15, 'NICARAGUA': 0.15, 'REP DOMINICANA': 0.18, 'COSTA RICA': 0.13, 'URUGUAY': 0.22
}

MESES = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

def to_float(v):
    try:
        if pd.isnull(v): return 0.0
        if isinstance(v, str):
            v = v.replace('$', '').replace(',', '').strip()
        return float(v)
    except:
        return 0.0

def format_month(m_idx):
    return MESES[m_idx - 1]

def process_dashboard():
    print("🚀 Iniciando actualización del Dashboard P&L → Parquet...")

    if not os.path.exists(EXCEL_PATH):
        print(f"❌ Error: No se encuentra {EXCEL_PATH}")
        return

    xls = pd.ExcelFile(EXCEL_PATH)

    def get_sheet(name):
        n = name.strip().upper()
        for s in xls.sheet_names:
            if s.strip().upper() == n: return s
        return None

    # ── Recopilar todos los datos en una estructura de árbol temporal ──
    raw_tree = {}

    # 1. Hojas de columna única (value por cadena/año/mes)
    single_col_sheets = {
        'VENTAS NETAS': 'ventas_netas',
        'PRESUPUESTO DE VENTAS': 'ppto_ventas',
        'COBRO POR ENVIO': 'cobro_envio'
    }
    for sheet_name, key in single_col_sheets.items():
        actual = get_sheet(sheet_name)
        if actual:
            df = pd.read_excel(xls, sheet_name=actual)
            sheet_dict = {}
            for _, row in df.iterrows():
                cadena = str(row.iloc[0]).strip()
                if not cadena or cadena == 'nan' or 'TOTAL' in cadena.upper() or 'CADENA' in cadena.upper(): continue
                vals = []
                for i in range(12):
                    vals.append({'year': 2025, 'month': i+1, 'value': to_float(row.iloc[i+1])})
                for i in range(12):
                    vals.append({'year': 2026, 'month': i+1, 'value': to_float(row.iloc[i+13])})
                sheet_dict[cadena] = vals
            raw_tree[key] = sheet_dict

    # 2. Hojas con pares de columnas
    pair_col_sheets = {
        'CANCELACIONES': ('cancelaciones', 'CANCELADO',   'CANCEL_EST', False),
        'DESCUENTOS':    ('descuentos',    'DESC_APLICADO','DESC_EST',   False),
        'MARGEN':        ('margen',        'MARGEN_REAL', 'MARGEN_EST', False),
        'A&P':           ('ap',            'AP_EJEC',     'AP_PPTO',    False),
        'OVERHEAD':      ('overhead',      'OH_EJEC',     'OH_PPTO',    True),
        'ADMIN':         ('admin',         'ADMIN_EJEC',  'ADMIN_PPTO', False),
        'COSTO DE EMPAQUE': ('empaque',    'EMP_EJEC',    'EMP_PPTO',   True),
        'COSTO DE ENVIO':   ('costo_envio','ENV_EJEC',    'ENV_PPTO',   False)
    }
    for sheet_name, (key, f_real, f_ppto, is_swapped) in pair_col_sheets.items():
        actual = get_sheet(sheet_name)
        if not actual:
            if 'ADMIN' in sheet_name: actual = get_sheet('Admin expenses')
            elif 'MARGEN' in sheet_name: actual = get_sheet('MARGEN')
        if actual:
            df = pd.read_excel(xls, sheet_name=actual)
            sheet_dict = {}
            for _, row in df.iterrows():
                cadena = str(row.iloc[0]).strip()
                if not cadena or cadena == 'nan' or 'TOTAL' in cadena.upper() or 'CADENA' in cadena.upper(): continue
                vals = []
                idx_r = 1 if not is_swapped else 2
                idx_p = 2 if not is_swapped else 1
                for i in range(12):
                    v_r = to_float(row.iloc[idx_r + i*2])
                    v_p = to_float(row.iloc[idx_p + i*2])
                    vals.append({'year': 2025, 'month': i+1, f_real: v_r, f_ppto: v_p})
                idx_r_26 = 25 if not is_swapped else 26
                idx_p_26 = 26 if not is_swapped else 25
                for i in range(12):
                    v_r = to_float(row.iloc[idx_r_26 + i*2])
                    v_p = to_float(row.iloc[idx_p_26 + i*2])
                    vals.append({'year': 2026, 'month': i+1, f_real: v_r, f_ppto: v_p})
                sheet_dict[cadena] = vals
            raw_tree[key] = sheet_dict

    # ── Detectar último mes cerrado 2026 ──
    max_month = 1
    if 'ventas_netas' in raw_tree:
        for vals in raw_tree['ventas_netas'].values():
            for m in vals:
                if m['year'] == 2026 and m['value'] > 0:
                    if m['month'] > max_month: max_month = m['month']

    periodo_str = f"{format_month(max_month)} 2026"
    print(f"  📊 Datos hasta: {periodo_str}")

    # ── Aplanar árbol → DataFrame ──
    # Columnas: cadena_key, year, month + todas las métricas
    all_cadenas = set()
    for key in raw_tree:
        all_cadenas.update(raw_tree[key].keys())

    flat_rows = []
    for cadena_key in all_cadenas:
        for year in [2025, 2026]:
            for month in range(1, 13):
                row_data = {'cadena_key': cadena_key, 'year': year, 'month': month}

                # Hojas de valor único
                for sheet_key, value_col in [
                    ('ventas_netas', 'ventas_netas'),
                    ('ppto_ventas',  'ppto_ventas'),
                    ('cobro_envio',  'cobro_envio')
                ]:
                    if sheet_key in raw_tree and cadena_key in raw_tree[sheet_key]:
                        item = next((m for m in raw_tree[sheet_key][cadena_key]
                                     if m['year'] == year and m['month'] == month), None)
                        row_data[value_col] = item['value'] if item else 0.0
                    else:
                        row_data[value_col] = 0.0

                # Hojas de pares
                pair_map = {
                    'cancelaciones': ('CANCELADO',    'CANCEL_EST'),
                    'descuentos':    ('DESC_APLICADO','DESC_EST'),
                    'margen':        ('MARGEN_REAL',  'MARGEN_EST'),
                    'ap':            ('AP_EJEC',      'AP_PPTO'),
                    'overhead':      ('OH_EJEC',      'OH_PPTO'),
                    'admin':         ('ADMIN_EJEC',   'ADMIN_PPTO'),
                    'empaque':       ('EMP_EJEC',     'EMP_PPTO'),
                    'costo_envio':   ('ENV_EJEC',     'ENV_PPTO'),
                }
                for sheet_key, (col_r, col_p) in pair_map.items():
                    if sheet_key in raw_tree and cadena_key in raw_tree[sheet_key]:
                        item = next((m for m in raw_tree[sheet_key][cadena_key]
                                     if m['year'] == year and m['month'] == month), None)
                        row_data[col_r] = item.get(col_r, 0.0) if item else 0.0
                        row_data[col_p] = item.get(col_p, 0.0) if item else 0.0
                    else:
                        row_data[col_r] = 0.0
                        row_data[col_p] = 0.0

                flat_rows.append(row_data)

    df_flat = pd.DataFrame(flat_rows)
    df_flat['cadena_key'] = df_flat['cadena_key'].astype('category')
    df_flat['year']  = df_flat['year'].astype('int16')
    df_flat['month'] = df_flat['month'].astype('int8')

    # Convertir columnas numéricas
    numeric_cols = [c for c in df_flat.columns if c not in ('cadena_key', 'year', 'month')]
    for c in numeric_cols:
        df_flat[c] = pd.to_numeric(df_flat[c], errors='coerce').fillna(0.0)

    df_flat.to_parquet(PARQUET_PATH, engine='pyarrow', compression='snappy', index=False)
    size_kb = os.path.getsize(PARQUET_PATH) / 1024
    print(f"  ✅ Parquet → {PARQUET_PATH} ({size_kb:.0f} KB, {len(df_flat)} filas)")

    # ── Guardar META (JSON liviano) ──
    meta = {
        'tax_rates':   TAX_RATES,
        'MAX':         max_month,
        'periodo_str': periodo_str
    }
    with open(META_PATH, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False)
    print(f"  ✅ Meta → {META_PATH}")

    # ── Actualizar HTML (solo el badge visual del periodo) ──
    if os.path.exists(HTML_PATH):
        with open(HTML_PATH, 'r', encoding='utf-8') as f:
            html = f.read()
        html = re.sub(r'(<span id="v-periodo">)[^<]*(</span>)', f'\\g<1>{periodo_str}\\g<2>', html)
        html = re.sub(r'Periodo (Cerrado|Consolidado YTD) \| Actualización: [A-Za-z]+ \d{4}',
                      f'Periodo Consolidado YTD | Actualización: {periodo_str}', html)
        html = re.sub(r'Periodo Cerrado: [A-Za-z]+ \d{4}', f'Periodo Cerrado: {periodo_str}', html)
        with open(HTML_PATH, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"  ✅ HTML actualizado: {os.path.basename(HTML_PATH)}")

    print(f"\n✅ Proceso completado. Datos hasta: {periodo_str}")

if __name__ == "__main__":
    try:
        process_dashboard()
    except Exception as e:
        import traceback
        print(f"❌ Error: {str(e)}")
        traceback.print_exc()
