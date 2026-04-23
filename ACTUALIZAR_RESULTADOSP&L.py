import pandas as pd
import json
import os
import re

# CONFIGURACIÓN DE RUTAS
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_PATH = os.path.join(BASE_DIR, 'RESULTADOS.xlsx')
HTML_PATH = os.path.join(BASE_DIR, 'INFORME_RESULTADOS.html')
JSON_PATH = os.path.join(BASE_DIR, 'resultados_data.json')

TAX_RATES = {
    'COLOMBIA': 0.19, 'PANAMA': 0.07, 'EL SALVADOR': 0.13, 'GUATEMALA': 0.12,
    'HONDURAS': 0.15, 'NICARAGUA': 0.15, 'REP DOMINICANA': 0.18, 'COSTA RICA': 0.13, 'URUGUAY': 0.22
}

MESES = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]

class DataDictCompressor:
    def __init__(self):
        self.dict = {}
        self.list = []
    def add(self, string_val):
        if not isinstance(string_val, str): return string_val
        if string_val not in self.dict:
            self.dict[string_val] = len(self.list)
            self.list.append(string_val)
        return -(self.dict[string_val] + 1)

def recursive_compress(data, dc):
    if isinstance(data, dict):
        return {k: recursive_compress(v, dc) for k, v in data.items()}
    elif isinstance(data, list):
        return [recursive_compress(v, dc) for v in data]
    elif isinstance(data, str) and len(data) > 0:
        return dc.add(data)
    else:
        return data

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
    print("🚀 Iniciando actualización del Dashboard con corrección de Overhead...")
    
    if not os.path.exists(EXCEL_PATH):
        print(f"❌ Error: No se encuentra el archivo {EXCEL_PATH}")
        return

    xls = pd.ExcelFile(EXCEL_PATH)
    final_data = {"tax_rates": TAX_RATES}

    def get_sheet(name):
        n = name.strip().upper()
        for s in xls.sheet_names:
            if s.strip().upper() == n: return s
        return None

    # 1. PROCESAR HOJAS DE UNA SOLA COLUMNA
    single_col_sheets = {
        'VENTAS NETAS': 'ventas_netas',
        'PRESUPUESTO DE VENTAS': 'ppto_ventas',
        'COBRO POR ENVIO': 'cobro_envio'
    }

    for sheet_name, key in single_col_sheets.items():
        actual_name = get_sheet(sheet_name)
        if actual_name:
            df = pd.read_excel(xls, sheet_name=actual_name)
            sheet_dict = {}
            for _, row in df.iterrows():
                cadena = str(row.iloc[0]).strip()
                if not cadena or cadena == 'nan' or 'TOTAL' in cadena.upper() or 'CADENA' in cadena.upper(): continue
                vals = []
                for i in range(12):
                    vals.append({"year": 2025, "month": i+1, "value": to_float(row.iloc[i+1])})
                for i in range(12):
                    vals.append({"year": 2026, "month": i+1, "value": to_float(row.iloc[i+13])})
                sheet_dict[cadena] = vals
            final_data[key] = sheet_dict

    # 2. PROCESAR HOJAS CON PARES DE COLUMNAS (Swapped logic handled)
    pair_col_sheets = {
        'CANCELACIONES': ('cancelaciones', 'CANCELADO', 'CANCEL_EST', False),
        'DESCUENTOS': ('descuentos', 'DESC_APLICADO', 'DESC_EST', False),
        'MARGEN': ('margen', 'MARGEN_REAL', 'MARGEN_EST', False),
        'A&P': ('ap', 'AP_EJEC', 'AP_PPTO', False),
        'OVERHEAD': ('overhead', 'OH_EJEC', 'OH_PPTO', True), # Swapped: Ppto, Real
        'ADMIN': ('admin', 'ADMIN_EJEC', 'ADMIN_PPTO', False),
        'COSTO DE EMPAQUE': ('empaque', 'EMP_EJEC', 'EMP_PPTO', True), # Swapped: Ppto, Real
        'COSTO DE ENVIO': ('costo_envio', 'ENV_EJEC', 'ENV_PPTO', False)
    }

    for sheet_name, (key, f_real, f_ppto, is_swapped) in pair_col_sheets.items():
        actual_name = get_sheet(sheet_name)
        if not actual_name:
            if 'ADMIN' in sheet_name: actual_name = get_sheet('Admin expenses')
            elif 'MARGEN' in sheet_name: actual_name = get_sheet('MARGEN')
        
        if actual_name:
            df = pd.read_excel(xls, sheet_name=actual_name)
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
                    vals.append({"year": 2025, "month": i+1, f_real: v_r, f_ppto: v_p})
                idx_r_26 = 25 if not is_swapped else 26
                idx_p_26 = 26 if not is_swapped else 25
                for i in range(12):
                    v_r = to_float(row.iloc[idx_r_26 + i*2])
                    v_p = to_float(row.iloc[idx_p_26 + i*2])
                    vals.append({"year": 2026, "month": i+1, f_real: v_r, f_ppto: v_p})
                sheet_dict[cadena] = vals
            final_data[key] = sheet_dict

    # 3. DETECTAR ÚLTIMO MES CERRADO 2026
    max_month = 1
    if "ventas_netas" in final_data:
        for cadena_vals in final_data["ventas_netas"].values():
            for m in cadena_vals:
                if m["year"] == 2026 and m["value"] > 0:
                    if m["month"] > max_month: max_month = m["month"]
    
    periodo_str = f"{format_month(max_month)} 2026"
    final_data["MAX"] = max_month
    final_data["periodo_str"] = periodo_str
    print(f"📊 Datos procesados hasta: {periodo_str}")

    # 4. COMPRESIÓN Y GUARDADO JSON
    dc = DataDictCompressor()
    compressed_data = recursive_compress(final_data, dc)
    compressed_data['__dict__'] = dc.list

    with open(JSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(compressed_data, f, ensure_ascii=False)

    # 5. ACTUALIZAR HTML
    if os.path.exists(HTML_PATH):
        with open(HTML_PATH, 'r', encoding='utf-8') as f:
            html = f.read()
        # Solo actualizamos el header y periodos visuales en el HTML, no los datos
        html = re.sub(r'(<span id="v-periodo">)[^<]*(</span>)', f'\\g<1>{periodo_str}\\g<2>', html)
        html = re.sub(r'Periodo (Cerrado|Consolidado YTD) \| Actualización: [A-Za-z]+ \d{4}', 
                      f'Periodo Consolidado YTD | Actualización: {periodo_str}', html)
        html = re.sub(r'Periodo Cerrado: [A-Za-z]+ \d{4}', f'Periodo Cerrado: {periodo_str}', html)

        with open(HTML_PATH, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"✅ ¡Hecho! El archivo '{os.path.basename(HTML_PATH)}' ha sido actualizado y corregido.")
    else:
        print(f"⚠️ Aviso: No se encontró el archivo HTML.")

if __name__ == "__main__":
    try:
        process_dashboard()
    except Exception as e:
        print(f"❌ Error: {str(e)}")
