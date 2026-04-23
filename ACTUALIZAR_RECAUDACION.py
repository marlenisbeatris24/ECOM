import pandas as pd
import json
import os
import re
from datetime import datetime

# CONFIGURACIÓN DE RUTAS
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_PATH = os.path.join(BASE_DIR, 'Picking Log operaciones.xlsx')
JSON_PATH = os.path.join(BASE_DIR, 'recaudacion.json')

# 1 USD = X Moneda Local
EXCHANGE_RATES = {
    'GT': 7.78, 'PA': 1.0, 'SV': 1.0, 'CR': 518.5, 'HN': 24.7,
    'NI': 36.6, 'RD': 58.5, 'DO': 58.5, 'UR': 39.2, 'UY': 39.2, 'CO': 3950.0
}
COUNTRY_MAP = {
    'GT': 'GUATEMALA', 'PA': 'PANAMA', 'SV': 'EL SALVADOR', 'CR': 'COSTA RICA',
    'HN': 'HONDURAS', 'NI': 'NICARAGUA', 'RD': 'REP DOMINICANA', 'DO': 'REP DOMINICANA', 
    'UR': 'URUGUAY', 'UY': 'URUGUAY', 'CO': 'COLOMBIA'
}

class DataDictCompressor:
    def __init__(self):
        self.dict = {}
        self.list = []
    def add(self, string_val):
        if string_val not in self.dict:
            self.dict[string_val] = len(self.list)
            self.list.append(string_val)
        return -(self.dict[string_val] + 1)

def parse_tag(tag):
    s = str(tag).upper().strip()
    base = s.split('/')[0].strip()
    parts = base.split(' ')
    chain_raw = parts[0] if len(parts) > 0 else ''
    brand = 'OTRO'
    if 'SLA' in chain_raw or 'SPORTLINE' in chain_raw: brand = 'SPORTLINE'
    elif 'KICKS' in chain_raw or 'KIKCS' in chain_raw: brand = 'KICKS'
    elif 'CANCHA' in chain_raw: brand = 'LA CANCHA'
    elif 'CONVERSE' in chain_raw: brand = 'CONVERSE'
    
    country_raw = parts[-1] if len(parts) > 1 else ''
    code = 'PA' 
    valid_codes = ['GT','SV','CR','HN','NI','RD','UR','CO','PA','UY','DO']
    if country_raw in valid_codes:
        code = country_raw
    else:
        for vc in valid_codes:
            if s.endswith(vc):
                code = vc
                break
    return brand, code

def clean_currency(val):
    if pd.isna(val) or val == 'nan': return 0.0
    if isinstance(val, (int, float)): return float(val)
    try:
        s = str(val).replace(',', '').replace('$', '').strip()
        return float(re.findall(r'[-+]?\d*\.\d+|\d+', s)[0])
    except: return 0.0

def process_recaudacion():
    print(f"🚀 Procesando Análisis de Recaudación (Restaurado)...")
    dc = DataDictCompressor()
    if not os.path.exists(EXCEL_PATH): return

    xls = pd.ExcelFile(EXCEL_PATH)
    
    # 1. PRESUPUESTO
    df_ppto = pd.read_excel(xls, 'PRESUPUESTO DE VENTA')
    ppto_list = []
    for _, row in df_ppto.iterrows():
        p_str = str(row['PAIS ']).strip().upper()
        if 'TOTAL' in p_str or not p_str or p_str == 'NAN': continue
        if p_str == 'R DOMINICANA': p_str = 'REP DOMINICANA'
        
        cadena = str(row['CADENA ']).upper()
        brand = 'OTRO'
        if any(k in cadena for k in ['SLA', 'SPORTLINE']): brand = 'SPORTLINE'
        elif 'KICKS' in cadena: brand = 'KICKS'
        elif 'CANCHA' in cadena: brand = 'LA CANCHA'
        elif 'CONVERSE' in cadena: brand = 'CONVERSE'

        for col in df_ppto.columns[2:]:
            if isinstance(col, datetime):
                ppto_list.append({
                    'pais': p_str, 'brand': brand,
                    'periodo': col.strftime('%Y-%m'), 'usd': clean_currency(row[col])
                })

    # 2. ÓRDENES MENSUALES
    month_sheets = [s for s in xls.sheet_names if any(m in s.upper() for m in ['ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO','JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE'])]
    all_transactions = []
    
    for sheet in month_sheets:
        print(f"📦 Procesando {sheet}...")
        df = pd.read_excel(xls, sheet_name=sheet)
        for _, row in df.iterrows():
            ref_eco = str(row.get('Referencia Ecommerce', '')).strip()
            if not ref_eco or ref_eco == 'nan': continue
            
            etiqueta = str(row.get('Etiqueta de Orden', ''))
            brand, p_code = parse_tag(etiqueta)
            rate = EXCHANGE_RATES.get(p_code, 1.0)
            
            status = str(row.get('Estado Actual', '')).strip().lower()
            
            # VENTA NETA (Columna AY - Index 50)
            netsales_local = clean_currency(row.iloc[50])
            
            # LÓGICA SINCRONIZADA
            is_pending = ('pendiente' in status and 'pago' in status and 'verificado' in status)
            is_active = not is_pending and status != 'cancelado'
            
            fe_eco = row.get('Fecha Orden Ecommerce')
            if pd.isna(fe_eco): continue
            if isinstance(fe_eco, str):
                try: fe_eco = datetime.strptime(fe_eco, '%Y-%m-%d %H:%M:%S')
                except: continue

            all_transactions.append([
                dc.add(fe_eco.strftime('%Y-%m')), dc.add(COUNTRY_MAP.get(p_code, 'PANAMA')), 
                dc.add(brand), dc.add(str(row.get('Almacén', 'Ecommerce')).split('/')[0].strip()),
                dc.add(fe_eco.strftime('%Y-%m-%d')), dc.add(str(row.get('Orden Madre', ref_eco))),
                dc.add(ref_eco), round(netsales_local / rate, 2), rate,
                dc.add(str(row.get('Canal de Venta', 'Web Store'))), 1 if is_active else 0,
                dc.add(fe_eco.strftime('%Y')), dc.add(fe_eco.strftime('%m')),
                dc.add(str(row.get('Region', 'CENTRO'))), dc.add(str(row.iloc[6])),
                netsales_local # 15: Local Amount (Opcional)
            ])

    results = {
        'last_update': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'rates': EXCHANGE_RATES,
        'ppto': ppto_list,
        '__dict__': dc.list,
        'data': all_transactions
    }

    with open(JSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False)
    print(f"✅ Recaudación Restaurada: {JSON_PATH}")

if __name__ == "__main__":
    process_recaudacion()
