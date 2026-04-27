import pandas as pd
import json
import os
import re
from datetime import datetime

# CONFIGURACIÓN DE RUTAS
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_PATH    = os.path.join(BASE_DIR, 'Picking Log operaciones.xlsx')
PARQUET_PATH  = os.path.join(BASE_DIR, 'recaudacion_data.parquet')
META_PATH     = os.path.join(BASE_DIR, 'recaudacion_meta.json')

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
    print(f"🚀 Procesando Recaudación → Parquet...")
    if not os.path.exists(EXCEL_PATH):
        print(f"❌ No se encontró {EXCEL_PATH}")
        return

    xls = pd.ExcelFile(EXCEL_PATH)

    # 1. PRESUPUESTO
    df_ppto = pd.read_excel(xls, 'PRESUPUESTO DE VENTA')
    ppto_rows = []
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
                ppto_rows.append({
                    'pais': p_str, 'brand': brand,
                    'periodo': col.strftime('%Y-%m'), 'usd': clean_currency(row[col])
                })

    # 2. ÓRDENES MENSUALES
    month_sheets = [s for s in xls.sheet_names if any(m in s.upper() for m in [
        'ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO',
        'JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE'
    ])]
    all_transactions = []

    for sheet in month_sheets:
        print(f"  📦 Procesando {sheet}...")
        df = pd.read_excel(xls, sheet_name=sheet)
        for _, row in df.iterrows():
            ref_eco = str(row.get('Referencia Ecommerce', '')).strip()
            if not ref_eco or ref_eco == 'nan': continue

            etiqueta = str(row.get('Etiqueta de Orden', ''))
            brand, p_code = parse_tag(etiqueta)
            rate = EXCHANGE_RATES.get(p_code, 1.0)

            status = str(row.get('Estado Actual', '')).strip().lower()

            netsales_local = clean_currency(row.iloc[50])

            is_pending = ('pendiente' in status and 'pago' in status and 'verificado' in status)
            is_active = not is_pending and status != 'cancelado'

            fe_eco = row.get('Fecha Orden Ecommerce')
            if pd.isna(fe_eco): continue
            if isinstance(fe_eco, str):
                try: fe_eco = datetime.strptime(fe_eco, '%Y-%m-%d %H:%M:%S')
                except: continue

            all_transactions.append({
                'periodo':        fe_eco.strftime('%Y-%m'),
                'pais':           COUNTRY_MAP.get(p_code, 'PANAMA'),
                'brand':          brand,
                'tienda':         str(row.get('Almacén', 'Ecommerce')).split('/')[0].strip(),
                'fecha':          fe_eco.strftime('%Y-%m-%d'),
                'orden_madre':    str(row.get('Orden Madre', ref_eco)),
                'ref':            ref_eco,
                'amount_usd':     round(netsales_local / rate, 2),
                'rate':           rate,
                'canal':          str(row.get('Canal de Venta', 'Web Store')),
                'is_active':      1 if is_active else 0,
                'year':           fe_eco.strftime('%Y'),
                'month':          fe_eco.strftime('%m'),
                'region':         str(row.get('Region', 'CENTRO')),
                'payment_method': str(row.iloc[6]),
                'local_amount':   netsales_local
            })

    # --- GUARDAR PARQUET: TRANSACCIONES (único archivo) ---
    df_data = pd.DataFrame(all_transactions)
    str_cols = ['periodo','pais','brand','tienda','fecha','orden_madre','ref',
                'canal','year','month','region','payment_method']
    for c in str_cols:
        if c in df_data.columns:
            df_data[c] = df_data[c].astype('category')

    df_data.to_parquet(PARQUET_PATH, engine='pyarrow', compression='snappy', index=False)
    print(f"  ✅ Data → {PARQUET_PATH} ({os.path.getsize(PARQUET_PATH)/1024:.0f} KB)")

    # --- GUARDAR META (JSON liviano: tasas + ppto) ---
    meta = {
        'last_update': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'rates': EXCHANGE_RATES,
        'ppto':  ppto_rows  # pequeño, cabe perfecto en JSON
    }
    with open(META_PATH, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False)
    print(f"  ✅ Meta → {META_PATH}")
    print(f"\n✅ Proceso completado: {len(all_transactions)} transacciones guardadas.")

if __name__ == "__main__":
    process_recaudacion()
