import pandas as pd
import json
import os
import re
from datetime import datetime

# CONFIGURACIÓN DE RUTAS
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_PATH     = os.path.join(BASE_DIR, 'Picking Log operaciones.xlsx')
PARQUET_PATH   = os.path.join(BASE_DIR, 'operaciones_data.parquet')
META_PATH      = os.path.join(BASE_DIR, 'operaciones_meta.json')

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
NAME_TO_CODE = {
    'GUATEMALA': 'GT', 'PANAMA': 'PA', 'EL SALVADOR': 'SV', 'COSTA RICA': 'CR',
    'HONDURAS': 'HN', 'NICARAGUA': 'NI', 'REP DOMINICANA': 'RD', 'R DOMINICANA': 'RD',
    'URUGUAY': 'UY', 'COLOMBIA': 'CO'
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

def normalize_brand(s):
    if not isinstance(s, str): return 'OTRO'
    s = s.upper()
    if any(k in s for k in ['SLA', 'SPORTLINE']): return 'SPORTLINE'
    if 'KICKS' in s: return 'KICKS'
    if 'CANCHA' in s: return 'LA CANCHA'
    if 'CONVERSE' in s: return 'CONVERSE'
    return 'OTRO'

def clean_currency(val):
    if pd.isna(val) or val == 'nan': return 0.0
    if isinstance(val, (int, float)): return float(val)
    try:
        s = str(val).replace(',', '').replace('$', '').strip()
        return float(re.findall(r'[-+]?\d*\.\d+|\d+', s)[0])
    except: return 0.0

def process_ops():
    print(f"🚀 Procesando Operaciones → Parquet...")
    if not os.path.exists(EXCEL_PATH):
        print(f"❌ No se encontró {EXCEL_PATH}")
        return

    xls = pd.ExcelFile(EXCEL_PATH)

    # 1. CANCELACIONES
    df_canc = pd.read_excel(xls, 'CANCELACIONES')
    map_canc = {}
    for _, row in df_canc.iterrows():
        ref = str(row.get('NÚMERO DE PEDIDO', '')).strip()
        tipo = str(row.get('TIPO DE SOLICITUD', '')).strip()
        if not ref or not tipo or "REASIGNAR TIENDA" in tipo.upper(): continue
        map_canc[ref] = tipo

    # 2. PRESUPUESTO
    df_ppto = pd.read_excel(xls, 'PRESUPUESTO DE VENTA')
    ppto_rows = []
    for _, row in df_ppto.iterrows():
        p_str = str(row['PAIS ']).strip().upper()
        if 'TOTAL' in p_str or not p_str or p_str == 'NAN': continue
        if p_str == 'R DOMINICANA': p_str = 'REP DOMINICANA'
        brand = normalize_brand(row['CADENA '])
        for col in df_ppto.columns[2:]:
            if isinstance(col, datetime):
                ppto_rows.append({
                    'pais': p_str, 'brand': brand,
                    'periodo': col.strftime('%Y-%m'), 'usd': clean_currency(row[col])
                })

    # 3. ÓRDENES MENSUALES
    month_sheets = [s for s in xls.sheet_names if any(m in s.upper() for m in [
        'ENERO','FEBRERO','MARZO','ABRIL','MAYO','JUNIO',
        'JULIO','AGOSTO','SEPTIEMBRE','OCTUBRE','NOVIEMBRE','DICIEMBRE'
    ])]
    all_orders = []

    for sheet in month_sheets:
        print(f"  📦 Procesando {sheet}...")
        df = pd.read_excel(xls, sheet_name=sheet)
        for _, row in df.iterrows():
            ref = str(row.get('Referencia Ecommerce', '')).strip()
            if not ref or ref == 'nan': continue

            etiqueta = str(row.get('Etiqueta de Orden', ''))
            brand, p_code = parse_tag(etiqueta)
            rate = EXCHANGE_RATES.get(p_code, 1.0)

            status = str(row.get('Estado Actual', '')).strip()
            status_low = status.lower()

            total_l = clean_currency(row.iloc[50])

            is_pending_pay = ('pendiente' in status_low and 'pago' in status_low and 'verificado' in status_low)
            is_pago_verificado = (status_low == 'pago verificado')
            is_fulfilled = not is_pending_pay and status_low != 'cancelado'

            dt_orden = row.get('Fecha Orden Ecommerce')
            if pd.isna(dt_orden): continue
            if isinstance(dt_orden, str):
                try: dt_orden = datetime.strptime(dt_orden, '%Y-%m-%d %H:%M:%S')
                except: continue

            dt_ent = row.iloc[13]
            days = None
            if pd.notna(dt_ent) and is_fulfilled:
                if isinstance(dt_ent, str):
                    try: dt_ent = datetime.strptime(dt_ent, '%Y-%m-%d %H:%M:%S')
                    except: dt_ent = None
                if dt_ent: days = (dt_ent - dt_orden).total_seconds() / 86400

            all_orders.append({
                'periodo':                dt_orden.strftime('%Y-%m'),
                'pais':                   COUNTRY_MAP.get(p_code, 'PANAMA'),
                'brand':                  brand,
                'tienda':                 str(row.get('Almacén', 'Ecommerce')).split('/')[0].strip(),
                'fecha':                  dt_orden.strftime('%Y-%m-%d %H:%M'),
                'is_fulfilled':           1 if is_fulfilled else 0,
                'total_usd':              round(total_l / rate, 2),
                'days':                   round(days, 2) if days else 0,
                'canc_motivo':            map_canc.get(ref, 'Rechazo / Abandono') if status_low == 'cancelado' else '',
                'brand_chart':            brand,
                'genero':                 str(row.get('Genero', 'Unisex')),
                'tipo':                   str(row.get('Tipo de Producto', 'Otros')),
                'city':                   str(row.get('Estado', 'Desconocido')),
                'sku':                    str(row.get('Sku', 'N/A')),
                'nombre':                 str(row.get('Nombre', 'Producto')),
                'carrier':                str(row.get('Transportista', 'Sin Transportista')) if pd.notna(row.get('Transportista')) and str(row.get('Transportista')).lower() != 'nan' else 'Sin Transportista',
                'is_cancelled_strict':    1 if status_low == 'cancelado' else 0,
                'is_pago_verificado':     1 if is_pago_verificado else 0,
                'metodo_entrega':         str(row.get('Pedido de venta/Método de entrega', 'Retiro en Tienda')),
                'qty':                    int(row.iloc[42]) if pd.notna(row.iloc[42]) else 1,
                'ref':                    ref,
                'fulfilled_v2':           1 if is_fulfilled else 0,
                'orden_madre':            str(row.iloc[1]).strip()
            })

    # --- GUARDAR PARQUET: ÓRDENES (único archivo) ---
    df_orders = pd.DataFrame(all_orders)

    # Optimizar tipos
    str_cols = ['periodo','pais','brand','tienda','fecha','canc_motivo','brand_chart',
                'genero','tipo','city','sku','nombre','carrier','metodo_entrega','ref','orden_madre']
    for c in str_cols:
        if c in df_orders.columns:
            df_orders[c] = df_orders[c].astype('category')

    df_orders.to_parquet(PARQUET_PATH, engine='pyarrow', compression='snappy', index=False)
    orig_kb = os.path.getsize(PARQUET_PATH) / 1024
    print(f"  ✅ Parquet → {PARQUET_PATH} ({orig_kb:.0f} KB)")

    # --- GUARDAR META (JSON liviano: filtros + ppto + tasas) ---
    all_periodos = sorted(list(set(o['periodo'] for o in all_orders)), reverse=True)
    all_paises   = sorted(list(set(o['pais']    for o in all_orders)))
    all_brands   = sorted(list(set(o['brand']   for o in all_orders)))
    meta = {
        'last_update': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'filters': {
            'countries': all_paises,
            'brands':    all_brands,
            'periods':   all_periodos
        },
        'rates':        EXCHANGE_RATES,
        'name_to_code': NAME_TO_CODE,
        'ppto':         ppto_rows  # pequeño, cabe perfecto en JSON
    }
    with open(META_PATH, 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False)
    print(f"  ✅ Meta   → {META_PATH}")
    print(f"\n✅ Proceso completado: {len(all_orders)} órdenes guardadas.")

if __name__ == "__main__":
    process_ops()
