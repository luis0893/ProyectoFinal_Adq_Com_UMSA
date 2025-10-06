from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from meteostat import Daily, Hourly
import pandas as pd
import requests
import re
import sqlite3
import os
from pathlib import Path

DAG_DIR = Path(__file__).parent

csv_enviroment = str(DAG_DIR / "csv" / "Environment_Temperature_change_E_All_Data_NOFLAG.csv")
csv_faostat = str(DAG_DIR / "csv" / "FAOSTAT_data_en_11-1-2024.csv")

url = 'https://api.openweathermap.org/data/2.5/weather'

stations = [
    {"city": "La Paz", "name": "La Paz / Alto", "code": "85201", "lat": -16.5167, "lon": -68.1833, "country": "BO"},
    {"city": "Cochabamba", "name": "Cochabamba", "code": "85223", "lat": -17.4167, "lon": -66.1833, "country": "BO"},
    {"city": "Beni", 'name': 'Guayaramerin', 'code': '85033', 'lat': -10.8176, 'lon': -65.35, 'country': 'BO'},
    {"city": "Pando", 'name': 'Cobija', 'code': '85041', 'lat': -11.0333, 'lon': -68.7833, 'country': 'BO'},
    {"city": "Beni", 'name': 'Riberalta', 'code': '85043', 'lat': -11.0, 'lon': -66.1167, 'country': 'BO'},
    {"city": "Beni", "name": "San Joaquin", "code": "85104", "lat": -13.0667, "lon": -64.8167, "country": "BO"},
    {"city": "Santa Cruz", "name": "San Ramon", "code": "85109", "lat": -13.3, "lon": -64.7, "country": "BO"},
    {"city": "Beni", "name": "Magdalena", "code": "85114", "lat": -13.3333, "lon": -64.1167, "country": "BO"},
    {"city": "Beni", "name": "Santa Ana", "code": "85123", "lat": -13.7667, "lon": -65.4333, "country": "BO"},
    {"city": "Beni", "name": "Santa Rosa", "code": "85139", "lat": -14.1667, "lon": -66.9, "country": "BO"},
    {"city": "Beni", "name": "Reyes", "code": "85140", "lat": -14.3167, "lon": -67.3833, "country": "BO"},
    {"city": "La Paz", "name": "Rurrenabaque", "code": "85141", "lat": -14.4667, "lon": -67.5667, "country": "BO"},
    {"city": "La Paz", "name": "Apolo", "code": "85151", "lat": -14.7333, "lon": -68.5, "country": "BO"},
    {"city": "Beni", "name": "San Borja", "code": "85152", "lat": -14.8667, "lon": -66.8667, "country": "BO"},
    {"city": "Beni", "name": "San Ignacio De Moxos", "code": "85153", "lat": -14.9167, "lon": -65.6, "country": "BO"},
    {"city": "Beni", "name": "Trinidad", "code": "85154", "lat": -14.8167, "lon": -64.9167, "country": "BO"},
    {"city": "Santa Cruz", "name": "Ascencion De Guarayos", "code": "85175", "lat": -15.7167, "lon": -63.1,
     "country": "BO"},
    {"city": "Santa Cruz", "name": "San Javier", "code": "85195", "lat": -16.2667, "lon": -62.4667, "country": "BO"},
    {"city": "Santa Cruz", "name": "Concepcion", "code": "85196", "lat": -16.15, "lon": -62.0167, "country": "BO"},
    {"city": "Santa Cruz", "name": "San Ignacio De Velasco", "code": "85207", "lat": -16.3833, "lon": -60.9667,
     "country": "BO"},
    {"city": "Santa Cruz", "name": "San Matias", "code": "85210", "lat": -16.3333, "lon": -58.3833, "country": "BO"},
    {"city": "Cochabamba", "name": "Cochabamba", "code": "85223", "lat": -17.4167, "lon": -66.1833, "country": "BO"},
    {"city": "La Paz", "name": "Charana", "code": "85230", "lat": -17.5833, "lon": -69.6, "country": "BO"},
    {"city": "Oruro", "name": "Oruro", "code": "85242", "lat": -17.9667, "lon": -67.0667, "country": "BO"},
    {"city": "Santa Cruz", "name": "Viru-Viru", "code": "85244", "lat": -17.6333, "lon": -63.1333, "country": "BO"},
    {"city": "Santa Cruz", "name": "Santa Cruz / El Trompillo", "code": "85245", "lat": -17.8, "lon": -63.1833,
     "country": "BO"},
    {"city": "Santa Cruz", "name": "San Jose De Chiquitos", "code": "85247", "lat": -17.8, "lon": -60.7333,
     "country": "BO"},
    {"city": "Santa Cruz", "name": "Vallegrande", "code": "85264", "lat": -18.4667, "lon": -64.1, "country": "BO"},
    {"city": "Santa Cruz", "name": "Robore", "code": "85268", "lat": -18.3167, "lon": -59.7667, "country": "BO"},
    {"city": "Chuquisaca", "name": "Sucre", "code": "85283", "lat": -19.0167, "lon": -65.3, "country": "BO"},
    {"city": "Santa Cruz", "name": "Puerto Suarez", "code": "85289", "lat": -18.9833, "lon": -57.8167, "country": "BO"},
    {"city": "Potosi", "name": "Potosi", "code": "85293", "lat": -19.55, "lon": -65.7333, "country": "BO"},
    {"city": "Chuquisaca", "name": "Monteagudo", "code": "85312", "lat": -19.7833, "lon": -63.9167, "country": "BO"},
    {"city": "Santa Cruz", "name": "Camiri", "code": "85315", "lat": -20.0, "lon": -63.5333, "country": "BO"},
    {"city": "Santa Cruz", "name": "Villamontes", "code": "85345", "lat": -21.25, "lon": -63.45, "country": "BO"},
    {"city": "Tarija", "name": "Tarija", "code": "85364", "lat": -21.55, "lon": -64.7, "country": "BO"},
    {"city": "Tarija", "name": "Yacuiba", "code": "85365", "lat": -21.95, "lon": -63.65, "country": "BO"},
    {"city": "Tarija", "name": "Bermejo", "code": "85394", "lat": -22.7667, "lon": -64.3167, "country": "BO"},
    {"city": "Chuquisaca", "name": "Monteagudo", "code": "SLAG0", "lat": -19.827, "lon": -63.961, "country": "BO"},
    {"city": "Pando", "name": "Baures", "code": "SLBU0", "lat": -13.6592, "lon": -63.7028, "country": "BO"},
    {"city": "Potosi", "name": "Collpani / Huaycho", "code": "SLCL0", "lat": -15.8, "lon": -68.8, "country": "BO"},
    {"city": "Santa Cruz", "name": "San Rámon", "code": "SLRA0", "lat": -13.2639, "lon": -64.6039, "country": "BO"},
    {"city": "Santa Cruz", "name": "Santa Rosa De Yacuma / San Joaquín", "code": "SLSR0", "lat": -14.0662,
     "lon": -66.7868, "country": "BO"},
    {"city": "Santa Cruz", "name": "San Matías", "code": "SLTI0", "lat": -16.368, "lon": -58.461, "country": "BO"}
]

OPENWEATHER_API_KEY = "c5a664c56d367e1800469fa320ba6beb"
cities = ['La Paz', 'Santa Cruz de la Sierra', 'Cochabamba', 'Oruro', 'Beni', 'Potosi', 'Tarija', 'Pando', 'Sucre']

mapa_meses = {
    "Jan": "January",
    "Feb": "February",
    "Mar": "March",
    "Apr": "April",
    "May": "May",
    "Jun": "June",
    "Jul": "July",
    "Aug": "August",
    "Sep": "September",
    "Oct": "October",
    "Nov": "November",
    "Dec": "December"
}


def separar_meses(s):
    return re.findall(r'[A-Z][a-z]{2}', s)


def clean_csv_data(df):
    df = df.dropna(how='all', axis=1)
    df.columns = [c.strip().replace(" ", "_").lower() for c in df.columns]
    return df


def temp_bucket(temp):
    if temp is None:
        return None
    try:
        temp = float(temp)
    except Exception:
        return None
    if temp < 10:
        return "frío"
    elif temp < 25:
        return "templado"
    else:
        return "caliente"


def wind_bucket(wind):
    if wind is None:
        return None
    try:
        wind = float(wind)
    except Exception:
        return None
    if wind < 2:
        return "calmo"
    elif wind < 6:
        return "moderado"
    else:
        return "fuerte"


def fetch_city_weather(name, country, api_key):
    r = requests.get(url, params={'q': f'{name},{country}', 'appid': api_key, 'units': 'metric'}, timeout=30)
    r.raise_for_status()
    data = r.json()

    lat, lon = data['coord'].get('lat'), data['coord'].get('lon')
    temp_c = data['main'].get('temp')
    wind_mps = data['wind'].get('speed')

    return {
        'date': datetime.utcnow().strftime("%Y-%m-%d"),
        'country': data.get('sys', {}).get('country', country),
        'city': name,
        'lat': lat,
        'lon': lon,
        'temp_c': temp_c,
        'temp_min_c': data['main'].get('temp_min'),
        'temp_max_c': data['main'].get('temp_max'),
        'humidity_pct': data['main'].get('humidity'),
        'pressure_hpa': data['main'].get('pressure'),
        'wind_mps': wind_mps,
        'weather_main': (data.get('weather') or [{}])[0].get('main'),
        'source': 'openweather_api',
        'anomaly_c': None,
        'temp_bucket': temp_bucket(temp_c),
        'wind_bucket': wind_bucket(wind_mps),
        'year_hist': None,
        'country_hist': None
    }


def csv_ingestion(file_path, file_name, **context):
    """Función robusta para ingesta de CSV que guarda en XCom"""
    try:
        print(f"📥 Intentando leer: {file_path}")

        if not os.path.exists(file_path):
            print(f"⚠️  Archivo no encontrado: {file_path}")
            print("📋 Creando DataFrame vacío para continuar...")

            # Crear DataFrame vacío con estructura esperada
            empty_df = pd.DataFrame(columns=['area', 'months', 'unit', 'value', 'year'])
            context['ti'].xcom_push(key=file_name, value=empty_df.to_dict(orient='records'))
            return f"{file_name} - Sin datos"

        # Si el archivo existe, procesarlo normalmente
        df = clean_csv_data(pd.read_csv(file_path, encoding='latin1'))
        print(f"✅ {file_name} procesado exitosamente - Filas: {len(df)}")
        print(f"📊 Columnas: {df.columns.tolist()}")

        # Guardar en XCom para que otras tareas puedan acceder
        context['ti'].xcom_push(key=file_name, value=df.to_dict(orient='records'))
        return f"{file_name} - {len(df)} filas"

    except Exception as e:
        print(f"❌ Error procesando {file_path}: {e}")
        # Crear DataFrame vacío para que el pipeline continúe
        empty_df = pd.DataFrame()
        context['ti'].xcom_push(key=file_name, value=empty_df.to_dict(orient='records'))
        return f"{file_name} - Error: {str(e)}"


def ingesta_csv(**context):
    """Función principal de ingesta CSV que procesa ambos archivos"""
    print("🚀 Iniciando ingesta de CSV...")

    # Procesar ambos archivos y guardar en XCom
    resultado_env = csv_ingestion(csv_enviroment, 'environment_data', **context)
    resultado_fao = csv_ingestion(csv_faostat, 'faostat_data', **context)

    print(f"📈 Resultados:")
    print(f"   - Environment: {resultado_env}")
    print(f"   - FAOSTAT: {resultado_fao}")

    return "CSV ingestion completed"


def transformacion_csv(**context):
    df_temp_raw = context['ti'].xcom_pull(key='environment_data', task_ids='ingesta_csv')
    df_temp = pd.DataFrame(df_temp_raw)

    if df_temp.empty:
        print("⚠️ No hay datos de environment_data para transformar")
        context['ti'].xcom_push(key='transformed_csv', value=pd.DataFrame().to_dict(orient='records'))
        return "No hay datos para transformacion_csv"

    df_largo = df_temp.melt(
        id_vars=['area', 'months', 'unit'],
        value_vars=[col for col in df_temp.columns if str(col).startswith('y')],
        var_name='year',
        value_name='value'
    )

    df_largo['year'] = df_largo['year'].astype(str).str.replace('y', '', regex=False)
    df_largo['year'] = pd.to_numeric(df_largo['year'], errors='coerce')
    df_bolivia1 = df_largo[df_largo['area'].str.contains('Bolivia', case=False, na=False)]
    context['ti'].xcom_push(key='transformed_csv', value=df_bolivia1.to_dict(orient='records'))
    return "Transformación CSV completada"


def fao_transform(**context):
    df_fao_raw = context['ti'].xcom_pull(key='faostat_data', task_ids='ingesta_csv')
    df_fao = pd.DataFrame(df_fao_raw)

    if df_fao.empty:
        print("⚠️ No hay datos de faostat_data para transformar")
        context['ti'].xcom_push(key='fao_transformed', value=pd.DataFrame().to_dict(orient='records'))
        return "No hay datos para fao_transform"

    # Intentar seleccionar columnas seguras
    cols = [c for c in df_fao.columns if c in ['area', 'months', 'unit', 'value', 'year']]
    if not cols:
        print("⚠️ Las columnas esperadas no existen en FAO, devolviendo vacío")
        context['ti'].xcom_push(key='fao_transformed', value=pd.DataFrame().to_dict(orient='records'))
        return "FAO transform vacio"

    df_filtrado = df_fao[cols].copy()
    df_filtrado['unit'] = df_filtrado.get('unit', '').astype(str).str.replace('Â°c', '°C', regex=False)
    df_bolivia2 = df_filtrado[df_filtrado['area'].str.contains('Bolivia', case=False, na=False)]
    context['ti'].xcom_push(key='fao_transformed', value=df_bolivia2.to_dict(orient='records'))
    return "Transformación FAO completada"


def unir_csv(**context):
    df_bolivia1_raw = context['ti'].xcom_pull(key='transformed_csv', task_ids='transformacion_csv')
    df_bolivia2_raw = context['ti'].xcom_pull(key='fao_transformed', task_ids='fao_transform')

    df_bolivia1 = pd.DataFrame(df_bolivia1_raw)
    df_bolivia2 = pd.DataFrame(df_bolivia2_raw)

    if df_bolivia1.empty and df_bolivia2.empty:
        context['ti'].xcom_push(key='unified_csv', value=pd.DataFrame().to_dict(orient='records'))
        return "No hay datos para unir"

    df_bolivia = pd.concat([df_bolivia1, df_bolivia2], ignore_index=True).reset_index(drop=True)
    df_bolivia.index = df_bolivia.index + 1
    df_bolivia = df_bolivia.sort_values('year', ascending=True)
    context['ti'].xcom_push(key='unified_csv', value=df_bolivia.to_dict(orient='records'))
    return "Unión de CSV completada"


def openweather(**context):
    records = []
    for city in cities:
        try:
            rec = fetch_city_weather(city, 'BO', OPENWEATHER_API_KEY)
            records.append(rec)
        except Exception as e:
            print(f"❌ Error en {city} -> {e}")

    df = pd.DataFrame(records)
    context['ti'].xcom_push(key='openweather_data', value=df.to_dict(orient='records'))
    return "OpenWeather data collected"


def limpieza_openweather(**context):
    records_raw = context['ti'].xcom_pull(key='openweather_data', task_ids='openweather')
    records = pd.DataFrame(records_raw) if records_raw else pd.DataFrame()

    if not records.empty:
        avg_temp_c = records['temp_c'].mean()
        now = datetime.utcnow()
        current_year = now.year
        current_month = now.month

        formatted_data = {
            'area': 'Bolivia (Plurinational State of)',
            'months': current_month,
            'year': current_year,
            'unit': '°C',
            'value': round(avg_temp_c, 2)
        }

        df_final = pd.DataFrame([formatted_data])
        context['ti'].xcom_push(key='cleaned_openweather', value=df_final.to_dict(orient='records'))
        return "Limpieza OpenWeather completada"
    else:
        context['ti'].xcom_push(key='cleaned_openweather', value=pd.DataFrame().to_dict(orient='records'))
        return "No hay datos de OpenWeather"


def meteostat(**context):
    hoy = datetime.today()
    start = datetime(2020, 1, 1)
    end = datetime(hoy.year, hoy.month, hoy.day)
    datos_finales = []

    for s in stations:
        try:
            daily_data = Daily(s['code'], start, end).fetch()
            if not daily_data.empty:
                data_reset = daily_data.reset_index()
                data_reset['months'] = data_reset['time'].dt.strftime('%B')
                data_reset['year'] = data_reset['time'].dt.year

                mensual = data_reset.groupby(['year', 'months'])['tavg'].mean().reset_index()
                for _, fila in mensual.iterrows():
                    if pd.notna(fila['tavg']):
                        datos_finales.append({
                            'area': 'Bolivia (Plurinational State of)',
                            'months': fila['months'],
                            'year': fila['year'],
                            'unit': '°C',
                            'value': round(fila['tavg'], 2)
                        })
        except Exception as e:
            print(f"Error con {s['city']}: {e}")
            continue

    context['ti'].xcom_push(key='meteostat_raw', value=datos_finales)
    return "Meteostat data collected"


def limpieza_meteo(**context):
    datos_finales_raw = context['ti'].xcom_pull(key='meteostat_raw', task_ids='meteostat')

    if datos_finales_raw:
        df_boliviaM = pd.DataFrame(datos_finales_raw)
        mes_orden = ['January', 'February', 'March', 'April', 'May', 'June',
                     'July', 'August', 'September', 'October', 'November', 'December']
        df_boliviaM['months'] = pd.Categorical(df_boliviaM['months'], categories=mes_orden, ordered=True)
        df_boliviaM = df_boliviaM.sort_values(['year', 'months'])
        context['ti'].xcom_push(key='meteostat_data', value=df_boliviaM.to_dict(orient='records'))
        return "Limpieza Meteostat completada"
    else:
        print("❌ No se pudieron obtener datos de Meteostat")
        context['ti'].xcom_push(key='meteostat_data', value=pd.DataFrame().to_dict(orient='records'))
        return "No hay datos de Meteostat"


def limpieza_final(**context):
    df_unified_raw = context['ti'].xcom_pull(key='unified_csv', task_ids='union_csv')
    df_boliviaF = pd.DataFrame(df_unified_raw) if df_unified_raw else pd.DataFrame()

    if not df_boliviaF.empty:
        df_boliviaF["months"] = df_boliviaF["months"].astype(str).str.replace("â", "-", regex=False)
        df_boliviaF["meses"] = df_boliviaF["months"].str.split("-")
        df_expandido = df_boliviaF.explode("meses").reset_index(drop=True)
        df_expandido["meses"] = df_expandido["meses"].str.strip()
        df_expandido["meses"] = df_expandido["meses"].str.replace(r"[^A-Za-z]", "", regex=True)
        df_expandido["meses"] = df_expandido["meses"].apply(separar_meses)
        df_expandido = df_expandido.explode("meses").reset_index(drop=True)
        df_expandido["meses"] = df_expandido["meses"].map(mapa_meses).fillna(df_expandido["meses"])
        df_expandido = df_expandido[~df_expandido["meses"].str.contains("Met", na=False)].reset_index(drop=True)
        df_unico = df_expandido.groupby(['area', 'meses', 'year', 'unit'])['value'].mean().reset_index()
        df_unico["temp_categoria"] = df_unico["value"].apply(temp_bucket)

        context['ti'].xcom_push(key='final_cleaned_data', value=df_unico.to_dict(orient='records'))
        return "Limpieza final completada"
    else:
        context['ti'].xcom_push(key='final_cleaned_data', value=pd.DataFrame().to_dict(orient='records'))
        return "No hay datos para limpiar"


def verificar(**context):
    try:
        # Recuperar datos de todas las fuentes
        # Datos de CSV procesados
        final_cleaned_data_raw = context['ti'].xcom_pull(key='final_cleaned_data', task_ids='limpieza_final')

        # Datos de APIs
        owm_data_raw = context['ti'].xcom_pull(key='cleaned_openweather', task_ids='limpieza_openweather')
        meteostat_data_raw = context['ti'].xcom_pull(key='meteostat_data', task_ids='limpieza_meteo')

        # Convertir a DataFrames
        csv_data = pd.DataFrame(final_cleaned_data_raw) if final_cleaned_data_raw else pd.DataFrame()
        owm_data = pd.DataFrame(owm_data_raw) if owm_data_raw else pd.DataFrame()
        meteostat_data = pd.DataFrame(meteostat_data_raw) if meteostat_data_raw else pd.DataFrame()

        print(f"📊 Datos recibidos - CSV: {csv_data.shape}, OWM: {owm_data.shape}, Meteostat: {meteostat_data.shape}")

        # Verificar si al menos hay datos de alguna fuente
        if csv_data.empty and owm_data.empty and meteostat_data.empty:
            raise ValueError("❌ No hay datos de ninguna fuente")

        # Preparar datos para unificación
        datasets = []

        # Procesar datos CSV si existen
        if not csv_data.empty:
            # Renombrar columnas para consistencia
            csv_processed = csv_data.rename(columns={'meses': 'months'})
            csv_processed['source'] = 'csv_historical'
            datasets.append(csv_processed)
            print("✅ Datos CSV listos para unir")

        # Procesar datos de OpenWeather si existen
        if not owm_data.empty:
            owm_processed = owm_data.copy()
            # Asegurar que tenga las columnas necesarias
            if 'months' not in owm_processed.columns:
                now = datetime.utcnow()
                owm_processed['months'] = now.month
            owm_processed['source'] = 'openweather'
            datasets.append(owm_processed)
            print("✅ Datos OpenWeather listos para unir")

        # Procesar datos de Meteostat si existen
        if not meteostat_data.empty:
            meteostat_processed = meteostat_data.copy()
            meteostat_processed['source'] = 'meteostat'
            datasets.append(meteostat_processed)
            print("✅ Datos Meteostat listos para unir")

        # Unir todos los datasets
        if len(datasets) == 1:
            combined_df = datasets[0]
            print(f"✅ Solo una fuente de datos - USANDO: {combined_df['source'].iloc[0]}")
        else:
            # Unir múltiples datasets
            combined_df = pd.concat(datasets, ignore_index=True)
            print(f"✅ {len(datasets)} fuentes de datos unidas exitosamente")

        # Estandarizar columnas y tipos de datos
        columnas_esperadas = ['area', 'months', 'year', 'unit', 'value', 'source']

        # Asegurar que todas las columnas esperadas existan
        for col in columnas_esperadas:
            if col not in combined_df.columns:
                combined_df[col] = None

        # Convertir tipos de datos
        if 'year' in combined_df.columns:
            combined_df['year'] = pd.to_numeric(combined_df['year'], errors='coerce')
        if 'value' in combined_df.columns:
            combined_df['value'] = pd.to_numeric(combined_df['value'], errors='coerce')
        if 'months' in combined_df.columns:
            # Convertir meses a formato consistente
            combined_df['months'] = combined_df['months'].astype(str)

        # Ordenar datos
        if 'year' in combined_df.columns and 'months' in combined_df.columns:
            # Crear categorías para meses
            mes_orden = ['January', 'February', 'March', 'April', 'May', 'June',
                         'July', 'August', 'September', 'October', 'November', 'December',
                         '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']

            # Convertir números de mes a nombres
            month_mapping = {
                '1': 'January', '2': 'February', '3': 'March', '4': 'April',
                '5': 'May', '6': 'June', '7': 'July', '8': 'August',
                '9': 'September', '10': 'October', '11': 'November', '12': 'December'
            }
            combined_df['months'] = combined_df['months'].replace(month_mapping)

            combined_df['months'] = pd.Categorical(combined_df['months'],
                                                   categories=mes_orden,
                                                   ordered=True)
            combined_df = combined_df.sort_values(['year', 'months'])

        # Agregar categorías de temperatura
        if 'value' in combined_df.columns:
            combined_df['temp_categoria'] = combined_df['value'].apply(temp_bucket)

        # Agregar timestamp de procesamiento
        combined_df['processed_at'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # Mostrar información del dataset resultante
        print(f"🎯 Dataset final unificado: {combined_df.shape}")
        print("📋 Columnas:", combined_df.columns.tolist())
        print("🔍 Primeras filas:")
        print(combined_df.head())

        # Mostrar distribución por fuente
        if 'source' in combined_df.columns:
            print("📊 Distribución por fuente:")
            print(combined_df['source'].value_counts())

        # Verificar que el dataset no esté vacío
        if combined_df.empty:
            raise ValueError("❌ El dataset resultante está vacío")

        # Pushear datos combinados a XCom
        context['ti'].xcom_push(key='combined_data', value=combined_df.to_dict(orient='records'))

        # También guardar información de metadatos
        metadata = {
            'total_records': len(combined_df),
            'sources': combined_df['source'].value_counts().to_dict() if 'source' in combined_df.columns else {},
            'date_range': {
                'min_year': combined_df['year'].min() if 'year' in combined_df.columns else None,
                'max_year': combined_df['year'].max() if 'year' in combined_df.columns else None
            },
            'processed_at': datetime.utcnow().isoformat()
        }
        context['ti'].xcom_push(key='processing_metadata', value=metadata)

        return f"✅ Datos unificados exitosamente: {len(combined_df)} registros de {combined_df['source'].nunique() if 'source' in combined_df.columns else 1} fuentes"

    except Exception as e:
        print(f"❌ Error en verificación y unificación: {e}")
        raise


def guardar(**context):
    import json
    import sqlite3
    import pandas as pd

    combined_data_raw = context['ti'].xcom_pull(key='combined_data', task_ids='verificar')
    metadata_raw = context['ti'].xcom_pull(key='processing_metadata', task_ids='verificar')

    df = pd.DataFrame(combined_data_raw) if combined_data_raw else pd.DataFrame()
    metadata = metadata_raw if metadata_raw else {}

    if df.empty:
        print("❌ No hay datos para guardar")
        return

    try:
        # Conexión a la base de datos SQLite
        conn = sqlite3.connect("/tmp/clima5.db")

        # Guardar datos principales
        df.to_sql("clima_unificado", conn, if_exists='replace', index=False)

        # Normalizar metadatos para evitar errores de tipos no soportados
        safe_metadata = {}
        for k, v in (metadata.items() if isinstance(metadata, dict) else []):
            try:
                safe_metadata[k] = json.dumps(v, default=str)
            except Exception:
                safe_metadata[k] = str(v)

        if isinstance(metadata, dict):
            sources = metadata.get('sources')
            date_range = metadata.get('date_range')
            safe_metadata['sources'] = json.dumps(sources, default=str) if sources is not None else None
            safe_metadata['date_range'] = json.dumps(date_range, default=str) if date_range is not None else None
            safe_metadata['total_records'] = metadata.get('total_records')
            safe_metadata['processed_at'] = metadata.get('processed_at')

        # Guardar metadatos
        metadata_df = pd.DataFrame([safe_metadata])
        metadata_df.to_sql("procesamiento_metadata", conn, if_exists='replace', index=False)

        # 🔹 Exportar los datos principales a CSV (desde la tabla SQLite)
        csv_filename = "/tmp/clima5.csv"
        df_from_db = pd.read_sql_query("SELECT * FROM clima_unificado", conn)
        df_from_db.to_csv(csv_filename, index=False, encoding='utf-8')
        print(f"💾 CSV guardado en {csv_filename} con {len(df_from_db)} registros")

        conn.close()

        print(f"✅ Guardado {len(df)} registros en la base de datos")
        print(f"📊 Fuentes de datos: {safe_metadata.get('sources', {})}")
        print(f"📅 Rango de años: {safe_metadata.get('date_range', {})}")

        return f"Datos unificados guardados exitosamente: {len(df)} registros"

    except Exception as e:
        print(f"❌ Error al guardar en la base de datos: {e}")
        raise


def reporte(**context):
    import sqlite3
    import pandas as pd
    import matplotlib.pyplot as plt
    import base64
    from io import BytesIO
    import os

    print("📊 Generando reporte HTML...")

    # Ruta del archivo HTML de salida
    output_path = "/var/www/html/air/reporte_clima.html"

    # Ruta de la base de datos
    db_path = "/tmp/clima5.db"

    # Conectarse a la base de datos
    conn = sqlite3.connect(db_path)

    # ===================== CONSULTAS =====================
    query1 = """
        SELECT area, year, AVG(value) AS promedio_valor
        FROM clima_unificado
        GROUP BY area, year
        ORDER BY area, year;
    """

    query2 = """
        SELECT months, AVG(value) AS promedio_mensual
        FROM clima_unificado
        GROUP BY months
        ORDER BY CAST(months AS INTEGER);
    """

    query3 = """
        SELECT temp_categoria, AVG(value) AS promedio_valor, COUNT(*) AS registros
        FROM clima_unificado
        GROUP BY temp_categoria
        ORDER BY promedio_valor DESC;
    """

    df_area = pd.read_sql_query(query1, conn)
    df_meses = pd.read_sql_query(query2, conn)
    df_temp = pd.read_sql_query(query3, conn)

    conn.close()

    # ===================== GENERAR GRÁFICOS =====================

    def fig_to_base64(fig):
        buf = BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        buf.seek(0)
        return base64.b64encode(buf.read()).decode("utf-8")

    # --- Gráfico 1: Promedio por área y año ---
    fig1, ax1 = plt.subplots(figsize=(8, 4))
    for area, group in df_area.groupby("area"):
        ax1.plot(group["year"], group["promedio_valor"], marker='o', label=area)
    ax1.set_title("Promedio anual por área")
    ax1.set_xlabel("Año")
    ax1.set_ylabel("Promedio de valor")
    ax1.legend()
    img1 = fig_to_base64(fig1)
    plt.close(fig1)

    # --- Gráfico 2: Promedio mensual ---
    fig2, ax2 = plt.subplots(figsize=(8, 4))
    ax2.bar(df_meses["months"], df_meses["promedio_mensual"], color="skyblue")
    ax2.set_title("Promedio mensual")
    ax2.set_xlabel("Mes")
    ax2.set_ylabel("Valor promedio")
    img2 = fig_to_base64(fig2)
    plt.close(fig2)

    # --- Gráfico 3: Categorías de temperatura ---
    fig3, ax3 = plt.subplots(figsize=(8, 4))
    ax3.bar(df_temp["temp_categoria"], df_temp["promedio_valor"], color="orange")
    ax3.set_title("Promedio por categoría de temperatura")
    ax3.set_xlabel("Categoría")
    ax3.set_ylabel("Promedio de valor")
    plt.xticks(rotation=45)
    img3 = fig_to_base64(fig3)
    plt.close(fig3)

    # ===================== CREAR HTML =====================
    html_content = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Reporte Climático</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 40px;
                background-color: #f8f9fa;
            }}
            h1 {{
                color: #2c3e50;
            }}
            h2 {{
                color: #34495e;
            }}
            img {{
                max-width: 90%;
                margin: 20px 0;
                border-radius: 10px;
                box-shadow: 0 0 5px rgba(0,0,0,0.2);
            }}
            table {{
                border-collapse: collapse;
                width: 90%;
                margin-bottom: 30px;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 8px;
                text-align: center;
            }}
            th {{
                background-color: #2c3e50;
                color: white;
            }}
        </style>
    </head>
    <body>
        <h1>🌦️ Reporte Climático</h1>

        <h2>Promedio anual por área</h2>
        <img src="data:image/png;base64,{img1}" alt="Promedio anual por área">
        {df_area.to_html(index=False)}

        <h2>Promedio mensual</h2>
        <img src="data:image/png;base64,{img2}" alt="Promedio mensual">
        {df_meses.to_html(index=False)}

        <h2>Promedio por categoría de temperatura</h2>
        <img src="data:image/png;base64,{img3}" alt="Promedio por categoría">
        {df_temp.to_html(index=False)}

        <p>Generado automáticamente desde Apache Airflow 🚀</p>
    </body>
    </html>
    """

    # Crear directorio si no existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Guardar HTML
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"✅ Reporte generado correctamente: {output_path}")

    return f"Reporte HTML generado en {output_path}"


def create_variables():
    print("Creando Pipeline...")


# Definir tareas

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'CLIMA360',
    default_args=default_args,
    description='Pipeline de Climatologico',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# ==================== TAREAS DE INGESTIÓN ====================

variables_task = PythonOperator(
    task_id="variables",
    python_callable=create_variables,
    dag=dag,
)

# Ingestion CSV
ingestion_csv_task = PythonOperator(
    task_id="ingesta_csv",
    python_callable=ingesta_csv,
    provide_context=True,
    dag=dag,
)

# Ingestion OpenWeather
ingestion_openweather_task = PythonOperator(
    task_id="openweather",
    python_callable=openweather,
    provide_context=True,
    dag=dag,
)

# Ingestion Meteostat
ingestion_meteostat_task = PythonOperator(
    task_id="meteostat",
    python_callable=meteostat,
    provide_context=True,
    dag=dag,
)

# ==================== TAREAS DE TRANSFORMACIÓN ====================

# Transformación CSV
transformacion_csv_task = PythonOperator(
    task_id="transformacion_csv",
    python_callable=transformacion_csv,
    provide_context=True,
    dag=dag,
)

transformacion_fao_task = PythonOperator(
    task_id="fao_transform",
    python_callable=fao_transform,
    provide_context=True,
    dag=dag,
)

union_csv_task = PythonOperator(
    task_id="union_csv",
    python_callable=unir_csv,
    provide_context=True,
    dag=dag,
)

limpieza_final_csv_task = PythonOperator(
    task_id="limpieza_final",
    python_callable=limpieza_final,
    provide_context=True,
    dag=dag,
)

# Transformación OpenWeather
transformacion_openweather_task = PythonOperator(
    task_id="limpieza_openweather",
    python_callable=limpieza_openweather,
    provide_context=True,
    dag=dag,
)

# Transformación Meteostat
transformacion_meteostat_task = PythonOperator(
    task_id="limpieza_meteo",
    python_callable=limpieza_meteo,
    provide_context=True,
    dag=dag,
)

# ==================== TAREAS FINALES ====================

verificar_task = PythonOperator(
    task_id="verificar",
    python_callable=verificar,
    provide_context=True,
    dag=dag,
)

guardar_task = PythonOperator(
    task_id="guardar",
    python_callable=guardar,
    provide_context=True,
    dag=dag,
)

reporte_task = PythonOperator(
    task_id="reporte",
    python_callable=reporte,
    provide_context=True,
    dag=dag,
)

# ==================== DEPENDENCIAS SEGÚN TU DIAGRAMA ====================

# Flujo CSV: Ingestion -> Transformaciones -> Limpieza final
variables_task >> [ingestion_csv_task, ingestion_openweather_task, ingestion_meteostat_task]

ingestion_csv_task >> [transformacion_csv_task, transformacion_fao_task]
transformacion_csv_task >> union_csv_task
transformacion_fao_task >> union_csv_task
union_csv_task >> limpieza_final_csv_task

# Flujo OpenWeather: Ingestion -> Transformación
ingestion_openweather_task >> transformacion_openweather_task

# Flujo Meteostat: Ingestion -> Transformación
ingestion_meteostat_task >> transformacion_meteostat_task

# Convergencia: Todas las transformaciones alimentan Verificar
[limpieza_final_csv_task, transformacion_openweather_task, transformacion_meteostat_task] >> verificar_task

# Pipeline final
verificar_task >> guardar_task >> reporte_task
