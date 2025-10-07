PIPELINE DE INTEGRACIÓN Y ANÁLISIS DE DATOS  
CLIMÁTICOS PARA BOLIVIA CLIMA 360 
INTEGRANTES: 
 GEORGINA QUIROZ MENDOZA 
 CARLOS ALMARAZ ESCOBAR 
 DILAN CONDORI ALEJO 
 ALVARO LUIS JURADO ALFARO 
 JAIME MONTECINOS MARQUEZ 
1. Descripción General 
Clima360 es un proyecto de ciencia de datos orientado a la integración, limpieza, validación 
y análisis de datos climáticos provenientes de múltiples fuentes, tanto en tiempo real como 
históricas. 
El objetivo principal es construir un pipeline ETL automatizado utilizando Python y Apache 
Airflow, capaz de unificar y analizar información meteorológica de 9 ciudades de Bolivia, 
permitiendo: 
 Generar datasets limpios y consolidados. 
 Analizar patrones climáticos históricos y actuales. 
 Preparar información para dashboards y reportes analíticos. 
Ciudades analizadas: La Paz, Santa Cruz, Cochabamba, Oruro, Beni, Potosí, Tarija, Pando 
y Sucre. 
2. Arquitectura del Proyecto 
Se implementó una arquitectura batch diaria (24h) con ejecución programada mediante 
Airflow, asegurando trazabilidad y reproducibilidad del pipeline. 
Flujo general del pipeline: 
Arquitectura detallada con componentes: 
Componente 
Descripción 
Ingesta 
APIs y CSV históricos: OpenWeather, Meteostat, FAOSTAT, 
Environment Temperature Change. 
Procesamiento 
Almacenamiento 
Limpieza, normalización y validación de datos con Pandas. 
Base de datos SQLite local + CSV procesado. 
Orquestación 
DAG en Apache Airflow para controlar ejecución ETL diaria. 
Reporte 
HTML y Excel con análisis de calidad de datos y diccionario de 
datos. 
Componente 
Descripción 
Visualización 
futura 
Dashboards interactivos (Power BI, Grafana, Dash). 
Nota: La arquitectura es escalable y puede migrarse a PostgreSQL, BigQuery o entornos en 
la nube para datasets más grandes. 
3. Estructura del Proyecto 
Clima360/ 
│ 
├── data/ 
│   ├── raw/                     
│   ├── processed/               
│   └── clima360.db              
│ 
├── notebooks/ 
# Datos originales (APIs y CSVs) 
# Datos limpios e integrados 
# Base de datos SQLite 
│   ├── OpenWeather_Extraction.ipynb 
│   ├── Meteostat_Extraction.ipynb 
│   ├── Data_Cleaning_Integration.ipynb 
│   ├── Data_Profiling_Report.ipynb 
│   └── Airflow_DAG_Clima360.ipynb 
│ 
├── dags/ 
│   └── clima360_dag.py          
│ 
├── reports/ 
# DAG de Airflow (orquestación ETL) 
│   ├── data_quality_report.html 
│   └── diccionario_datos.xlsx 
│ 
├── requirements.txt 
└── README.md 
4. Ingesta de Datos 
OpenWeather API 
 Consumo de API mediante requests. 
 Extracción de datos climáticos en tiempo real (temperatura, humedad, presión, 
velocidad del viento). 
 Manejo de errores y reconexiones automáticas. 
import requests 
import pandas as pd 
url = 
f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=m
 etric" 
response = requests.get(url) 
data = response.json() 
df = pd.DataFrame([{"city": city, "temp": data["main"]["temp"],  
"humidity": data["main"]["humidity"], "date": pd.Timestamp.now().date()}]) 
Meteostat API 
 Datos históricos diarios de clima (2021–2025). 
 Resampling mensual para cálculo de promedios. 
from meteostat import Point, Daily 
from datetime import datetime 
location = Point(-16.5, -68.15)  # La Paz 
data = Daily(location, datetime(2021,1,1), datetime(2025,1,1)) 
df_meteo = data.fetch().resample('M').mean().reset_index() 
import seaborn as sns 
import matplotlib.pyplot as plt 
sns.lineplot(data=df_meteo, x='time', y='tavg') 
plt.title('Temperatura Promedio Mensual - La Paz') 
plt.xlabel('Fecha') 
plt.ylabel('°C') 
plt.show() 
Fuentes Históricas 
 FAOSTAT: Datos relacionados con clima (2020–2024). 
 Environment Temperature Change (Kaggle): Datos históricos 1961–2019. 
 Normalización de fechas y unidades, unificación con Pandas. 
5. Limpieza y Validación 
 Eliminación de duplicados y caracteres extraños. 
 Normalización de unidades y fechas. 
 Integración de datasets con merge y concat. 
 Perfilado de datos (.info(), .isnull().sum(), .value_counts()). 
df_all = pd.concat([df_openweather, df_meteostat, df_fao, df_enviro], ignore_index=True) 
df_all['temp_categoria'] = pd.cut(df_all['value'], bins=[-10, 5, 15, 40], labels=['Baja', 
'Media', 'Alta']) 
df_all['temp_categoria'].value_counts().plot(kind='bar', color=['blue','orange','red']) 
plt.title('Distribución de Categorías de Temperatura') 
plt.ylabel('Cantidad de registros') 
plt.show() 
6. Procesamiento y Clasificación 
 Cálculo de anomalías climáticas por ciudad. 
 Clasificación de temperaturas: Baja / Media / Alta. 
 Validación cruzada FAOSTAT–Meteostat para consistencia. 
correlation = df_fao['value'].corr(df_meteostat['value']) 
print("Correlación FAO–Meteostat:", round(correlation, 3)) 
sns.heatmap(df_all.corr(), annot=True, cmap='coolwarm') 
plt.title('Correlación entre Variables Climáticas') 
plt.show() 
7. Almacenamiento 
 CSV procesado y SQLite (clima360.db) para persistencia local. 
from sqlalchemy import create_engine 
engine = create_engine('sqlite:///data/clima360.db', echo=False) 
df_all.to_sql('clima_unificado', con=engine, if_exists='replace', index=False) 
8. Orquestación con Airflow 
 DAG principal clima360_dag.py con tareas: extract_data, transform_data, 
load_data, generate_report. 
from airflow import DAG 
from airflow.operators.python import PythonOperator 
from datetime import datetime 
with DAG('clima360_dag', schedule='@daily', start_date=datetime(2025,10,1)) as dag: 
extract = PythonOperator(task_id='extract_data', python_callable=extract_data) 
transform = PythonOperator(task_id='transform_data', python_callable=transform_data) 
load = PythonOperator(task_id='load_data', python_callable=load_data) 
report = PythonOperator(task_id='generate_report', python_callable=generate_report) 
extract >> transform >> load >> report 
 Diagrama de Airflow 
9. Reportes Generados 
 data_quality_report.html: Completitud, unicidad, validez, consistencia, exactitud. 
 diccionario_datos.xlsx: Descripción de columnas y tipos de datos. 
10. Resultados Obtenidos 
 Pipeline reproducible y automatizado. 
 Dataset final: +1000 registros (2020–2025, 9 ciudades). 
 Correlación FAOSTAT–Meteostat confirmada. 
11. Librerías Utilizadas 
pandas, numpy, requests, meteostat, sqlalchemy, airflow, datetime, matplotlib, seaborn. 
12. Mejora Continua y Futuro 
 Migración a PostgreSQL o BigQuery. 
 Dashboards interactivos: Power BI, Grafana, Plotly Dash. 
 Procesamiento en streaming con Kafka. 
 Contenerización con Docker y pipelines CI/CD. 
 Monitoreo y optimización continua de recursos. 
