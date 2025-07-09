"""
DAG principal para el pipeline ETL
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Añadir el directorio raíz al path para imports
sys.path.insert(0, '/opt/airflow')

# Importar funciones ETL
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data

# Configuración por defecto del DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL completo',
    schedule_interval=timedelta(days=1),  # Ejecutar diariamente
    catchup=False,
    tags=['etl', 'data-pipeline'],
)

def extract_task(**context):
    """
    Task para extraer datos
    """
    print("🔍 Iniciando extracción de datos...")
    
    # Llamar a tu función de extracción
    data = extract_data()
    
    print(f"✅ Extracción completada. Registros extraídos: {len(data) if data is not None else 0}")
    
    # Pasar datos al siguiente task usando XCom
    return {"status": "success", "records": len(data) if data is not None else 0}

def transform_task(**context):
    """
    Task para transformar datos
    """
    print("🔄 Iniciando transformación de datos...")
    
    # Obtener resultado del task anterior
    extract_result = context['task_instance'].xcom_pull(task_ids='extract_data')
    print(f"📊 Resultado de extracción: {extract_result}")
    
    # Llamar a tu función de transformación
    transformed_data = transform_data()
    
    print(f"✅ Transformación completada. Registros transformados: {len(transformed_data) if transformed_data is not None else 0}")
    
    return {"status": "success", "transformed_records": len(transformed_data) if transformed_data is not None else 0}

def load_task(**context):
    """
    Task para cargar datos
    """
    print("📥 Iniciando carga de datos...")
    
    # Obtener resultado del task anterior
    transform_result = context['task_instance'].xcom_pull(task_ids='transform_data')
    print(f"📊 Resultado de transformación: {transform_result}")
    
    # Llamar a tu función de carga
    result = load_data()
    
    print("✅ Carga completada exitosamente")
    
    return {"status": "success", "load_result": result}

def check_data_quality(**context):
    """
    Task para verificar calidad de datos
    """
    print("🔍 Verificando calidad de datos...")
    
    # Obtener resultados de tasks anteriores
    extract_result = context['task_instance'].xcom_pull(task_ids='extract_data')
    transform_result = context['task_instance'].xcom_pull(task_ids='transform_data')
    load_result = context['task_instance'].xcom_pull(task_ids='load_data')
    
    print(f"📊 Resumen del pipeline:")
    print(f"   - Extracción: {extract_result}")
    print(f"   - Transformación: {transform_result}")
    print(f"   - Carga: {load_result}")
    
    # Aquí puedes añadir validaciones adicionales
    if (extract_result and extract_result.get('status') == 'success' and 
        transform_result and transform_result.get('status') == 'success' and
        load_result and load_result.get('status') == 'success'):
        print("✅ Pipeline completado exitosamente - Calidad de datos OK")
        return {"status": "success", "quality_check": "passed"}
    else:
        raise ValueError("❌ Pipeline falló - Verificar logs")

# Crear tasks
extract_task_op = PythonOperator(
    task_id='extract_data',
    python_callable=extract_task,
    dag=dag,
)

transform_task_op = PythonOperator(
    task_id='transform_data',
    python_callable=transform_task,
    dag=dag,
)

load_task_op = PythonOperator(
    task_id='load_data',
    python_callable=load_task,
    dag=dag,
)

quality_check_task_op = PythonOperator(
    task_id='data_quality_check',
    python_callable=check_data_quality,
    dag=dag,
)

# Task de inicio
start_task = BashOperator(
    task_id='start_pipeline',
    bash_command='echo "🚀 Iniciando pipeline ETL..."',
    dag=dag,
)

# Task de fin
end_task = BashOperator(
    task_id='end_pipeline',
    bash_command='echo "🎉 Pipeline ETL completado exitosamente!"',
    dag=dag,
)

# Definir dependencias (orden de ejecución)
start_task >> extract_task_op >> transform_task_op >> load_task_op >> quality_check_task_op >> end_task