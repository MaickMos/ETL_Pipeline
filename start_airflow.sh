#!/bin/bash

echo "🚀 Iniciando Airflow..."

# Crear directorios necesarios
mkdir -p airflow/dags airflow/logs airflow/plugins

# Establecer UID de Airflow
export AIRFLOW_UID=$(id -u)

# Iniciar servicios
docker-compose up -d

echo "✅ Airflow iniciado!"
echo "🌐 Accede a la interfaz web: http://localhost:8080"
echo "👤 Usuario: admin"
echo "🔑 Contraseña: admin"
echo ""
echo "📋 Comandos útiles:"
echo "   docker-compose logs -f airflow-webserver    # Ver logs del webserver"
echo "   docker-compose logs -f airflow-scheduler    # Ver logs del scheduler"
echo "   docker-compose down                         # Detener Airflow"