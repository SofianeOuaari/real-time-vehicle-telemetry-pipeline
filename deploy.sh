#!/bin/bash
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}[1/6] Setting up environment...${NC}"
if [ ! -f .env.local ]; then
    echo "AIRFLOW_UID=$(id -u)" > .env.local
    cat .env.local >> .env
fi
echo -e "${GREEN}✓ done${NC}"
echo ""

echo -e "${YELLOW}[2/6] Creating directories...${NC}"
mkdir -p dags logs plugins data/{parquet,analysis_results,archive,duckdb}
chmod 777 logs data 2>/dev/null || true
echo -e "${GREEN}✓ done${NC}"
echo ""

echo -e "${YELLOW}[3/6] Building images...${NC}"

echo "  -> custom-airflow"
docker build -t custom-airflow:latest airflow-custom

# shared schemas need to be inside each service build context
echo "  -> copying shared schemas"
for svc in generator preprocessor parquet_writer langgraph_agent; do
    cp -r shared services/$svc/shared 2>/dev/null || true
done

echo "  -> generator"
docker build -t generator:latest services/generator

echo "  -> preprocessor"
docker build -t preprocessor:latest services/preprocessor

echo "  -> parquet-writer"
docker build -t parquet-writer:latest services/parquet_writer

echo "  -> langgraph-agent"
docker build -t langgraph-agent:latest services/langgraph_agent

echo "  -> elasticsearch-writer"
docker build -t elasticsearch-writer:latest services/elasticsearch_writer

rm -rf services/*/shared
echo -e "${GREEN}✓ all images built${NC}"
echo ""

echo -e "${YELLOW}[4/6] Starting infrastructure...${NC}"
docker compose up -d \
    zookeeper kafka kafka-ui redis postgres-airflow \
    ollama elasticsearch kibana elasticsearch-writer
echo "waiting 90s for services to be healthy..."
sleep 90
echo -e "${GREEN}✓ infrastructure ready${NC}"
echo ""

echo -e "${YELLOW}[5/6] Initializing Airflow...${NC}"
docker compose up airflow-init
echo "starting webserver and scheduler..."
docker compose up -d airflow-webserver airflow-scheduler
echo -e "${GREEN}✓ Airflow running${NC}"
echo ""

echo -e "${YELLOW}[6/6] Pulling llama3.2 (this takes a few minutes)...${NC}"
docker compose --profile init up ollama-init
echo -e "${GREEN}✓ model ready${NC}"
echo ""

echo -e "${GREEN}All services are up.${NC}"
echo ""
echo "  Airflow       http://localhost:8081  (admin / admin)"
echo "  Kafka UI      http://localhost:8080"
echo "  Kibana        http://localhost:5601"
echo "  Elasticsearch http://localhost:9200"
echo "  Ollama        http://localhost:11435"
echo ""
echo "Enable telemetry_pipeline and analysis_pipeline in Airflow to start."
echo "Logs: docker compose logs -f airflow-scheduler"
