# airflow-demo

Práctica de la clase 7 (Orquestación con Apache Airflow) — Diplomatura en Cloud Data Engineering.

Airflow 3.4 con LocalExecutor en Docker Compose y dos DAGs:

- `hola_airflow`: tareas, dependencias, XCom, contexto y un template Jinja.
- `ventas_diarias`: el ETL de ventas de la clase de Docker, orquestado. Espera el archivo del día
  (sensor), transforma, carga en Postgres de forma idempotente y, los domingos, arma un reporte semanal (branching).

```
.
├── docker-compose.yaml     # postgres (metadatos) + warehouse (datos) + api-server + scheduler + dag-processor
├── .env.example            # AIRFLOW_UID (Linux)
├── dags/
│   ├── hola_airflow.py
│   └── ventas_diarias.py
├── data/raw/               # ventas_YYYY-MM-DD.csv (1 al 17/11/2026 ya generados)
├── scripts/generar_ventas.py
└── logs/
```

## Levantar

Requisitos: Docker Desktop con al menos 4 GB de memoria.

```bash
cp .env.example .env            # en Linux: echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up -d            # la primera vez descarga la imagen (~1 GB)
docker compose ps               # esperar a que api-server y scheduler estén healthy
```

UI: http://localhost:8080 (sin login: solo para clase).

## Generar datos de otro día

```bash
python scripts/generar_ventas.py 2026-11-18
python scripts/generar_ventas.py 2026-11-18 2026-11-25   # rango
```

## Ver los datos cargados

```bash
docker compose exec warehouse psql -U etl -d ventas -c "SELECT fecha, SUM(importe) FROM ventas_diarias GROUP BY 1 ORDER BY 1;"
```

O desde tu máquina: `postgresql://etl:etl@localhost:5433/ventas`.

## Apagar

```bash
docker compose down        # conserva las bases
docker compose down -v     # borra todo, incluida la metadata de Airflow
```
