"""Pipeline diario de ventas: el ETL de la clase de Docker, ahora orquestado.

Cada run procesa UN día: el que empieza en data_interval_start. Así se puede
re-correr o hacer backfill de cualquier fecha y el resultado es siempre el mismo.
(Usamos data_interval_start y no ds: en un run manual, ds puede ser None o no
coincidir con el intervalo.)

    crear_tablas >> esperar_archivo >> transformar >> cargar >> es_domingo
                                                               ├─> reporte_semanal ─┐
                                                               └────────────────────┴─> fin
"""

import csv
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import CronDataIntervalTimetable, PokeReturnValue, dag, task

RAW_DIR = Path("/opt/airflow/data/raw")
CONN_ID = "warehouse"  # viene de la variable de entorno AIRFLOW_CONN_WAREHOUSE


@dag(
    # Ventana real de un día: el run de [14/11 00:00, 15/11 00:00) corre el 15/11 a las 00:00
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=3,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=20)},
    tags=["clase7", "ventas"],
)
def ventas_diarias():
    crear_tablas = SQLExecuteQueryOperator(
        task_id="crear_tablas",
        conn_id=CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS ventas_diarias (
                fecha     date,
                sucursal  text,
                ordenes   int,
                unidades  int,
                importe   numeric,
                cargado_en timestamptz DEFAULT now(),
                PRIMARY KEY (fecha, sucursal)
            );
            CREATE TABLE IF NOT EXISTS ventas_semanales (
                semana_hasta date,
                sucursal     text,
                importe      numeric,
                PRIMARY KEY (semana_hasta, sucursal)
            );
        """,
    )

    @task.sensor(poke_interval=20, timeout=60 * 30, mode="reschedule")
    def esperar_archivo(data_interval_start=None) -> PokeReturnValue:
        archivo = RAW_DIR / f"ventas_{data_interval_start:%Y-%m-%d}.csv"
        print(f"Buscando {archivo}")
        return PokeReturnValue(is_done=archivo.exists(), xcom_value=str(archivo))

    @task
    def transformar(archivo: str) -> list[dict]:
        vistos, agregado = set(), defaultdict(lambda: {"ordenes": 0, "unidades": 0, "importe": 0})
        with open(archivo, encoding="utf-8") as f:
            for fila in csv.DictReader(f):
                if fila["order_id"] in vistos or not fila["cantidad"]:
                    continue  # duplicados y cantidades vacías afuera
                vistos.add(fila["order_id"])
                sucursal = fila["sucursal"].strip().title()
                cantidad = int(fila["cantidad"])
                agregado[sucursal]["ordenes"] += 1
                agregado[sucursal]["unidades"] += cantidad
                agregado[sucursal]["importe"] += cantidad * int(fila["precio_unitario"])
        filas = [{"sucursal": s, **v} for s, v in sorted(agregado.items())]
        print(f"{len(vistos)} órdenes válidas -> {len(filas)} sucursales")
        return filas  # chico: viaja por XCom sin problema

    @task
    def cargar(filas: list[dict], data_interval_start=None):
        dia = data_interval_start.date()
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        # Idempotente: borro el día y lo vuelvo a insertar
        hook.run("DELETE FROM ventas_diarias WHERE fecha = %s", parameters=(dia,))
        hook.insert_rows(
            table="ventas_diarias",
            rows=[(dia, f["sucursal"], f["ordenes"], f["unidades"], f["importe"]) for f in filas],
            target_fields=["fecha", "sucursal", "ordenes", "unidades", "importe"],
        )
        print(f"Cargadas {len(filas)} filas para {dia}")

    @task.branch
    def es_domingo(data_interval_start=None) -> str:
        return "reporte_semanal" if data_interval_start.weekday() == 6 else "fin"

    reporte_semanal = SQLExecuteQueryOperator(
        task_id="reporte_semanal",
        conn_id=CONN_ID,
        sql="""
            {% set dia = data_interval_start | ds %}
            DELETE FROM ventas_semanales WHERE semana_hasta = '{{ dia }}';
            INSERT INTO ventas_semanales (semana_hasta, sucursal, importe)
            SELECT '{{ dia }}', sucursal, SUM(importe)
            FROM ventas_diarias
            WHERE fecha BETWEEN '{{ macros.ds_add(dia, -6) }}' AND '{{ dia }}'
            GROUP BY sucursal;
        """,
    )

    fin = EmptyOperator(task_id="fin", trigger_rule="none_failed_min_one_success")

    archivo = esperar_archivo()
    crear_tablas >> archivo
    cargado = cargar(transformar(archivo))
    rama = es_domingo()
    cargado >> rama >> [reporte_semanal, fin]
    reporte_semanal >> fin


ventas_diarias()
