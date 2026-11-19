"""Primer DAG: tareas, dependencias, contexto y templates.

- Un BashOperator (operator "clásico") con un template Jinja.
- Tareas con @task (TaskFlow): funciones de Python que se pasan resultados (XCom).
- Una tarea que imprime el contexto del run para ver las fechas.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task


@dag(
    schedule="@daily",                 # Airflow 3: punto en el tiempo (CronTriggerTimetable)
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["clase7"],
)
def hola_airflow():
    saludo = BashOperator(
        task_id="saludo_bash",
        # Template Jinja: Airflow reemplaza {{ ... }} justo antes de ejecutar
        bash_command='echo "Hola desde Airflow. run_id={{ run_id }} | run_after={{ dag_run.run_after }}"',
    )

    @task
    def mostrar_contexto(**context):
        for clave in ["run_id", "logical_date", "ds", "data_interval_start", "data_interval_end"]:
            print(f"{clave:>20} => {context.get(clave)}")

    @task
    def sumar(a: int, b: int) -> int:
        return a + b

    @task
    def imprimir(resultado: int):
        print(f"El resultado que vino por XCom es {resultado}")

    saludo >> mostrar_contexto()
    imprimir(sumar(2, 3))


hola_airflow()
