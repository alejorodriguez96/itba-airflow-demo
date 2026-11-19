"""Genera el archivo de ventas crudas de un día: data/raw/ventas_YYYY-MM-DD.csv

Uso:
    python scripts/generar_ventas.py 2026-11-18
    python scripts/generar_ventas.py 2026-11-01 2026-11-17   # rango (inclusive)

Solo usa la librería estándar. Los datos son sintéticos y determinísticos por fecha,
con algo de suciedad a propósito (duplicados, espacios, cantidades vacías).
"""

import csv
import random
import sys
from datetime import date, timedelta
from pathlib import Path

SUCURSALES = ["Palermo", "Belgrano", "Caballito", "Quilmes", "San Isidro"]
PRODUCTOS = {
    "Café": ("Bebidas", 2500),
    "Medialuna": ("Panadería", 900),
    "Tostado": ("Comidas", 4200),
    "Jugo": ("Bebidas", 3100),
    "Budín": ("Panadería", 3800),
    "Ensalada": ("Comidas", 6500),
}
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def generar(dia: date) -> Path:
    rnd = random.Random(dia.toordinal())
    filas = []
    base = 60 if dia.weekday() < 5 else 110  # el finde se vende más
    for i in range(rnd.randint(base, base + 40)):
        producto = rnd.choice(list(PRODUCTOS))
        categoria, precio = PRODUCTOS[producto]
        sucursal = rnd.choice(SUCURSALES)
        if rnd.random() < 0.03:
            sucursal = f" {sucursal.upper()} "
        cantidad = "" if rnd.random() < 0.02 else rnd.randint(1, 4)
        fila = [f"{dia:%Y%m%d}-{i:04d}", dia.isoformat(), sucursal, producto, categoria, cantidad, precio]
        filas.append(fila)
        if rnd.random() < 0.02:
            filas.append(fila)  # duplicado
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    destino = RAW_DIR / f"ventas_{dia.isoformat()}.csv"
    with destino.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "fecha", "sucursal", "producto", "categoria", "cantidad", "precio_unitario"])
        w.writerows(filas)
    return destino


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        sys.exit(__doc__)
    desde = date.fromisoformat(sys.argv[1])
    hasta = date.fromisoformat(sys.argv[-1])
    d = desde
    while d <= hasta:
        print("generado", generar(d))
        d += timedelta(days=1)
