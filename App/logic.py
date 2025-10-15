import time
import os
import csv 
import tabulate 
import math
import re
from DataStructures.List import array_list as lt
from DataStructures.Stack import stack as st
from DataStructures.Queue import queue as q

data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Data")

_FLOAT_RE = re.compile(r'^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$')
def _parse_float_or_none(s: str):
    if s is None:
        return None
    s2 = s.strip().replace(",", ".")
    return float(s2) if _FLOAT_RE.match(s2) else None

def new_logic():
    """
    Crea el catalogo para almacenar las estructuras de datos
    """
    catalogo = {"trips" : None , 
                "barrios" : None }
    
    catalogo["trips"] = lt.new_list()
    catalogo["barrios"] = lt.new_list()
    
    return catalogo


# Funciones para la carga de datos

def load_data(catalog, filename):
    start = get_time()
    path = os.path.join(data_dir, filename)
    file = open(path, encoding="utf-8")
    reader = csv.DictReader(file)
    for row in reader:
        pick_t = time.strptime(row["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
        drop_t = time.strptime(row["dropoff_datetime"], "%Y-%m-%d %H:%M:%S")
        pick_ts = time.mktime(pick_t)
        drop_ts = time.mktime(drop_t)
        duration_min = (drop_ts - pick_ts) / 60.0

        lt.add_last(catalog["trips"], {
            "pickup_datetime": row["pickup_datetime"],
            "dropoff_datetime": row["dropoff_datetime"],
            "pickup_ts": pick_ts,
            "dropoff_ts": drop_ts,
            "pickup_hour": pick_t.tm_hour,

            "passenger_count": int(row["passenger_count"]),
            "payment_type": row["payment_type"].strip().upper(),

            "trip_distance": float(row["trip_distance"]),
            "fare_amount": float(row["fare_amount"]),
            "tip_amount": float(row["tip_amount"]),
            "tolls_amount": float(row["tolls_amount"]),
            "total_amount": float(row["total_amount"]),

            "duration_min": duration_min,

            "pickup_longitude": float(row["pickup_longitude"]),
            "pickup_latitude": float(row["pickup_latitude"]),
            "dropoff_longitude": float(row["dropoff_longitude"]),
            "dropoff_latitude": float(row["dropoff_latitude"]),
        })

    file.close()
    end = get_time()
    tiempo_milisegundos = delta_time(start, end)

    elements = catalog["trips"]["elements"]
    size = catalog["trips"]["size"]

    min_trip = None
    max_trip = None

    for t in elements:
        dist = t.get("trip_distance", 0.0)

        if dist > 0.0:
            if min_trip is None or dist < min_trip["trip_distance"]:
                min_trip = t

        if max_trip is None or dist > max_trip["trip_distance"]:
            max_trip = t

    primeras_slice = elements[:5] if size > 0 else []
    for t in primeras_slice:
        primeras5_rows = [{
            "pickup_datetime": t["pickup_datetime"],
            "dropoff_datetime": t["dropoff_datetime"],
            "duration_min": round(t["duration_min"], 2),
            "trip_distance": round(t["trip_distance"], 3),
            "total_amount": round(t["total_amount"], 2),
        }]
    ultimas_slice = elements[-5:] 
    for t in ultimas_slice:
        ultimas5_rows = [{
            "pickup_datetime": t["pickup_datetime"],
            "dropoff_datetime": t["dropoff_datetime"],
            "duration_min": round(t["duration_min"], 2),
            "trip_distance": round(t["trip_distance"], 3),
            "total_amount": round(t["total_amount"], 2),
        }]

    total_registros = lt.size(catalog["trips"])
    resumen = [
        ["Archivo", filename],
        ["Registros cargados", total_registros],
        ["Tiempo de carga (ms)", round(tiempo_milisegundos, 3)]
    ]

    print("\n=== Resumen de carga de datos de viajes ===") 
    print(tabulate.tabulate(resumen, headers=["Métrica", "Valor"], tablefmt="psql"))
    
    print("\n=== Trayecto de menor distancia ===")
    print(tabulate.tabulate([{
        "pickup_datetime": min_trip["pickup_datetime"],
        "trip_distance": round(min_trip["trip_distance"], 3),
        "total_amount": round(min_trip["total_amount"], 2)
    }], headers="keys", tablefmt="psql"))

    print("\n=== Trayecto de mayor distancia ===")
    print(tabulate.tabulate([{
        "pickup_datetime": max_trip["pickup_datetime"],
        "trip_distance": round(max_trip["trip_distance"], 3),
        "total_amount": round(max_trip["total_amount"], 2)
    }], headers="keys", tablefmt="psql"))
    
    print("\n=== Primeros 5 trayectos ===")
    print(tabulate.tabulate(primeras5_rows, headers="keys", tablefmt="psql"))

    print("\n=== Últimos 5 trayectos ===")
    print(tabulate.tabulate(ultimas5_rows, headers="keys", tablefmt="psql"))


    return catalog

def load_neighborhoods(catalog, filename):
    """
    Carga nyc-neighborhoods.csv en catalog['barrios'].
    - Soporta separador ';'
    - Convierte decimales con coma (e.g., 40,81155 -> 40.81155)
    """
    start = get_time()
    path = os.path.join(data_dir, filename)
    with open(path, encoding="utf-8") as file:
        # El CSV de barrios viene con ';' como delimitador
        reader = csv.DictReader(file, delimiter=';')

        cargados = 0
        for row in reader:
            # Accesos directos (headers exactos: borough;neighborhood;latitude;longitude)
            boro = row.get("borough")
            nbhd = row.get("neighborhood")
            lat_s = row.get("latitude")
            lon_s = row.get("longitude")
            
            lat = _parse_float_or_none(lat_s)
            lon = _parse_float_or_none(lon_s)
            if lat is None or lon is None:
                continue

            lt.add_last(catalog["barrios"], {
                "borough": boro.strip(),
                "neighborhood": nbhd.strip(),
                "latitude": lat,
                "longitude": lon,
            })
            cargados += 1

    end = get_time()
    tiempo_milisegundos = delta_time(start, end)
    total_registros = lt.size(catalog["barrios"])
    resumen = [
        ["Archivo", filename],
        ["Registros cargados", total_registros],
        ["Tiempo de carga (ms)", round(tiempo_milisegundos, 3)]
    ]

    print("\n=== Resumen de carga de datos de barrios ===")
    print(tabulate.tabulate(resumen, headers=["Métrica", "Valor"], tablefmt="psql"))

    if catalog["barrios"]["size"] > 0:
        print(f"\nBarrios cargados: {cargados}")
        print(tabulate.tabulate(catalog["barrios"]["elements"][:5], headers="keys", tablefmt="psql"))
    else:
        print("\nNo se cargaron barrios. Revisa delimitador ';' y columnas: borough;neighborhood;latitude;longitude")
    return catalog

# Funciones de consulta sobre el catálogo

def get_data(catalog, idx):
    """
    Retorna un dato por su índice (0-based) dentro de trips.
    """
    return lt.get_element(catalog["trips"], idx)

def req_1(catalog, start_dt_str, end_dt_str, sample_n):
   
    start_clock = get_time()

    # Parseo de fechas/horas de entrada
    start_tuple = time.strptime(start_dt_str, "%Y-%m-%d %H:%M:%S")
    end_tuple   = time.strptime(end_dt_str,   "%Y-%m-%d %H:%M:%S")
    start_ts = time.mktime(start_tuple)
    end_ts   = time.mktime(end_tuple)

    # Acceso al array 
    trips_pylist = catalog["trips"]["elements"]

    # Filtrar por franja de recogida (pickup_ts en [start_ts, end_ts])
    filtered = [t for t in trips_pylist if start_ts <= t["pickup_ts"] <= end_ts]

    # Acá Ordenamos del más antiguo al más reciente por pickup_ts
    def sort_by_pickup_ts(trip):
        return trip["pickup_ts"]
    filtered_sorted = sorted(filtered, key=sort_by_pickup_ts)
    total = len(filtered_sorted)

    # Tamaño de muestra
    if isinstance(sample_n, int):
        N = sample_n
    else:
        s = str(sample_n).strip()
        N = int(s) if s.isdigit() else 5
    if N <= 0:
        N = 5

    # Construcción de filas de salida con los campos requeridos
    def build_row(trip):
        return {
            "pickup_datetime": trip["pickup_datetime"],
            "pickup_coords": [round(trip["pickup_latitude"], 6),
                              round(trip["pickup_longitude"], 6)],  # [Lat, Lon]
            "dropoff_datetime": trip["dropoff_datetime"],
            "dropoff_coords": [round(trip["dropoff_latitude"], 6),
                               round(trip["dropoff_longitude"], 6)],  # [Lat, Lon]
            "trip_distance": round(trip["trip_distance"], 3),
            "total_amount": round(trip["total_amount"], 2),
        }

    # Regla: si hay menos de 2N, se muestran todos los trayectos (sin duplicar)
    if total <= 2 * N:
        first_n_rows = [build_row(t) for t in filtered_sorted]
        last_n_rows = []
    else:
        first_n_rows = [build_row(t) for t in filtered_sorted[:N]]
        last_n_rows = [build_row(t) for t in filtered_sorted[-N:]]

    end_clock = get_time()
    elapsed_ms = round(delta_time(start_clock, end_clock), 3)

    return {
        "elapsed_ms": elapsed_ms,
        "total_trips": total,
        "first_n": first_n_rows,
        "last_n": last_n_rows
    }


def req_2(catalog):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    pass


def req_3(catalog, distancia_inicial, distancia_final, N):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    start_clock = get_time()

    # Acá lo que se hace es que se filtran los viajes que están entre la distancia inicial y final
    # y se guardan en una lista aparte
    filtrados = lt.new_list()
    total_trips = catalog["trips"]
    trips_size = lt.size(total_trips)

    for trip in range(trips_size):
        element = lt.get_element(total_trips, trip)
        dist = element["trip_distance"]
        if distancia_inicial <= dist and dist <= distancia_final:
            lt.add_last(filtrados, element)

    # Ahora lo que vamos a hacer es definir un sort_criteria basándonos en los criterios
    # que se definen en el enunciado del requerimiento, ya que segun lo que nos dicen el default_sort_criteria
    # no nos sirve ya que nos piden hacer otra comparación si son iguales 
    def sort_criteria_req3(trip1, trip2):
        if trip1["trip_distance"] > trip2["trip_distance"]:
            return True
        elif trip1["trip_distance"] < trip2["trip_distance"]:
            return False
        else:
            return trip1["total_amount"] > trip2["total_amount"]

    # Y se hace el ordenamiento de la lista de filtrados con merge sort con el sort_criteria que definimos
    size_filtrados = lt.size(filtrados)
    if size_filtrados > 1:
        filtrados = lt.merge_sort(filtrados, sort_criteria_req3)

    total_filtrados = size_filtrados

    primeros = lt.new_list()
    ultimos = lt.new_list()

    if total_filtrados <= 2 * N:
        # Si se cumple que es menor o igual a 2N, se devuelven toditos los elementos
        for t in filtrados["elements"]:
            lt.add_last(primeros, ({
                "pickup_datetime": t["pickup_datetime"],
                "pickup_coords": [t["pickup_latitude"], t["pickup_longitude"]],
                "dropoff_datetime": t["dropoff_datetime"],
                "dropoff_coords": [t["dropoff_latitude"], t["dropoff_longitude"]],
                "trip_distance": round(t["trip_distance"], 3),
                "total_amount": round(t["total_amount"], 2)
            }))
        # Como son lo mismo, se retornan los mismo elementos
        ultimos = primeros
    else:
        # Se hace el loop para los primeros valores en el rango de N
        for i in range(N):
            t = lt.get_element(filtrados, i)
            lt.add_last(primeros, ({
                "pickup_datetime": t["pickup_datetime"],
                "pickup_coords": [t["pickup_latitude"], t["pickup_longitude"]],
                "dropoff_datetime": t["dropoff_datetime"],
                "dropoff_coords": [t["dropoff_latitude"], t["dropoff_longitude"]],
                "trip_distance": round(t["trip_distance"], 3),
                "total_amount": round(t["total_amount"], 2)
            }))

        # Se hace el loop para los últimos valores en el rango de N
        for i in range(total_filtrados - N, total_filtrados):
            t = lt.get_element(filtrados, i)  
            lt.add_last(ultimos, ({
                "pickup_datetime": t["pickup_datetime"],
                "pickup_coords": [t["pickup_latitude"], t["pickup_longitude"]],
                "dropoff_datetime": t["dropoff_datetime"],
                "dropoff_coords": [t["dropoff_latitude"], t["dropoff_longitude"]],
                "trip_distance": round(t["trip_distance"], 3),
                "total_amount": round(t["total_amount"], 2)
            }))

    end_clock = get_time()
    tiempo_ms = round(delta_time(start_clock, end_clock), 3)

    # Por último se hace el retorno de los datos filtrados
    return {
        "tiempo_ms": tiempo_ms,
        "total_filtrados": total_filtrados,
        "primeros": primeros,
        "ultimos": ultimos
    }

def req_4(catalog):
    """
    Retorna el resultado del requerimiento 4
    """
    # TODO: Modificar el requerimiento 4
    pass


def req_5(catalog):
    """
    Retorna el resultado del requerimiento 5
    """
    # TODO: Modificar el requerimiento 5
    pass

def req_6(catalog):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    pass


# Funciones para medir tiempos de ejecucion

def get_time():
    """
    devuelve el instante tiempo de procesamiento en milisegundos
    """
    return float(time.perf_counter()*1000)


def delta_time(start, end):
    """
    devuelve la diferencia entre tiempos de procesamiento muestreados
    """
    elapsed = float(end - start)
    return elapsed
