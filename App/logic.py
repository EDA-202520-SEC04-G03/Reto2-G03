import time
import os
import csv 
import tabulate 
import math
import re
from DataStructures.List import array_list as lt
from DataStructures.Stack import stack as st
from DataStructures.Queue import queue as q
from DataStructures.Map import map_linear_probing as mp

data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Data", "Challenge-2")

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
    catalogo = {
        "trips": None,
        "barrios": None,
        "idx_req5": None
    }
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

    primeras5_rows = []
    ultimas5_rows = []

    primeras_slice = elements[:5] if size > 0 else []
    
    for trip in primeras_slice:
        primeras5_rows.append( [{
            "pickup_datetime": trip["pickup_datetime"],
            "dropoff_datetime": trip["dropoff_datetime"],
            "duration_min": round(trip["duration_min"], 2),
            "trip_distance": round(trip["trip_distance"], 3),
            "total_amount": round(trip["total_amount"], 2),
        }])

    ultimas_slice = elements[-5:] 
    for t in ultimas_slice:
        ultimas5_rows.append([{
            "pickup_datetime": t["pickup_datetime"],
            "dropoff_datetime": t["dropoff_datetime"],
            "duration_min": round(t["duration_min"], 2),
            "trip_distance": round(t["trip_distance"], 3),
            "total_amount": round(t["total_amount"], 2),
        }])

    total_registros = lt.size(catalog["trips"])

    return catalog, max_trip, min_trip, primeras5_rows, ultimas5_rows, tiempo_milisegundos, total_registros

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
    
    return catalog, tiempo_milisegundos, total_registros, cargados


def clave_hora_terminacion(trip):
    # Llave: "%Y-%m-%d %H" tomada de dropoff_datetime
    # dropoff_datetime viene como "YYYY-MM-DD HH:MM:SS"
    dt = trip["dropoff_datetime"]
    # Primeros 13 caracteres: "YYYY-MM-DD HH"
    return dt[:13]

def construir_indice_por_hora_terminacion(catalog):
    # Construye índice hash: key -> lista de viajes con esa fecha+hora de terminación
    if "idx_req5" in catalog and catalog["idx_req5"] is not None:
        return
    index = mp.new_map(15000, 0.5)
    trips = catalog["trips"]["elements"]
    for t in trips:
        k = clave_hora_terminacion(t)
        bucket = mp.get(index, k)
        if bucket is None:
            bucket = lt.new_list()
            mp.put(index, k, bucket)
        lt.add_last(bucket, t)
    catalog["idx_req5"] = index

# Funciones de consulta sobre el catálogo

def get_data(catalog, idx):
    """
    Retorna un dato por su índice (0-based) dentro de trips.
    """
    return lt.get_element(catalog["trips"], idx)



def req_1(catalog, start_dt_str, end_dt_str, sample_n):
    """
    Retorna el resultado del requerimiento 1 (filtrado por franja de recogida), con salida estilo REQ 3 y compatibilidad hacia atrás.
    """
    start_clock = get_time()
    
    start_tuple = time.strptime(start_dt_str, "%Y-%m-%d %H:%M:%S")
    end_tuple   = time.strptime(end_dt_str,   "%Y-%m-%d %H:%M:%S")
    start_ts = time.mktime(start_tuple)
    end_ts   = time.mktime(end_tuple)

    
    trips_pylist = catalog["trips"]["elements"]
    filtrados = lt.new_list()
    for t in trips_pylist:
        if start_ts <= t["pickup_ts"] and t["pickup_ts"] <= end_ts:
            lt.add_last(filtrados, t)

    
    def cmp_pickup_ts_asc(t1, t2):
        return t1["pickup_ts"] < t2["pickup_ts"]

    size_filtrados = lt.size(filtrados)
    if size_filtrados > 1:
        filtrados = lt.merge_sort(filtrados, cmp_pickup_ts_asc)

    if isinstance(sample_n, int):
        N = sample_n
    else:
        s = str(sample_n).strip()
        N = int(s) if s.isdigit() else 5
    if N <= 0:
        N = 5

    
    primeros = lt.new_list()
    ultimos  = lt.new_list()

    if size_filtrados <= 2 * N:
        # Mostrar todos en "primeros" y reutilizar en "ultimos"
        for t in filtrados["elements"]:
            lt.add_last(primeros, {
                "pickup_datetime": t["pickup_datetime"],
                "pickup_coords": [t["pickup_latitude"], t["pickup_longitude"]],
                "dropoff_datetime": t["dropoff_datetime"],
                "dropoff_coords": [t["dropoff_latitude"], t["dropoff_longitude"]],
                "trip_distance": round(t["trip_distance"], 3),
                "total_amount": round(t["total_amount"], 2)
            })
        ultimos = primeros
    else:
        # N primeros
        for i in range(N):
            t = lt.get_element(filtrados, i)
            lt.add_last(primeros, {
                "pickup_datetime": t["pickup_datetime"],
                "pickup_coords": [t["pickup_latitude"], t["pickup_longitude"]],
                "dropoff_datetime": t["dropoff_datetime"],
                "dropoff_coords": [t["dropoff_latitude"], t["dropoff_longitude"]],
                "trip_distance": round(t["trip_distance"], 3),
                "total_amount": round(t["total_amount"], 2)
            })
        # N últimos
        for i in range(size_filtrados - N, size_filtrados):
            t = lt.get_element(filtrados, i)
            lt.add_last(ultimos, {
                "pickup_datetime": t["pickup_datetime"],
                "pickup_coords": [t["pickup_latitude"], t["pickup_longitude"]],
                "dropoff_datetime": t["dropoff_datetime"],
                "dropoff_coords": [t["dropoff_latitude"], t["dropoff_longitude"]],
                "trip_distance": round(t["trip_distance"], 3),
                "total_amount": round(t["total_amount"], 2)
            })

    end_clock = get_time()
    tiempo_ms = round(delta_time(start_clock, end_clock), 3)

    
    first_py = []
    for e in primeros["elements"]:
        first_py.append(e)
    last_py = []
    for e in ultimos["elements"]:
        last_py.append(e)

    return {
        "tiempo_ms": tiempo_ms,
        "total_filtrados": size_filtrados,
        "primeros": primeros,
        "ultimos": ultimos,
        "elapsed_ms": tiempo_ms,
        "total_trips": size_filtrados,
        "first_n": first_py,
        "last_n": last_py
    }


def req_2(catalog, lat_min, lat_max, N):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    start = get_time()

    # Aca lo que hacemos es que filtramos los viajes por lo que dice el requerimiento
    filtrados = lt.new_list()
    size_trips = lt.size(catalog["trips"])

    for i in range(size_trips):
        trip = get_data(catalog, i)
        lat = trip["pickup_latitude"]
        if lat_min <= lat and lat <= lat_max:
            lt.add_last(filtrados, trip)

    # Definimos el criterio de ordenamiento especifico para el requisito 2
    def sort_criteria_req2(a, b):
        if a["pickup_latitude"] > b["pickup_latitude"]:
            return True
        elif a["pickup_latitude"] < b["pickup_latitude"]:
            return False
        else:
            return a["pickup_longitude"] > b["pickup_longitude"]

    # Aca ordenamos usando merge sort
    size_filtrados = lt.size(filtrados)
    if size_filtrados > 1:
        filtrados = lt.merge_sort(filtrados, sort_criteria_req2)

    # Creamos las listas para los primeros y ultimos
    primeros = []
    ultimos = []

    if size_filtrados <= 2 * N:
        # Se devuelve solamente una vez los elementos y se igualan
        for i in range(size_filtrados):
            t = lt.get_element(filtrados, i)
            primeros.append(primeros({
                "pickup_datetime": t["pickup_datetime"],
                "pickup_coords": [t["pickup_latitude"], t["pickup_longitude"]],
                "dropoff_datetime": t["dropoff_datetime"],
                "dropoff_coords": [t["dropoff_latitude"], t["dropoff_longitude"]],
                "trip_distance": round(t["trip_distance"], 3),
                "total_amount": round(t["total_amount"], 2)
            }))
        # Esto es para decir que los elementos sean iguales para no imprimirlos 2 veces porque los filtrados son menor que 2N
        ultimos = primeros 
    else:
        # Aca agregamos los primeros elementos el rango de N
        for i in range(N):
            t = lt.get_element(filtrados, i)
            primeros.append({
                "pickup_datetime": t["pickup_datetime"],
                "pickup_coords": [t["pickup_latitude"], t["pickup_longitude"]],
                "dropoff_datetime": t["dropoff_datetime"],
                "dropoff_coords": [t["dropoff_latitude"], t["dropoff_longitude"]],
                "trip_distance": round(t["trip_distance"], 3),
                "total_amount": round(t["total_amount"], 2)
            })

        # Aca agregamos los ultimos elementos en el rango de N
        for i in range(size_filtrados - N, size_filtrados):
            t = lt.get_element(filtrados, i)
            ultimos.append({
                "pickup_datetime": t["pickup_datetime"],
                "pickup_coords": [t["pickup_latitude"], t["pickup_longitude"]],
                "dropoff_datetime": t["dropoff_datetime"],
                "dropoff_coords": [t["dropoff_latitude"], t["dropoff_longitude"]],
                "trip_distance": round(t["trip_distance"], 3),
                "total_amount": round(t["total_amount"], 2)
            })

    end = get_time()
    tiempo_ms = round(delta_time(start, end), 3)

    return {
        "tiempo_ms": tiempo_ms,
        "total_filtrados": size_filtrados,
        "primeros": primeros,
        "ultimos": ultimos
    }


def req_3(catalog, distancia_min, distancia_max, n_muestra):
    """
    Filtra trayectos con trip_distance en [distancia_min, distancia_max],
    ordena por distancia ↓ y, si empata, por total_amount ↓,
    y retorna {tiempo_ms, total, primeros, ultimos}.
    """
    inicio_ms = get_time()

    # Filtrar
    filtrados = lt.new_list()
    for t in catalog["trips"]["elements"]:
        d = t["trip_distance"]
        if distancia_min <= d <= distancia_max:
            lt.add_last(filtrados, t)

    # Comparator: distancia ↓, costo ↓
    def cmp_desc_dist_cost(a, b):
        if a["trip_distance"] != b["trip_distance"]:
            return a["trip_distance"] > b["trip_distance"]
        return a["total_amount"] > b["total_amount"]

    filtrados = lt.merge_sort(filtrados, cmp_desc_dist_cost)
    total = lt.size(filtrados)

    # Armar salida
    primeros, ultimos = [], []

    def fila(t):
        return {
            "pickup_datetime": t["pickup_datetime"],
            "pickup_coord": f'[{round(t["pickup_latitude"],5)}, {round(t["pickup_longitude"],5)}]',
            "dropoff_datetime": t["dropoff_datetime"],
            "dropoff_coord": f'[{round(t["dropoff_latitude"],5)}, {round(t["dropoff_longitude"],5)}]',
            "trip_distance(mi)": round(t["trip_distance"], 3),
            "total_amount": round(t["total_amount"], 2)
        }

    if total <= 2 * n_muestra:
        i = 0
        while i < total:
            primeros.append(fila(lt.get_element(filtrados, i)))
            i += 1
    else:
        i = 0
        while i < n_muestra:
            primeros.append(fila(lt.get_element(filtrados, i)))
            i += 1
        i = total - n_muestra
        while i < total:
            ultimos.append(fila(lt.get_element(filtrados, i)))
            i += 1

    fin_ms = get_time()
    return {
        "tiempo_ms": round(delta_time(inicio_ms, fin_ms), 3),
        "total": total,
        "primeros": primeros,
        "ultimos": ultimos
    }



def req_4(catalog, fecha_yyyy_mm_dd, momento_interes, hora_referencia_hms, n_muestra):
    """
    Usa Hash cuya llave es la fecha de terminación 'YYYY-MM-DD'.
    Filtra:
      - ANTES   -> dropoff_time < hora_referencia_hms
      - DESPUES -> dropoff_time > hora_referencia_hms
    Orden final: dropoff_datetime ↓ (más reciente primero).
    Retorna {tiempo_ms, total, primeros, ultimos}.
    """
    inicio_ms = get_time()

    # Índice hash por fecha de terminación
    indice = mp.new_map(num_elements=max(1, lt.size(catalog["trips"])), load_factor=0.5)
    for t in catalog["trips"]["elements"]:
        llave = t["dropoff_datetime"][:10]  # YYYY-MM-DD
        balde = mp.get(indice, llave)
        if balde is None:
            balde = lt.new_list()
            mp.put(indice, llave, balde)
        lt.add_last(balde, t)

    del_dia = mp.get(indice, fecha_yyyy_mm_dd)

    # Filtrar por hora respecto a referencia
    candidatos = lt.new_list()
    if del_dia is not None and lt.size(del_dia) > 0:
        i = 0
        tam = lt.size(del_dia)
        if momento_interes == "ANTES":
            while i < tam:
                t = lt.get_element(del_dia, i)
                hhmmss = t["dropoff_datetime"][11:19]
                if hhmmss < hora_referencia_hms:
                    lt.add_last(candidatos, t)
                i += 1
        else:  # DESPUES
            while i < tam:
                t = lt.get_element(del_dia, i)
                hhmmss = t["dropoff_datetime"][11:19]
                if hhmmss > hora_referencia_hms:
                    lt.add_last(candidatos, t)
                i += 1

    # Ordenar por dropoff_datetime ↓
    def cmp_drop_desc(a, b):
        return a["dropoff_datetime"] > b["dropoff_datetime"]

    candidatos = lt.merge_sort(candidatos, cmp_drop_desc)
    total = lt.size(candidatos)

    # Armar salida
    primeros, ultimos = [], []

    def fila(t):
        return {
            "pickup_datetime": t["pickup_datetime"],
            "pickup_coord": f'[{round(t["pickup_latitude"],5)}, {round(t["pickup_longitude"],5)}]',
            "dropoff_datetime": t["dropoff_datetime"],
            "dropoff_coord": f'[{round(t["dropoff_latitude"],5)}, {round(t["dropoff_longitude"],5)}]',
            "trip_distance(mi)": round(t["trip_distance"], 3),
            "total_amount": round(t["total_amount"], 2)
        }

    if total <= 2 * n_muestra:
        i = 0
        while i < total:
            primeros.append(fila(lt.get_element(candidatos, i)))
            i += 1
    else:
        i = 0
        while i < n_muestra:
            primeros.append(fila(lt.get_element(candidatos, i)))
            i += 1
        i = total - n_muestra
        while i < total:
            ultimos.append(fila(lt.get_element(candidatos, i)))
            i += 1

    fin_ms = get_time()
    return {
        "tiempo_ms": round(delta_time(inicio_ms, fin_ms), 3),
        "total": total,
        "primeros": primeros,
        "ultimos": ultimos
    }


def req_5(catalog, term_dt_hour_str, sample_n=5):
    
    start_clock = get_time()

    #
    construir_indice_por_hora_terminacion(catalog)
    idx = catalog["idx_req5"]

    
    bucket = mp.get(idx, term_dt_hour_str)
    if bucket is None:
        tiempo_ms = round(delta_time(start_clock, get_time()), 3)
        
        vacia = lt.new_list()
        return {
            "tiempo_ms": tiempo_ms,
            "total_filtrados": 0,
            "primeros": vacia,
            "ultimos": vacia,
            "elapsed_ms": tiempo_ms,
            "total_trips": 0,
            "first_n": [],
            "last_n": []
        }

    
    def cmp_dropoff_ts_desc(t1, t2):
        return t1["dropoff_ts"] > t2["dropoff_ts"]

    size_bucket = lt.size(bucket)
    if size_bucket > 1:
        bucket = lt.merge_sort(bucket, cmp_dropoff_ts_desc)

    
    if isinstance(sample_n, int):
        n_val = sample_n
    else:
        s = str(sample_n).strip()
        n_val = int(s) if s.isdigit() else 5
    if n_val <= 0:
        n_val = 5

    
    primeros = lt.new_list()
    ultimos  = lt.new_list()

    if size_bucket <= 2 * n_val:
        for t in bucket["elements"]:
            lt.add_last(primeros, {
                "pickup_datetime": t["pickup_datetime"],
                "pickup_coords": [t["pickup_latitude"], t["pickup_longitude"]],
                "dropoff_datetime": t["dropoff_datetime"],
                "dropoff_coords": [t["dropoff_latitude"], t["dropoff_longitude"]],
                "trip_distance": round(t["trip_distance"], 3),
                "total_amount": round(t["total_amount"], 2)
            })
        ultimos = primeros
    else:
        for i in range(n_val):
            t = lt.get_element(bucket, i)
            lt.add_last(primeros, {
                "pickup_datetime": t["pickup_datetime"],
                "pickup_coords": [t["pickup_latitude"], t["pickup_longitude"]],
                "dropoff_datetime": t["dropoff_datetime"],
                "dropoff_coords": [t["dropoff_latitude"], t["dropoff_longitude"]],
                "trip_distance": round(t["trip_distance"], 3),
                "total_amount": round(t["total_amount"], 2)
            })
        for i in range(size_bucket - n_val, size_bucket):
            t = lt.get_element(bucket, i)
            lt.add_last(ultimos, {
                "pickup_datetime": t["pickup_datetime"],
                "pickup_coords": [t["pickup_latitude"], t["pickup_longitude"]],
                "dropoff_datetime": t["dropoff_datetime"],
                "dropoff_coords": [t["dropoff_latitude"], t["dropoff_longitude"]],
                "trip_distance": round(t["trip_distance"], 3),
                "total_amount": round(t["total_amount"], 2)
            })

    tiempo_ms = round(delta_time(start_clock, get_time()), 3)

    
    first_py = []
    for e in primeros["elements"]:
        first_py.append(e)
    last_py = []
    for e in ultimos["elements"]:
        last_py.append(e)

    return {
        "tiempo_ms": tiempo_ms,
        "total_filtrados": size_bucket,
        "primeros": primeros,
        "ultimos": ultimos,
        "elapsed_ms": tiempo_ms,
        "total_trips": size_bucket,
        "first_n": first_py,
        "last_n": last_py
    }

def req_6(catalog, barrio_buscar, hora_inicio_str, hora_fin_str, n_muestra):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    
    # Se define la funcion haversine como pide en el documento
    def haversine(lat1, lon1, lat2, lon2):
        R = 6371  # Radio de la Tierra en km
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return R * c

    inicio_ms = get_time()

    # Normalizar barrio
    barrio_key = (barrio_buscar or "").strip().lower()

    # Parsear horas
    def parse_hour(s, default):
        if s is None:
            return default
        ss = str(s).strip()
        if ss.isdigit():
            v = int(ss)
            if 0 <= v <= 23:
                return v
        return default

    h_start = parse_hour(hora_inicio_str, 0)
    h_end = parse_hour(hora_fin_str, 23)

    # Parsear N
    def parse_n(s, default):
        # Caso: s ya es un número entero positivo
        if type(s) in (int, float) and s == int(s) and s > 0:
            return int(s)

        # Caso: s es string que representa un entero positivo
        if type(s) == str:
            ss = s.strip()
            if ss.isdigit():  # solo acepta dígitos, sin signos ni decimales
                v = int(ss)
                if v > 0:
                    return v

        return default

    N = parse_n(n_muestra, 5)

    # Construir índice hash por barrio con el indice de los barrios como pide el documento
    barrios_list = catalog["barrios"]
    size_barrios = lt.size(barrios_list)
    idx_barrios = mp.new_map(max(1, size_barrios), 0.5)
    for i in range(size_barrios):
        b = lt.get_element(barrios_list, i)
        nb_key = b["neighborhood"].strip().lower()
        if mp.get(idx_barrios, nb_key) is None:
            mp.put(idx_barrios, nb_key, lt.new_list())

    # Asignar cada viaje al barrio más cercano
    for i in range(lt.size(catalog["trips"])):
        t = get_data(catalog, i)
        plat = t.get("pickup_latitude")
        plon = t.get("pickup_longitude")
        if plat is not None and plon is not None:
            mejor_nb = None
            mejor_dist = None
            for j in range(lt.size(barrios_list)):
                b = lt.get_element(barrios_list, j)
                lat_b = b["latitude"]
                lon_b = b["longitude"]
                if lat_b is not None or lon_b is not None:
                    d = haversine(plat, plon, lat_b, lon_b)
                    if mejor_dist is None or d < mejor_dist:
                        mejor_dist = d
                        mejor_nb = b["neighborhood"].strip().lower()
            if mejor_nb is not None:
                bucket = mp.get(idx_barrios, mejor_nb)
                if bucket is None:
                    bucket = lt.new_list()
                    mp.put(idx_barrios, mejor_nb, bucket)
                lt.add_last(bucket, t)

    # Obtener bucket del barrio buscado
    bucket_barrio = mp.get(idx_barrios, barrio_key)
    if bucket_barrio is None or lt.size(bucket_barrio) == 0:
        tiempo_ms = round(delta_time(inicio_ms, get_time()), 3)
        return {"tiempo_ms": tiempo_ms, 
        "total": 0, 
        "primeros": [], 
        "ultimos": []
        }

    # Filtrar por hora
    candidatos = lt.new_list()
    tam = lt.size(bucket_barrio)
    crosses_midnight = (h_start > h_end)
    i = 0
    while i < tam:
        trip = lt.get_element(bucket_barrio, i)
        ph = trip.get("pickup_hour")
        if ph is None:
            pd = trip.get("pickup_datetime")
            if isinstance(pd, str) and len(pd) >= 13:
                hh = pd[11:13]
                if hh.isdigit():
                    ph = int(hh)
        if ph is not None:
            if not crosses_midnight:
                if h_start <= ph and ph <= h_end:
                    lt.add_last(candidatos, trip)
            else:
                if ph >= h_start or ph <= h_end:
                    lt.add_last(candidatos, trip)
        i += 1

    total = lt.size(candidatos)

    # Ordenar por pickup_ts ascendente (más antiguo primero)
    def sort_criteria_req6(a, b):
        return a["pickup_ts"] < b["pickup_ts"]

    if total > 1:
        candidatos = lt.merge_sort(candidatos, sort_criteria_req6)

    # Preparar salida
    primeros = []
    ultimos = []

    def fila(t):
        return {
            "pickup_datetime": t["pickup_datetime"],
            "pickup_coords": [round(t["pickup_latitude"], 6), round(t["pickup_longitude"], 6)],
            "dropoff_datetime": t["dropoff_datetime"],
            "dropoff_coords": [round(t["dropoff_latitude"], 6), round(t["dropoff_longitude"], 6)],
            "trip_distance": round(t["trip_distance"], 3),
            "total_amount": round(t["total_amount"], 2)
        }

    if total == 0:
        tiempo_ms = round(delta_time(inicio_ms, get_time()), 3)
        return {"tiempo_ms": tiempo_ms,
        "total": 0, 
        "primeros": primeros, 
        "ultimos": ultimos
        }

    if total <= 2 * N:
        j = 0
        while j < total:
            primeros.append(fila(lt.get_element(candidatos, j)))
            j += 1
        ultimos = primeros
    else:
        j = 0
        while j < N:
            primeros.append(fila(lt.get_element(candidatos, j)))
            j += 1
        j = total - N
        while j < total:
            ultimos.append(fila(lt.get_element(candidatos, j)))
            j += 1

    tiempo_ms = round(delta_time(inicio_ms, get_time()), 3)
    return {"tiempo_ms": tiempo_ms, 
    "total": total, 
    "primeros": primeros, 
    "ultimos": ultimos
    }


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
