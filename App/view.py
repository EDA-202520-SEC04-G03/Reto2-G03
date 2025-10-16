import sys
import os
import App.logic as logic
from tabulate import tabulate
default_limit = 1000
sys.setrecursionlimit(default_limit*10)

def new_logic():
    """
        Se crea una instancia del controlador
    """
    #TODO: Llamar la función de la lógica donde se crean las estructuras de datos
    return logic.new_logic()

def print_menu():
    print("Bienvenido")
    print("0- Cargar información")
    print("1- Ejecutar Requerimiento 1")
    print("2- Ejecutar Requerimiento 2")
    print("3- Ejecutar Requerimiento 3")
    print("4- Ejecutar Requerimiento 4")
    print("5- Ejecutar Requerimiento 5")
    print("6- Ejecutar Requerimiento 6")
    print("7- Salir")

def load_data(control):
    """
    Carga los datos
    """
    # Cargar viajes
    taxis_file = "taxis-large.csv"
    logic.load_data(control, taxis_file)

    # Cargar barrios
    barrios_file = "nyc-neighborhoods.csv"
    logic.load_neighborhoods(control, barrios_file)



def print_data(control, id):
    """
        Función que imprime un dato dado su ID
    """
    print("El elemento es: ", logic.get_data(control, id))

def print_req_1(control):
    """
        Función que imprime la solución del Requerimiento 1 en consola
    """
    print("\n=== REQ 1: Trayectos en franja de fecha y hora de recogida ===")
    start_dt = input("Fecha/hora inicial (AAAA-MM-DD HH:MM:SS): ").strip()
    end_dt   = input("Fecha/hora final   (AAAA-MM-DD HH:MM:SS): ").strip()
    sample_n = input("Tamaño de muestra N (enter para 5): ").strip()
    sample_n = int(sample_n) if sample_n else 5
    
    result = logic.req_1(control, start_dt, end_dt, sample_n)
    
    print(f"\nTiempo de ejecución: {result['elapsed_ms']} ms")
    print(f"Total de trayectos en la franja: {result['total_trips']}")
    
    # hacemos las tablas linda
    def rows_to_table(rows):
        table = []
        for r in rows:
            table.append([
                r["pickup_datetime"],
                f"[{r['pickup_coords'][0]}, {r['pickup_coords'][1]}]",
                r["dropoff_datetime"],
                f"[{r['dropoff_coords'][0]}, {r['dropoff_coords'][1]}]",
                r["trip_distance"],
                r["total_amount"],
            ])
        return table
    
    headers = ["Pickup (fecha/hora)", "Pickup [Lat, Lon]", "Dropoff (fecha/hora)", "Dropoff [Lat, Lon]", "Dist (mi)", "Costo (USD)"]
    
    first_tbl = rows_to_table(result["first_n"])
    last_tbl  = rows_to_table(result["last_n"])
    
    if first_tbl:
        print("\n-- N primeros (más antiguos) --")
        print(tabulate(first_tbl, headers=headers, tablefmt="grid", stralign="center"))
    else:
        print("\n(No hay trayectos para mostrar en el inicio del rango)")
    
    if last_tbl:
        print("\n-- N últimos (más recientes) --")
        print(tabulate(last_tbl, headers=headers, tablefmt="grid", stralign="center"))
    else:
        if result["total_trips"] > 0 and len(first_tbl) == result["total_trips"]:
            print("\n(Se mostraron todos los trayectos en la primera tabla por ser menos de 2N)")
    
    print("")  


def print_req_2(control):
    """
        Función que imprime la solución del Requerimiento 2 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 2
    pass


def print_req_3(control):
    """
        Función que imprime la solución del Requerimiento 3 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 3
    distancia_min = float(input("Valor inicial de distancia (millas): ").strip())
    distancia_max = float(input("Valor final de distancia (millas): ").strip())
    n_muestra = int(input("Tamaño de la muestra N (para primeros y últimos): ").strip())

    respuesta = logic.req_3(control, distancia_min, distancia_max, n_muestra)

    print(f"\nTiempo de ejecución: {respuesta['tiempo_ms']} ms")
    print(f"Número total de trayectos que cumplen el filtro: {respuesta['total']}")

    # Primeros N
    print("\n=== Primeros N trayectos ===")
    if len(respuesta["primeros"]) > 0:
        print(tabulate(respuesta["primeros"], headers="keys", tablefmt="psql"))
    else:
        print("(vacío)")

    # Últimos N (puede venir vacío si total < 2N)
    print("\n=== Últimos N trayectos ===")
    if len(respuesta["ultimos"]) > 0:
        print(tabulate(respuesta["ultimos"], headers="keys", tablefmt="psql"))
    else:
        print("(vacío)")


def print_req_4(control):
    """
        Función que imprime la solución del Requerimiento 4 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 4
    fecha_terminacion = input('Fecha de terminación (formato "YYYY-MM-DD"): ').strip()
    momento_interes = input('Momento de interés ("ANTES" o "DESPUES"): ').strip().upper()
    hora_referencia = input('Hora de referencia (formato "HH:MM:SS"): ').strip()
    n_muestra = int(input("Tamaño de la muestra N (para primeros y últimos): ").strip())

    respuesta = logic.req_4(control, fecha_terminacion, momento_interes, hora_referencia, n_muestra)

    print(f"\nTiempo de ejecución: {respuesta['tiempo_ms']} ms")
    print(f"Número total de trayectos que cumplen fecha y hora: {respuesta['total']}")

    # Primeros N
    print("\n=== Primeros N trayectos ===")
    if len(respuesta["primeros"]) > 0:
        print(tabulate(respuesta["primeros"], headers="keys", tablefmt="psql"))
    else:
        print("(vacío)")

    # Últimos N (puede venir vacío si total < 2N)
    print("\n=== Últimos N trayectos ===")
    if len(respuesta["ultimos"]) > 0:
        print(tabulate(respuesta["ultimos"], headers="keys", tablefmt="psql"))
    else:
        print("(vacío)")


def print_req_5(control):
    """
        Función que imprime la solución del Requerimiento 5 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 5
    pass


def print_req_6(control):
    """
        Función que imprime la solución del Requerimiento 6 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 6
    pass

# Se crea la lógica asociado a la vista
control = new_logic()

# main del ejercicio
def main():
    """
    Menu principal
    """
    working = True
    #ciclo del menu
    while working:
        print_menu()
        inputs = input('Seleccione una opción para continuar\n')
        if int(inputs) == 0:
            print("Cargando información de los archivos ....\n")
            data = load_data(control)
        elif int(inputs) == 1:
            print_req_1(control)

        elif int(inputs) == 2:
            print_req_2(control)

        elif int(inputs) == 3:
            print_req_3(control)

        elif int(inputs) == 4:
            print_req_4(control)

        elif int(inputs) == 5:
            print_req_5(control)

        elif int(inputs) == 6:
        elif int(inputs) == 6:
            print_req_6(control)

        elif int(inputs) == 7:
            working = False
            print("\nGracias por utilizar el programa") 
        else:
            print("Opción errónea, vuelva a elegir.\n")
    sys.exit(0)
