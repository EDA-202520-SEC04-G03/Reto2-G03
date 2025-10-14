def new_list():
    newlist = {
        "elements": [],
        "size": 0,
    }
    return newlist

def get_element(my_list, index):
    return my_list["elements"][index]


def is_present(my_list, element, cmp_function):
    size = my_list["size"]
    if size > 0:
        keyexist = False
        for keypos in range(0, size):
            info = my_list["elements"][keypos]
            if cmp_function(element, info) == 0:
                keyexist = True
                break
        if keyexist:
            return keypos
    return -1

def add_first(my_list, element):
    my_list["elements"].insert(0, element)
    my_list["size"] += 1
    return my_list

def add_last(my_list, element):
    my_list["elements"].append(element)
    my_list["size"] += 1
    return my_list

def size(my_list):
    return my_list["size"]

def first_element(my_list):
    firstelement = -1
    if my_list["size"] == 0:
        firstelement = None
    else:
        firstelement = my_list["elements"][0]
    return firstelement

def is_empty(my_list):
    return my_list["size"] == 0

def last_element(my_list):
    lastelement = -1
    if my_list["size"] == 0:
        lastelement = None
    else:
        lastelement = my_list["elements"][-1]
    return lastelement

def delete_element(my_list, pos):
    element_removed = my_list["elements"].pop(pos)
    my_list["size"] -= 1
    return my_list

def remove_first(my_list):
    element_removed = my_list["elements"].pop(0)
    my_list["size"] -= 1
    return element_removed

def remove_last(my_list):
    element_removed = my_list["elements"].pop()
    my_list["size"] -= 1
    return element_removed

def insert_element(my_list, element, pos):
    my_list["elements"].insert(pos, element)
    my_list["size"] += 1
    return my_list

def change_info(my_list, pos, new_info):
    my_list["elements"][pos] = new_info
    return my_list

def exchange(my_list, pos_1, pos_2):
    my_list["elements"][pos_1], my_list["elements"][pos_2] = my_list["elements"][pos_2], my_list["elements"][pos_1]
    return my_list
    
def sub_list(my_list, pos_1, num_elements):
    if pos_1 < 0 or pos_1 >= my_list["size"]:
        raise IndexError("list index out of range")

    end_pos = pos_1 + num_elements

    sublist_elements = my_list["elements"][pos_1:end_pos]

    new_list_structure = new_list()
    new_list_structure["elements"] = sublist_elements
    new_list_structure["size"] = len(sublist_elements)

    return new_list_structure

def default_sort_criteria(element_1, element_2):
    is_sorted = False
    if element_1 < element_2:
        is_sorted = True
    return is_sorted

def selection_sort(my_list, sort_crit=None):
    if sort_crit is None:
        sort_crit = default_sort_criteria

    tamaño_lista = my_list["size"]
    
    if tamaño_lista <= 1:
        return my_list
    
    for i in range(tamaño_lista - 1):
        min_index = i
        for j in range(i + 1, tamaño_lista):
            if sort_crit(my_list["elements"][j], my_list["elements"][min_index]):
                min_index = j
        if min_index != i:
            my_list = exchange(my_list, i, min_index)

    return my_list

def insertion_sort(my_list, sort_crit=None):
    if sort_crit is None:
        sort_crit = default_sort_criteria

    tamaño_lista = my_list["size"]
    
    if tamaño_lista <= 1:
        return my_list
    
    for i in range(1, tamaño_lista):
        key = my_list["elements"][i]
        j = i - 1
        while j >= 0 and sort_crit(key, my_list["elements"][j]):
            my_list["elements"][j + 1] = my_list["elements"][j]
            j -= 1
        my_list["elements"][j + 1] = key

    return my_list


def shell_sort(my_list, sort_crit=None):
    if sort_crit is None:
        sort_crit = default_sort_criteria

    tamaño_lista = my_list["size"]
    
    if tamaño_lista <= 1:
        return my_list
    
    gap = tamaño_lista // 2

    while gap > 0:
        for i in range(gap, tamaño_lista):
            temp = my_list["elements"][i]
            j = i
            while j >= gap and sort_crit(temp, my_list["elements"][j - gap]):
                my_list["elements"][j] = my_list["elements"][j - gap]
                j -= gap
            my_list["elements"][j] = temp
        gap //= 2

    return my_list

def merge_sort(my_list, sort_crit=None):
    if sort_crit is None:
        sort_crit = default_sort_criteria

    tamaño_lista = my_list["size"]
    
    if tamaño_lista <= 1:
        return my_list

    mid = tamaño_lista // 2
    left_half = sub_list(my_list, 0, mid)
    right_half = sub_list(my_list, mid, tamaño_lista - mid)

    left_half = merge_sort(left_half, sort_crit)
    right_half = merge_sort(right_half, sort_crit)

    merged_list = new_list()
    i = j = 0

    while i < left_half["size"] and j < right_half["size"]:
        if sort_crit(left_half["elements"][i], right_half["elements"][j]):
            merged_list = add_last(merged_list, left_half["elements"][i])
            i += 1
        else:
            merged_list = add_last(merged_list, right_half["elements"][j])
            j += 1

    while i < left_half["size"]:
        merged_list = add_last(merged_list, left_half["elements"][i])
        i += 1

    while j < right_half["size"]:
        merged_list = add_last(merged_list, right_half["elements"][j])
        j += 1

    return merged_list

def quick_sort(my_list, sort_crit=None):
    if sort_crit is None:
        sort_crit = default_sort_criteria

    tamano_lista = size(my_list)
    if tamano_lista <= 1:
        return my_list
    # Elegimos el último elemento como pivote y lo ponemos en su lugar
    def partition(low, high):
        pivot = my_list["elements"][high]
        i = low
        for j in range(low, high):
            element = get_element(my_list, j)
            if sort_crit(element, pivot):
                exchange(my_list, i, j)
                i += 1
        exchange(my_list, i, high)
        return i
    
    # Aca definimos una funcion que se va a llamar recursivamente para hacer el quick_sort
    # Esto en base a lo que dice la documentacion y para no perder el O(1) en almacenamiento que tiene quick sort
    # Porque se podria llamar recursivamente la funcion que ya tenemos con sublistas de nuestra lista
    # Pero ahí perdemos el O(1) en espacial y lo que hace muy util a quick sort
    def recursiva_quick_sort(low, high):
        if low < high:
            pos_pivote = partition(low, high)
            recursiva_quick_sort(pos_pivote + 1, high)
            recursiva_quick_sort(low, pos_pivote - 1)
    
    low = 0
    high = tamano_lista - 1
    recursiva_quick_sort(low, high)
    
    return my_list
