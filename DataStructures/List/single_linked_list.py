def new_list():
    newlist = {
        "first": None,
        "last": None,
        "size": 0,
    }
    return newlist


def get_element(my_list, pos):
    searchpos = 0
    node = my_list["first"]
    while searchpos < pos:
        node = node["next"]
        searchpos += 1
    return node["info"]

def is_present(my_list, element, cmp_function):
    is_in_array = False
    temp = my_list["first"]
    count = 0
    while not is_in_array and temp is not None:
        if cmp_function(element, temp["info"]) == 0:
            is_in_array = True
        else:
            temp = temp["next"]
            count += 1

    if not is_in_array:
        count = -1
    return count


def add_first(my_list, element):
    new_node = {
        "info": element,
        "next": my_list["first"]
    }
    my_list["first"] = new_node
    if my_list["last"] is None:
        my_list["last"] = new_node
    my_list["size"] += 1
    return my_list


def add_last(my_list, element):
    new_node = {
        "info": element,
        "next": None
    }
    if my_list["last"] is not None:
        my_list["last"]["next"] = new_node
    my_list["last"] = new_node
    if my_list["first"] is None:
        my_list["first"] = new_node
    my_list["size"] += 1
    return my_list

def size(my_list):
    return my_list["size"]  

def first_element(my_list):
    if my_list["first"] is not None:
        return my_list["first"]["info"]
    return None

def is_empty(my_list):
    return my_list["size"] == 0

def last_element(my_list):
    if my_list["last"] is not None:
        return my_list["last"]["info"]
    return None

def remove_first(my_list):
    if my_list["first"] is None:
        return None
    element_removed = my_list["first"]["info"]
    my_list["first"] = my_list["first"]["next"]
    my_list["size"] -= 1
    if my_list["first"] is None:
        my_list["last"] = None
    return element_removed

def remove_last(my_list):
    if my_list["last"] is None:
        return None
    element_removed = my_list["last"]["info"]
    if my_list["first"] == my_list["last"]:
        my_list["first"] = None
        my_list["last"] = None
    else:
        current = my_list["first"]
        while current["next"] != my_list["last"]:
            current = current["next"]
        current["next"] = None
        my_list["last"] = current
    my_list["size"] -= 1
    return element_removed

def remove_last(my_list):
    if my_list["last"] is None:
        return None
    element_removed = my_list["last"]["info"]
    if my_list["first"] == my_list["last"]:
        my_list["first"] = None
        my_list["last"] = None
    else:
        current = my_list["first"]
        while current["next"] != my_list["last"]:
            current = current["next"]
        current["next"] = None
        my_list["last"] = current
    my_list["size"] -= 1
    return element_removed


def insert_element(my_list, element, pos):
    if pos == 0:
        return add_first(my_list, element)
    elif pos == my_list["size"]:
        return add_last(my_list, element)
    else:
        new_node = {
            "info": element,
            "next": None
        }
        current = my_list["first"]
        for _ in range(pos - 1):
            current = current["next"]
        new_node["next"] = current["next"]
        current["next"] = new_node
        my_list["size"] += 1
        return my_list
    
def insert_element(my_list, element, pos):
    if pos == 0:
        return add_first(my_list, element)
    elif pos == my_list["size"]:
        return add_last(my_list, element)
    else:
        new_node = {
            "info": element,
            "next": None
        }
        current = my_list["first"]
        for _ in range(pos - 1):
            current = current["next"]
        new_node["next"] = current["next"]
        current["next"] = new_node
        my_list["size"] += 1
        return my_list
    

def change_info(my_list, pos, new_element):
    current = my_list["first"]
    for _ in range(pos):
        current = current["next"]
    current["info"] = new_element
    return my_list

def exchange(my_list, pos1, pos2):
    if pos1 == pos2:
        return my_list
    current1 = my_list["first"]
    for _ in range(pos1):
        current1 = current1["next"]
    current2 = my_list["first"]
    for _ in range(pos2):
        current2 = current2["next"]
    current1["info"], current2["info"] = current2["info"], current1["info"]
    return my_list

def sub_list(my_list, pos, num_elements):
    if pos < 0 or pos >= my_list["size"] or num_elements < 0 or (pos + num_elements) > my_list["size"]:
        raise IndexError("list index out of range")
    
    new_list_structure = new_list()
    current = my_list["first"]
    for _ in range(pos):
        current = current["next"]
    
    for _ in range(num_elements):
        new_list_structure = add_last(new_list_structure, current["info"])
        current = current["next"]
        
    return new_list_structure
        
def delete_element(my_list, pos):
    if pos < 0 or pos >= my_list["size"]:
        raise IndexError("list index out of range")
    if pos == 0:
        remove_first(my_list)
    elif pos == my_list["size"] - 1:
        remove_last(my_list)
    else:
        current = my_list["first"]
        for _ in range(pos - 1):
            current = current["next"]
        current["next"] = current["next"]["next"]
        my_list["size"] -= 1
    return my_list

def default_sort_criteria(element_1, element_2):
    is_sorted = False
    if element_1 < element_2:
        is_sorted = True
    return is_sorted

def insertion_sort(my_list, sort_crit=None):
    if sort_crit is None:
        sort_crit = default_sort_criteria

    if my_list["size"] <= 1 or my_list["first"] is None:
        return my_list

    prev_i = my_list["first"]
    i = prev_i["next"]
    last_node = prev_i

    while i is not None:
        key = i
        if not sort_crit(key["info"], prev_i["info"]):
            last_node = i
            prev_i = i
            i = i["next"]
            continue

        prev_i["next"] = key["next"]

        if sort_crit(key["info"], my_list["first"]["info"]):
            key["next"] = my_list["first"]
            my_list["first"] = key
        else:
            search = my_list["first"]
            while (search["next"] is not None and not sort_crit(key["info"], search["next"]["info"])):
                search = search["next"]

            key["next"] = search["next"]
            search["next"] = key

        i = prev_i["next"]
        if prev_i["next"] is None:
            last_node = prev_i

    my_list["last"] = last_node
    return my_list

def selection_sort(my_list, sort_crit=None):
    if sort_crit is None:
        sort_crit = default_sort_criteria
    
    if my_list["size"] <= 1:
        return my_list

    first = my_list["first"]
    while first is not None:
        min_node = first
        actual = first["next"]
        while actual is not None:
            if sort_crit(actual["info"], min_node["info"]):
                min_node = actual
            actual = actual["next"]

        if min_node is not first:
            first["info"], min_node["info"] = min_node["info"], first["info"]

        tail = first
        first = first["next"]

    my_list["last"] = tail

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
            temp = get_element(my_list, i)
            j = i
            while j >= gap and sort_crit(temp, get_element(my_list, j - gap)):
                change_info(my_list, j, get_element(my_list, j - gap))
                j -= gap
            change_info(my_list, j, temp)
        gap //= 2

    return my_list

def merge_sort(my_list, sort_crit=None):
    if sort_crit is None:
        sort_crit = default_sort_criteria

    if my_list["size"] <= 1 or my_list["first"] is None:
        return my_list

    def split_list(source):
        if source is None or source["next"] is None:
            return source, None
        slow = source
        fast = source["next"]
        while fast is not None:
            fast = fast["next"]
            if fast is not None:
                slow = slow["next"]
                fast = fast["next"]
        middle = slow["next"]
        slow["next"] = None
        return source, middle

    def merge_lists(left, right):
        if left is None:
            return right
        if right is None:
            return left
        if sort_crit(left["info"], right["info"]):
            result = left
            result["next"] = merge_lists(left["next"], right)
        else:
            result = right
            result["next"] = merge_lists(left, right["next"])
        return result

    def merge_sort_rec(node):
        if node is None or node["next"] is None:
            return node
        left, right = split_list(node)
        left = merge_sort_rec(left)
        right = merge_sort_rec(right)
        return merge_lists(left, right)

    my_list["first"] = merge_sort_rec(my_list["first"])

    current = my_list["first"]
    while current and current["next"] is not None:
        current = current["next"]
    my_list["last"] = current

    return my_list

def quick_sort(my_list, sort_crit=None):
    if sort_crit is None:
        sort_crit = default_sort_criteria
    
    

    tamano_lista = size(my_list)
    if tamano_lista <= 1:
        return my_list
    # Aquí ordenamos sin intercambiar elementos, sino que dejamos todos los elementos que 
    # Son mayores que el pivote por detrás y los que son menores los dejamos por delante
    # Asi no hacemos intercambios dentro de la lista y nos ahorramos complejidad temporal
    # Tomamos como pivote el primer elemento
    def partition(head, end):
        pivot = end
        prev = None
        current = head
        tail = pivot

        new_head = None
        new_end = tail

        while current is not pivot:
            if sort_crit(current["info"], pivot["info"]):
                # Va al lado izquierdo 
                if new_head is None:
                    new_head = current
                prev = current
                current = current["next"]
            else:
                # Mover current detrás de tail
                next = current["next"]
                if prev is not None:
                    prev["next"] = next
                else:
                    # avanzó la cabeza
                    head = next
                current["next"] = None
                tail["next"] = current
                tail = current
                current = next

        if new_head is None:
            # todos fueron mayores que el pivot, este es la nueva cabeza
            new_head = pivot

        new_end = tail
        return (new_head, new_end, pivot)
    
    def quick_sort_rec(head, end):
        if head is None or head is end:
            return head, end

        new_head, new_end, pivot = partition(head, end)

        # Ordenar lado izquierdo si existe algo antes del pivote
        if new_head is not pivot:
            # cortar antes del pivote para la recursión izquierda
            temp = new_head
            while temp["next"] is not pivot:
                temp = temp["next"]
            left_end = temp

            left_head, left_end = quick_sort_rec(new_head, left_end)
            # conectar cola izquierda con el pivote
            left_end["next"] = pivot
            new_head = left_head

        # Ordenar lado derecho (después del pivote)
        if pivot is not new_end:
            right_head, right_end = quick_sort_rec(pivot["next"], new_end)
            pivot["next"] = right_head
            new_end = right_end
        else:
            new_end = pivot

        return new_head, new_end

    # Ejecutar quicksort sobre toda la lista
    head = my_list["first"]
    tail = head
    while tail["next"] is not None:
        tail = tail["next"]

    new_head, new_end = quick_sort_rec(head, tail)

    my_list["first"] = new_head
    my_list["last"] = new_end

    if new_end is not None:
        new_end["next"] = None

    return my_list
    