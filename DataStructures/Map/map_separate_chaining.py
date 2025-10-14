import DataStructures.Map.map_functions as mf
import DataStructures.List.single_linked_list as lt
import DataStructures.List.array_list as al
import DataStructures.Map.map_entry as me
import random



def new_map(num_elements, load_factor, prime=109345121):
    capacity = mf.next_prime(int(num_elements / load_factor))
    table = lt.new_list()
    c = 0
    while c < capacity:
        lt.add_last(table, me.new_map_entry(None, None))
        c += 1
    return {
        "prime": prime,
        "capacity": capacity,
        "scale": random.randrange(1, prime - 1),
        "shift": random.randrange(0, prime - 1),
        "table": table,
        "current_factor": 0,
        "limit_factor": load_factor,
        "size": 0
    }


def default_compare(key, element):
    k = me.get_key(element)
    if key == k:
        return 0
    elif k is None:
        return 1
    elif key > k:
        return 1
    return -1


def put(my_map, key, value):
    idx = mf.hash_value(my_map, key)
    table = my_map["table"]
    cell = lt.get_element(table, idx)

    if ("key" in cell) and ("value" in cell):
        if cell["key"] is None:
            bucket = lt.new_list()
            lt.change_info(table, idx, bucket)
            lt.add_last(bucket, me.new_map_entry(key, value))
            my_map["size"] += 1
        else:
            if default_compare(key, cell) == 0:
                cell["value"] = value
                lt.change_info(table, idx, cell)
            else:
                bucket = lt.new_list()
                lt.add_last(bucket, cell)
                lt.add_last(bucket, me.new_map_entry(key, value))
                lt.change_info(table, idx, bucket)
                my_map["size"] += 1
    elif ("first" in cell) and ("last" in cell) and ("size" in cell):
        bucket = cell
        found = False
        j = 0
        bsize = lt.size(bucket)
        while j < bsize and not found:
            entry = lt.get_element(bucket, j)
            if default_compare(key, entry) == 0:
                entry["value"] = value
                found = True
            else:
                j += 1
        if not found:
            lt.add_last(bucket, me.new_map_entry(key, value))
            my_map["size"] += 1
    else:
        bucket = lt.new_list()
        lt.add_last(bucket, me.new_map_entry(key, value))
        lt.change_info(table, idx, bucket)
        my_map["size"] += 1

    my_map["current_factor"] = my_map["size"] / my_map["capacity"]
    if my_map["current_factor"] > my_map["limit_factor"]:
        rehash(my_map)
    return my_map


def contains(my_map, key):
    if my_map["size"] == 0:
        return False
    idx = mf.hash_value(my_map, key)
    cell = lt.get_element(my_map["table"], idx)

    if ("key" in cell) and ("value" in cell):
        return cell["key"] == key

    if ("first" in cell) and ("last" in cell) and ("size" in cell):
        bsize = lt.size(cell)
        j = 0
        while j < bsize:
            entry = lt.get_element(cell, j)
            if entry["key"] == key:
                return True
            j += 1
    return False


def get(my_map, key):
    if my_map["size"] == 0:
        return None
    idx = mf.hash_value(my_map, key)
    cell = lt.get_element(my_map["table"], idx)

    if ("key" in cell) and ("value" in cell):
        if cell["key"] == key:
            return me.get_value(cell)
        return None

    if ("first" in cell) and ("last" in cell) and ("size" in cell):
        bsize = lt.size(cell)
        j = 0
        while j < bsize:
            entry = lt.get_element(cell, j)
            if entry["key"] == key:
                return me.get_value(entry)
            j += 1
    return None


def remove(my_map, key):
    if my_map["size"] == 0:
        return my_map
    idx = mf.hash_value(my_map, key)
    table = my_map["table"]
    cell = lt.get_element(table, idx)

    if ("key" in cell) and ("value" in cell):
        if cell["key"] is not None and cell["key"] == key:
            lt.change_info(table, idx, me.new_map_entry(None, None))
            my_map["size"] -= 1
            my_map["current_factor"] = my_map["size"] / my_map["capacity"]
        return my_map

    if ("first" in cell) and ("last" in cell) and ("size" in cell):
        bucket = cell
        bsize = lt.size(bucket)
        j = 0
        pos = -1
        while j < bsize and pos == -1:
            entry = lt.get_element(bucket, j)
            if entry["key"] == key:
                pos = j
            else:
                j += 1
        if pos != -1:
            lt.delete_element(bucket, pos)
            my_map["size"] -= 1
            if lt.size(bucket) == 0:
                lt.change_info(table, idx, me.new_map_entry(None, None))
            my_map["current_factor"] = my_map["size"] / my_map["capacity"]
    return my_map


def size(my_map):
    return my_map["size"]


def is_empty(my_map):
    return my_map["size"] == 0


def key_set(my_map):
    keys = al.new_list()
    table = my_map["table"]
    tsize = lt.size(table)
    i = 0
    while i < tsize:
        cell = lt.get_element(table, i)
        if ("key" in cell) and ("value" in cell):
            if cell["key"] is not None:
                al.add_last(keys, cell["key"])
        elif ("first" in cell) and ("last" in cell) and ("size" in cell):
            bsize = lt.size(cell)
            j = 0
            while j < bsize:
                entry = lt.get_element(cell, j)
                if entry["key"] is not None:
                    al.add_last(keys, entry["key"])
                j += 1
        i += 1
    return keys


def value_set(my_map):
    values = al.new_list()
    table = my_map["table"]
    tsize = lt.size(table)
    i = 0
    while i < tsize:
        cell = lt.get_element(table, i)
        if ("key" in cell) and ("value" in cell):
            if cell["key"] is not None:
                al.add_last(values, cell["value"])
        elif ("first" in cell) and ("last" in cell) and ("size" in cell):
            bsize = lt.size(cell)
            j = 0
            while j < bsize:
                entry = lt.get_element(cell, j)
                if entry["key"] is not None:
                    al.add_last(values, entry["value"])
                j += 1
        i += 1
    return values


def rehash(my_map):
    old_table = my_map["table"]
    old_capacity = my_map["capacity"]
    new_capacity = mf.next_prime(2 * old_capacity)
    new_table = lt.new_list()
    i = 0
    while i < new_capacity:
        lt.add_last(new_table, me.new_map_entry(None, None))
        i += 1

    my_map["capacity"] = new_capacity
    my_map["prime"] = mf.next_prime(new_capacity + 1)
    my_map["scale"] = random.randrange(1, my_map["prime"] - 1)
    my_map["shift"] = random.randrange(0, my_map["prime"] - 1)
    my_map["table"] = new_table
    my_map["size"] = 0
    my_map["current_factor"] = 0

    t = lt.size(old_table)
    i = 0
    while i < t:
        cell = lt.get_element(old_table, i)
        if ("key" in cell) and ("value" in cell):
            if cell["key"] is not None:
                put(my_map, cell["key"], cell["value"])
        elif ("first" in cell) and ("last" in cell) and ("size" in cell):
            bsize = lt.size(cell)
            j = 0
            while j < bsize:
                entry = lt.get_element(cell, j)
                if entry["key"] is not None:
                    put(my_map, entry["key"], entry["value"])
                j += 1
        i += 1
    return my_map