import DataStructures.Map.map_functions as mf
import DataStructures.List.array_list as lt
import DataStructures.Map.map_entry as me
import random


def new_map(num_elements, load_factor, prime=109345121):
    capacity = mf.next_prime(int(num_elements / load_factor))

    table = lt.new_list()
    for _ in range(capacity):
        lt.add_last(table, me.new_map_entry(None, None))

    hash_table = {
        "prime" : prime,
        "capacity" : capacity,
        "scale" : mf.randrange(1, prime - 1),
        "shift" : mf.randrange(0, prime - 1),
        "table" : table,
        "current_factor" : 0,
        "limit_factor" : load_factor,
        "size" : 0
    }

    return hash_table

def put(my_map, key, value):
    start = mf.hash_value(my_map, key)
    occupied, pos = find_slot(my_map, key, start)
    if pos == -1:
        return my_map
    table = my_map["table"]
    lt.change_info(table, pos, me.new_map_entry(key, value))
    if not occupied:
        my_map["size"] += 1
    return my_map

def contains(my_map, key):
    index = mf.hash_value(my_map, key)
    capacity = my_map["capacity"]
    table = my_map["table"]

    for i in range(capacity):
        probe_index = (index + i) % capacity
        entry = lt.get_element(table, probe_index)
        entry_key = me.get_key(entry)
        if entry_key is None:
            return False
        if entry_key == key:
            return True
    return False

def find_slot(my_map,key,hash_value):
    start = int(hash_value)
    capacity = my_map["capacity"]
    table = my_map["table"]

    i = 0
    while i < capacity:
        pos = (start + i) % capacity
        entry = lt.get_element(table, pos)
        k = me.get_key(entry)
        if k is None:
            return False, pos
        if k == key:
            return True, pos
        i += 1
    return False, -1

def random():
    return _rnd.random()

def randrange(start, stop=None):
    if stop is None:
        return _rnd.randrange(start)
    return _rnd.randrange(start, stop)

def randint(a, b):
    return _rnd.randint(a, b)

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
    my_map["scale"] = mf.randrange(1, my_map["prime"] - 1)
    my_map["shift"] = mf.randrange(0, my_map["prime"] - 1)
    my_map["table"] = new_table
    my_map["size"] = 0

    i = 0
    m = lt.size(old_table)
    while i < m:
        entry = lt.get_element(old_table, i)
        k = me.get_key(entry)
        if k is not None:
            put(my_map, k, me.get_value(entry))
        i += 1

    my_map["current_factor"] = my_map["size"] / my_map["capacity"]
    return my_map
            
def get(my_map,key):
    start = mf.hash_value(my_map, key)
    occupied, pos = find_slot(my_map, key, start)
    if occupied and pos != -1:
        entry = lt.get_element(my_map["table"], pos)
        return me.get_value(entry)
    return None
    
    
    
def remove(my_map,key):
    start = mf.hash_value(my_map, key)
    occupied, pos = find_slot(my_map, key, start)
    if occupied and pos != -1:
        lt.change_info(my_map["table"], pos, me.new_map_entry(None, None))
        my_map["size"] -= 1
        return True
    return False
    
def size(my_map):
    return my_map['size']

def key_set(my_map):

    keys = lt.new_list()
    table = my_map["table"]
    m = lt.size(table)
    i = 0
    while i < m:
        entry = lt.get_element(table, i)
        k = me.get_key(entry)
        if k is not None:
            lt.add_last(keys, k)
        i += 1
    return keys

def is_empty(my_map):
    return my_map["size"] == 0

def value_set(my_map):
    values = lt.new_list()
    table = my_map["table"]
    m = lt.size(table)
    i = 0
    while i < m:
        entry = lt.get_element(table, i)
        if me.get_key(entry) is not None:
            lt.add_last(values, me.get_value(entry))
        i += 1
    return values

def is_available(table, pos):
    entry = lt.get_element(table, pos)
    return me.get_key(entry) is None


