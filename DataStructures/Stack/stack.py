from DataStructures.List import single_linked_list as sl

def new_stack():
    return sl.new_list()

def push(my_stack, element):
    return sl.add_first(my_stack, element)

def pop(my_stack):
    if sl.is_empty(my_stack):
        raise Exception("EmptyStructureError: stack is empty")
    return sl.remove_first(my_stack)

def is_empty(my_stack):
    return sl.is_empty(my_stack)

def top(my_stack):
    if sl.is_empty(my_stack):
        raise Exception("EmptyStructureError: stack is empty")
    return sl.first_element(my_stack)

def size(my_stack):
    return sl.size(my_stack)