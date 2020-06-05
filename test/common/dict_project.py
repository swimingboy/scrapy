from typing import Union, Dict, Any, Callable
from inspect import isfunction, isclass


class ChangeName:
    name = ""

    def __init__(self, name):
        self.name = name


def dict_project(data: Dict[str, Any],
                 map_rules: Union[
                     Dict[Any, Union[ChangeName, Callable[[Any], Any], Any]],
                     list, tuple, None
                 ] = None) -> dict:
    """
    python dict project
    """
    if not map_rules:
        map_rules = {key: True for key in data.keys()}
    elif isinstance(map_rules, (list, tuple)):
        map_rules = {key: True for key in map_rules}

    if isinstance(data, dict):
        data = dict({
            value.name if isinstance(value, ChangeName) else key:
                value(data[key]) if isfunction(value) or isclass(value) else data[key]
            for key, value in map_rules.items()
            if key in data and value
        })
    else:
        raise ValueError(f'can not process: {data}')
    return data


def test_dict_project():
    test_dict = {
        'a': 1,
        'b': 2,
        'c': 3,
        'd': 4,
    }
    assert dict_project(test_dict) == test_dict
    assert dict_project(test_dict, {}) == test_dict
    assert dict_project(test_dict, ('a',)) == {'a': 1}
    assert dict_project(test_dict, ('a', 'b')) == {'a': 1, 'b': 2}
    assert dict_project(test_dict, ['a', 'b']) == {'a': 1, 'b': 2}

    assert dict_project(test_dict, {'a': 0, 'c': 1}) == {'c': 3}
    assert dict_project(test_dict, {'a': lambda x: x * 2, 'c': 1}) == {'a': 2, 'c': 3}
    assert dict_project(test_dict, {'a': str, 'c': 1}) == {'a': '1', 'c': 3}
    assert dict_project(test_dict, {'a': type, 'c': 1}) == {'a': int, 'c': 3}

    assert dict_project(test_dict, {'f': 0, 'c': 1}) == {'c': 3}
    assert dict_project(test_dict, {'a': ChangeName('e'), 'c': 1}) == {'e': 1, 'c': 3}
    assert dict_project(test_dict, {'a': ChangeName('c'), 'c': 1}) == {'c': 3}
    assert dict_project(test_dict, {'f': ChangeName('a'), 'c': 1}) == {'c': 3}
