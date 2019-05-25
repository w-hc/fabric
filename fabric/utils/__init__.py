import importlib


def dynamic_load_py_object(
    package_name, module_name, obj_name=None
):
    '''Dynamically import an object.
    Assumes that the object lives at module_name.py:obj_name
    If obj_name is not given, assume it shares the same name as the module.
    obj_name is case insensitive e.g. kitti.py/KiTTI is valid
    '''
    if obj_name is None:
        obj_name = module_name
    # use relative import syntax .targt_name
    target_module = importlib.import_module(
        '.{}'.format(module_name), package=package_name
    )
    target_obj = None
    for name, cls in target_module.__dict__.items():
        if name.lower() == obj_name.lower():
            target_obj = cls

    if target_obj is None:
        raise ValueError(
            "No object in {}.{}.py whose lower-case name matches {}".format(
                package_name, module_name, obj_name)
        )

    return target_obj
