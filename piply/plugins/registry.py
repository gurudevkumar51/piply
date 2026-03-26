STEP_REGISTRY = {}

def register_step(name, cls):
    STEP_REGISTRY[name] = cls

def get_step(name):
    return STEP_REGISTRY[name]