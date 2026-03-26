import importlib
import sys
from typing import Any
from piply.core.step import Step


class PythonStep(Step):
    def _execute(self, context) -> Any:
        """
        Execute a Python function.

        The function can be specified as:
        - module.function_name (e.g., "my_module.my_function")
        - file_path:function_name (e.g., "/path/to/file.py:main")
        """
        func_spec = self.config.get("function")
        if not func_spec:
            raise ValueError(
                f"PythonStep '{self.name}' missing 'function' configuration")

        # Parse function specification
        if ":" in func_spec:
            # File path format: /path/to/file.py:function_name
            file_path, func_name = func_spec.split(":", 1)
            spec = importlib.util.spec_from_file_location(
                "step_module", file_path)
            if spec is None:
                raise ImportError(f"Cannot load module from {file_path}")
            module = importlib.util.module_from_spec(spec)
            sys.modules["step_module"] = module
            spec.loader.exec_module(module)
            func = getattr(module, func_name)
        else:
            # Module format: module.submodule.function
            module_name, func_name = func_spec.rsplit(".", 1)
            module = importlib.import_module(module_name)
            func = getattr(module, func_name)

        # Call the function with context
        # The function can accept context as parameter or no parameters
        try:
            import inspect
            sig = inspect.signature(func)
            if len(sig.parameters) > 0:
                return func(context)
            else:
                return func()
        except Exception as e:
            raise RuntimeError(f"Error executing function '{func_spec}': {e}")
