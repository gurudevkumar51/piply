from piply.plugins.registry import register_step

from piply.plugins.python.step import PythonStep
from piply.plugins.shell.step import ShellStep
from piply.plugins.dbt.step import DbtStep

# Register all steps here
register_step("python", PythonStep)
register_step("shell", ShellStep)
register_step("dbt", DbtStep)
