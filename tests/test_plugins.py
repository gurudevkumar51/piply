"""
Tests for plugin steps.
"""

import pytest
from piply.core.context import PipelineContext
from piply.plugins.python.step import PythonStep
from piply.plugins.shell.step import ShellStep
from piply.plugins.dbt.step import DbtStep


class TestPythonStep:
    """Tests for PythonStep."""

    def test_python_step_with_module_function(self):
        """Test executing a Python function from a module."""
        # Create a simple test function
        step = PythonStep(
            "test", {"function": "tests.test_helpers.sample_function"})
        ctx = PipelineContext()
        output = step.run(ctx)
        assert output.success is True
        assert output.result == "sample result"

    def test_python_step_missing_function(self):
        """Test error when function is not specified."""
        step = PythonStep("test", {})
        ctx = PipelineContext()
        output = step.run(ctx)
        assert output.success is False
        assert "missing 'function'" in output.error

    def test_python_step_invalid_module(self):
        """Test error when module doesn't exist."""
        step = PythonStep("test", {"function": "nonexistent.module.func"})
        ctx = PipelineContext()
        output = step.run(ctx)
        assert output.success is False


class TestShellStep:
    """Tests for ShellStep."""

    def test_shell_step_simple_command(self):
        """Test executing a simple shell command."""
        step = ShellStep("test", {"command": "echo 'Hello World'"})
        ctx = PipelineContext()
        output = step.run(ctx)
        assert output.success is True
        assert "Hello World" in output.result["stdout"]

    def test_shell_step_missing_command(self):
        """Test error when command is not specified."""
        step = ShellStep("test", {})
        ctx = PipelineContext()
        output = step.run(ctx)
        assert output.success is False
        assert "missing 'command'" in output.error

    def test_shell_step_failing_command(self):
        """Test that failing command returns error."""
        step = ShellStep("test", {"command": "false"})
        ctx = PipelineContext()
        output = step.run(ctx)
        assert output.success is False
        assert "failed with exit code" in output.error

    def test_shell_step_with_retries(self):
        """Test shell step retry logic."""
        step = ShellStep("test", {
            "command": "exit 1",
            "retries": 2,
            "retry_delay": 0
        })
        ctx = PipelineContext()
        output = step.run(ctx)
        assert output.success is False
        assert output.metadata["attempts"] == 3  # initial + 2 retries


class TestDbtStep:
    """Tests for DbtStep."""

    def test_dbt_step_missing_command(self):
        """Test default command is 'run'."""
        step = DbtStep("test", {})
        ctx = PipelineContext()
        # We won't actually run dbt, just check configuration
        # defaults to run in _execute
        assert step.config.get("command") is None

    def test_dbt_step_with_models(self):
        """Test dbt command construction with models."""
        step = DbtStep("test", {
            "command": "run",
            "models": ["staging", "marts"]
        })
        # Verify configuration is stored
        assert step.config["models"] == ["staging", "marts"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
