"""
Unit tests for Piply core components.
"""

import pytest
import tempfile
import os
from pathlib import Path

from piply.core.context import PipelineContext, StepOutput
from piply.core.step import Step
from piply.core.pipeline import Pipeline
from piply.core.models import PipelineConfig, StepConfig, TenantConfig
from piply.parser.yaml_parser import load_pipeline
from piply.engine.local_engine import LocalEngine


class TestPipelineContext:
    """Tests for PipelineContext."""

    def test_create_context(self):
        ctx = PipelineContext(pipeline_name="test")
        assert ctx.pipeline_name == "test"
        assert ctx.tenant is None
        assert len(ctx.step_outputs) == 0

    def test_set_and_get_output(self):
        ctx = PipelineContext()
        output = StepOutput(step_name="test", success=True,
                            result={"key": "value"})
        ctx.set_output("test", output)
        retrieved = ctx.get_output("test")
        assert retrieved == output
        assert retrieved.success is True

    def test_set_and_get_variable(self):
        ctx = PipelineContext()
        ctx.set_variable("key", "value")
        assert ctx.get_variable("key") == "value"
        assert ctx.get_variable("missing", "default") == "default"


class TestStep:
    """Tests for Step base class."""

    def test_step_initialization(self):
        config = {"name": "test_step", "retries": 2, "retry_delay": 30}
        step = Step("test_step", config)
        assert step.name == "test_step"
        assert step.retries == 2
        assert step.retry_delay == 30
        assert step.depends_on == []

    def test_step_retry_logic(self):
        """Test that step retries on failure."""
        class FailingStep(Step):
            attempt_count = 0

            def _execute(self, context):
                FailingStep.attempt_count += 1
                if FailingStep.attempt_count < 3:
                    raise ValueError("Not yet")
                return "success"

        config = {"name": "failing", "retries": 2, "retry_delay": 0}
        step = FailingStep("failing", config)
        ctx = PipelineContext()
        output = step.run(ctx)

        assert output.success is True
        assert FailingStep.attempt_count == 3

    def test_step_exhausts_retries(self):
        """Test that step fails after all retries."""
        class AlwaysFailingStep(Step):
            attempt_count = 0

            def _execute(self, context):
                AlwaysFailingStep.attempt_count += 1
                raise ValueError("Always fails")

        config = {"name": "always_fail", "retries": 2, "retry_delay": 0}
        step = AlwaysFailingStep("always_fail", config)
        ctx = PipelineContext()
        output = step.run(ctx)

        assert output.success is False
        assert AlwaysFailingStep.attempt_count == 3  # initial + 2 retries


class TestPipeline:
    """Tests for Pipeline."""

    def test_pipeline_creation(self):
        steps = [Step("step1", {}), Step("step2", {})]
        pipeline = Pipeline("test", steps, tenants=["t1", "t2"])
        assert pipeline.name == "test"
        assert len(pipeline.steps) == 2
        assert pipeline.tenants == ["t1", "t2"]

    def test_pipeline_single_run(self):
        """Test pipeline execution without tenants."""
        class SuccessStep(Step):
            def _execute(self, context):
                return "done"

        steps = [SuccessStep("s1", {}), SuccessStep("s2", {})]
        pipeline = Pipeline("test", steps)
        engine = LocalEngine()

        # Should run without errors
        pipeline.run(engine)

    def test_pipeline_multi_tenant(self):
        """Test multi-tenant execution."""
        class TrackTenantStep(Step):
            tenants_seen = []

            def _execute(self, context):
                TrackTenantStep.tenants_seen.append(context.tenant)
                return f"done for {context.tenant}"

        steps = [TrackTenantStep("s1", {})]
        pipeline = Pipeline("test", steps, tenants=["t1", "t2"])
        engine = LocalEngine()
        pipeline.run(engine)

        assert TrackTenantStep.tenants_seen == ["t1", "t2"]


class TestYAMLParser:
    """Tests for YAML parser."""

    def test_load_valid_pipeline(self):
        """Test loading a valid YAML pipeline."""
        yaml_content = """
pipeline:
  name: test_pipeline
  steps:
    - name: step1
      type: python
      function: "test_module.func"
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            f.flush()

            pipeline = load_pipeline(f.name)
            assert pipeline.name == "test_pipeline"
            assert len(pipeline.steps) == 1
            assert pipeline.steps[0].name == "step1"

        os.unlink(f.name)

    def test_load_pipeline_with_tenants(self):
        """Test loading pipeline with tenants."""
        yaml_content = """
pipeline:
  name: multi_tenant_pipeline
  tenants:
    - tenant1
    - tenant2
  steps:
    - name: step1
      type: shell
      command: "echo hello"
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            f.flush()

            pipeline = load_pipeline(f.name)
            assert pipeline.tenants == ["tenant1", "tenant2"]

        os.unlink(f.name)

    def test_load_pipeline_with_dependencies(self):
        """Test loading pipeline with step dependencies."""
        yaml_content = """
pipeline:
  name: dag_pipeline
  steps:
    - name: step1
      type: python
      function: "module.func1"
    - name: step2
      type: shell
      command: "echo step2"
      depends_on:
        - step1
    - name: step3
      type: python
      function: "module.func3"
      depends_on:
        - step1
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(yaml_content)
            f.flush()

            pipeline = load_pipeline(f.name)
            step2 = next(s for s in pipeline.steps if s.name == "step2")
            step3 = next(s for s in pipeline.steps if s.name == "step3")

            assert step2.depends_on == ["step1"]
            assert step3.depends_on == ["step1"]

        os.unlink(f.name)


class TestLocalEngine:
    """Tests for LocalEngine."""

    def test_engine_executes_steps(self):
        """Test that engine executes all steps."""
        class TrackExecution(Step):
            executed = []

            def _execute(self, context):
                TrackExecution.executed.append(self.name)
                return "done"

        steps = [TrackExecution("s1", {}), TrackExecution("s2", {})]
        pipeline = Pipeline("test", steps)
        engine = LocalEngine()
        pipeline.run(engine)

        assert set(TrackExecution.executed) == {"s1", "s2"}

    def test_engine_respects_dependencies(self):
        """Test that engine respects step dependencies."""
        execution_order = []

        class OrderStep(Step):
            def __init__(self, name, config, order_num):
                super().__init__(name, config)
                self.order_num = order_num

            def _execute(self, context):
                execution_order.append(self.order_num)
                return "done"

        steps = [
            OrderStep("s1", {}, 1),
            OrderStep("s2", {"depends_on": ["s1"]}, 2),
            OrderStep("s3", {"depends_on": ["s2"]}, 3),
        ]
        pipeline = Pipeline("test", steps)
        engine = LocalEngine()
        pipeline.run(engine)

        assert execution_order == [1, 2, 3]

    def test_engine_fails_on_step_error(self):
        """Test that engine raises error when step fails."""
        class FailingStep(Step):
            def _execute(self, context):
                raise ValueError("Step failed")

        steps = [FailingStep("fail", {})]
        pipeline = Pipeline("test", steps)
        engine = LocalEngine()

        with pytest.raises(RuntimeError, match="Step 'fail' failed"):
            pipeline.run(engine)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
