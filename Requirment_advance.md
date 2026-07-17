# Feature Request: Pipeline Templates & Deployments Architecture

## Objective

Introduce a scalable pipeline deployment architecture while preserving Piply's current simplicity and backward compatibility.

The framework must support both:

1. Simple Mode (default)
2. Advanced Mode (optional)

The common/simple use case should remain extremely easy and require minimal YAML.

---

# Design Principle

The framework should follow:

"Simple things should remain simple. Advanced features should be optional."

Users should not be forced to learn concepts like deployments, tenants, environments, or template inheritance unless they actually need them.

---

# Simple Mode (Default)

The current YAML structure must continue working without modification.

Example:

```yaml
pipelines:

  extract_flow:

    schedule:
      every: 15m

    tasks:

      extract:
        type: python
        path: pipelines/extract.py
        function: extract_data

      transform:
        type: python
        path: pipelines/extract.py
        function: transform_data
        depends_on: [extract]
```

Requirements:

* Existing YAML remains valid.
* Existing functionality remains unchanged.
* Existing scheduling remains unchanged.
* Existing UI behavior remains unchanged.
* Existing APIs remain unchanged.
* Existing users should not be required to understand deployments.

Internally Piply may convert this into:

* Pipeline Template
* Default Deployment

but this should be completely transparent to users.

Example internal representation:

extract_flow
└── extract_flow.default

Users should not be required to define this manually.

---

# Advanced Mode (Optional)

Introduce optional support for:

* Pipeline Templates
* Pipeline Deployments

Example:

```yaml
pipeline_templates:

  report_pipeline:

    tasks:

      extract:
        type: python
        path: pipelines/extract.py
        function: extract_data

      transform:
        type: python
        path: pipelines/extract.py
        function: transform_data
        depends_on: [extract]

pipeline_deployments:

  client_a_reporting:

    template: report_pipeline

    schedule:
      cron: "*/15 * * * *"

    variables:
      tenant: client_a

  client_b_reporting:

    template: report_pipeline

    schedule:
      cron: "0 * * * *"

    variables:
      tenant: client_b
```

---

# Architecture Requirements

Introduce clear separation between:

Pipeline Template
↓
Pipeline Deployment
↓
Pipeline Run
↓
Task Execution

Definitions:

Pipeline Template:

* Workflow definition
* Tasks
* Dependencies
* Retry policies
* Runtime behavior

Pipeline Deployment:

* Schedule
* Variables
* Tenant configuration
* Environment configuration
* Enable/disable state

Pipeline Run:

* Individual execution instance

---

# Scheduler Behavior

Scheduler must schedule deployments, not templates.

Examples:

client_a_reporting
client_b_reporting

should appear as independently schedulable entities.

Each deployment may have:

* different schedule
* different variables
* different notifications
* different retry policies
* different execution settings

---

# Dynamic Task Expansion Compatibility

Existing task expansion should continue working.

Example:

```yaml
entities:

  report:
    - payment
    - adjustment
    - refund
```

Runtime:

payment.extract
payment.transform

adjustment.extract
adjustment.transform

refund.extract
refund.transform

Deployment architecture must work seamlessly with dynamic task expansion.

---

# Future Extensibility

Design architecture to support future capabilities without major refactoring:

* Multi-tenant deployments
* Deployment inheritance
* Environment-specific deployments
* Matrix expansion
* Deployment groups
* Dynamic deployment generation
* Deployment-level secrets
* Deployment-level notifications
* Timezone-specific scheduling

Implementation of these future features is NOT required now.

Architecture should simply not block them.

---

# Deliverables

* Full implementation
* Backward compatibility validation
* Migration strategy
* Updated YAML specification
* Automated tests
* Updated UI support
* Updated scheduler logic
* Architecture documentation
