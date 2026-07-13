# Feature Request: Runtime Recovery, UI Improvements, Documentation & Technical Architecture Guide

## Objective

Improve runtime reliability, scheduler state management, UI usability, and project maintainability.

Additionally create deep technical documentation that allows future developers to understand and modify the project safely.

---

# 1. Runtime State Recovery

## Current Problem

When a running pipeline is interrupted using Ctrl+C:

* Pipeline execution shold stop.
* Running tasks remain shows in RUNNING state.
* Running pipeline remains shows in RUNNING state.
* After restart, stale RUNNING records remain visible.
* Scheduler indicator remains green.

This causes state inconsistency.

---

## Requirements

### Graceful Shutdown

Implement proper shutdown handling.

When shutdown occurs:

* Stop scheduling new work.
* Stop accepting new executions.
* Persist final state.
* Mark active runs appropriately.

Recommended terminal state:

* INTERRUPTED

Alternative acceptable states:

* ABORTED
* CANCELLED

No task should remain permanently RUNNING.

---

### Startup Recovery

On application startup:

* Detect orphaned runs.
* Detect orphaned tasks.
* Reconcile state.
* Convert stale RUNNING records into terminal state.
* Restore system consistency.

---

### Scheduler Health

Scheduler status indicator must reflect actual scheduler state.

Requirements:

* Green only when scheduler is active.
* Red/gray when scheduler is inactive.
* Detect scheduler crashes.
* Detect scheduler shutdown.
* Automatically update UI.

---

### Testing

Add automated tests for:

* Ctrl+C interruption
* Unexpected process termination
* Scheduler crash
* Scheduler restart
* State recovery

---

# 2. Pipeline Details Page Improvements

Page:

Pipeline Details

Current issue:

Two metadata sections appear before DAG graph.

This reduces DAG visibility.

Requirements:

* Merge metadata sections into a single section.
* Reduce vertical space.
* Improve information density.
* Make DAG graph visible earlier.
* Preserve responsiveness.

---

# 3. Run Details Page Improvements

## Button Label

Current:

Rerun Run

Replace with:

Re-Run

Apply consistently across the application.

---

## Task Focus Section

Current issue:

Consumes excessive screen space.

Requirements:

* Make collapsible.
* Make expandable.
* Preserve state.
* Do not reload DAG data.
* Improve graph viewing area.
* Support smaller screens.

---

# 4. Documentation Updates

Update all existing documentation.

Include:

* YAML specification
* Runtime architecture
* Scheduler behavior
* Recovery mechanism
* UI documentation
* State lifecycle
* Retry lifecycle
* Task execution lifecycle
* Pipeline execution lifecycle

Include examples.

---

# 5. Create Deep Technical Architecture Documentation

Create a dedicated technical document intended for future maintainers and contributors.

Goal:

Allow someone unfamiliar with the project to quickly understand the complete architecture and safely perform major future changes.

Suggested file:

docs/architecture/technical_architecture.md

---

## Document Requirements

### High-Level Architecture

Explain:

* System overview
* Core concepts
* Execution flow

Include diagrams where useful.

---

### Project Structure

Document complete repository structure.

Example:

* backend
* frontend
* scheduler
* execution engine
* models
* services
* API layer
* UI layer
* persistence layer
* utilities

Explain purpose of every major module.

---

### Execution Architecture

Explain:

Pipeline
→ Run
→ Task
→ Executor

Include lifecycle details.

---

### Scheduler Architecture

Explain:

* scheduling process
* schedule registration
* trigger handling
* execution dispatching

---

### Runtime State Management

Explain:

* task states
* run states
* state transitions
* recovery process

---

### Database Architecture

Explain:

* schema overview
* tables
* relationships
* indexes

Include ER diagrams if possible.

---

### API Architecture

Explain:

* API layers
* endpoints
* request flow
* response flow

---

### Frontend Architecture

Explain:

* page structure
* components
* state management
* graph rendering

---

### Dependency Graph Architecture

Explain:

* DAG creation
* dependency resolution
* graph rendering

---

### Extensibility Guide

Explain how future developers can safely add:

* new task types
* new executors
* new sensors
* new schedulers
* new UI components
* new runtime states

---

### Development Guide

Document:

* local setup
* testing
* debugging
* build process
* deployment process

---

# Deliverables

* Runtime recovery implementation
* UI improvements
* Scheduler health improvements
* Automated tests
* Updated user documentation
* New deep technical architecture documentation
* Architecture diagrams where appropriate
