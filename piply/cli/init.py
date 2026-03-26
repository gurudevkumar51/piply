import os

def init_project():
    content = """
pipeline:
  name: demo_pipeline
  tenants:
    - practice1
    - practice2

  steps:
    - name: extract
      type: python
      function: extract_data

    - name: transform
      type: shell
      command: "echo Running dbt"
"""

    with open("piply.yaml", "w") as f:
        f.write(content.strip())

    print("piply.yaml created")