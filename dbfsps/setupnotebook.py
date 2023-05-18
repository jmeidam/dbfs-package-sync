_source = """
# Databricks notebook source
# COMMAND ----------

# MAGIC %pip install -r {package_path}/requirements.txt

# COMMAND ----------

import sys

print("Inserting {package_path} into system PATH")
sys.path.insert(0, "{package_path}")
print("Enabling autoreload")

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
"""


class SetupNotebook:
    def __init__(self, dbfs_package_path: str, notebook_path: str):
        self.notebook_path = notebook_path
        self.dbfs_path = dbfs_package_path
        self.source = _source.format(package_path=self.dbfs_path)

    def generate_notebook_file(self):
        with open(self.notebook_path, 'w') as f:
            f.write(self.source)

    def get_path(self) -> str:
        return self.notebook_path
