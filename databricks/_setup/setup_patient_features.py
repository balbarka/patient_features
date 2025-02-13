# Databricks notebook source
# DBTITLE 1,Install Packages
import subprocess

requirements = ['dbl-tempo', 'xgboost']

commands = [["pip", "install", r.strip()] for r in requirements]

for command in commands:
    try:
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        #subprocess.run(command, check=True)
    except subprocess.CalledProcessError as e:
        pass

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Define & Set Paths
import sys
from os import path

# Define absolute paths as python CONSTANTS
_nb_path_lst = ("/Workspace" + dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)).split('/')
PROJECT_ROOT_PATH = path.abspath('/'.join(_nb_path_lst[0:_nb_path_lst.index('databricks')]))
PYTHON_ROOT_PATH = path.join(PROJECT_ROOT_PATH, "python")
del _nb_path_lst

# Add PYTHON_ROOT_PATH to python path
# This is helpful when developing models that won't have code dependencies in mlflow experiment runs or using model-as-code
# however, if you will be deploying a custom whl file, you would not want to use relative path imports and instead should build
# and install the whl for local testing.
if PYTHON_ROOT_PATH not in sys.path:
    sys.path.insert(0, PYTHON_ROOT_PATH)
