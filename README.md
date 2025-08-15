# CEBA_Process

Canada Emergency Business Account (CEBA) Process Automation in GCP

# CEBA_Process

**Automated CEBA Process — Google Cloud Platform**

This repository contains a simplified, demonstration version of a real-world GCP project.
It illustrates the structure, configurations, and code required to automate CEBA process using **Google Cloud Platform** services.

---

## Repository Structure

| File/Folder                 | Purpose                                                                                                                                                                                                    |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`ceba_automation_v1.py`** | Python scripts containing SQL queries for extracting data, python functions to transform data and eventually load processed data into **BigQuery** target tables.\_                                        |
| **`accp.yaml`**             | Pipeline configuration file defining build instructions for the container image, including image name, build context, and other ACCP pipeline parameters. <br>_Referenced during pipeline initialization._ |
| **`Dockerfile`**            | Image build specification defining the base image, system dependencies, and Python packages required to execute the scripts. <br>_Referenced in `accp.yaml`._                                              |
| **`requirements.txt`**      | List of Python dependencies to be installed in the image environment. <br>_Referenced in the `Dockerfile`._                                                                                                |
| **`ceba_dag.py`**           | **Apache Airflow DAG** for orchestrating and scheduling the end-to-end process. References the container image built from the above files and defines execution logic and scheduling parameters.           |

---

## Workflow Overview

1. **Code and Configuration** — Python scripts, configuration files, and dependencies are stored in this repository.
2. **Image Build** — The ACCP pipeline uses `accp.yaml`, `Dockerfile` and `requirement.txt` to build a container image with all required dependencies.
3. **Data Processing** — The container runs Python scripts to fetch, transform, and load data into BigQuery.
4. **Orchestration** — Airflow triggers the container execution according to the schedule defined in `ceba_dag.py`.

---

![CEBA Process Diagram](/CEBA_Process_Diagram.png)
