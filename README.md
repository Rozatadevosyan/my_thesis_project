# Distributed Data Storage and Processing System

## Overview

This project presents a distributed data storage and processing system developed using **HDFS, Apache Spark, PostgreSQL, Dask, Docker, and Streamlit**.  
The main goal of the research is to optimize distributed data processing and compare partitioned vs non-partitioned datasets in order to evaluate performance improvements.

The system ensures efficient data storage, fast access, and scalable processing using modern Big Data technologies.

In addition, the project includes a **data-driven predictive model (Linear Regression)** used to assess patient health status based on biological indicators, demonstrating the integration of Artificial Intelligence into distributed systems.

---

## Objectives

- Optimize distributed data storage and processing
- Compare performance of partitioned and non-partitioned datasets
- Ensure data integrity and scalability
- Integrate Big Data tools with machine learning methods
- Build a user-friendly interface for data visualization

---

## Technologies Used

- Python
- Apache Spark (PySpark)
- HDFS (Hadoop Distributed File System)
- PostgreSQL
- Dask
- Docker
- Streamlit
- Scikit-learn (Linear Regression)

---

## System Architecture

The system consists of the following layers:

- **Data Storage Layer:** HDFS, PostgreSQL  
- **Processing Layer:** Apache Spark, Dask  
- **Application Layer:** Python services  
- **User Interface Layer:** Streamlit dashboard  
- **Machine Learning Layer:** Linear Regression model for prediction tasks  

---

##  How to Run the Project

### 1. Clone the repository
```bash
git clone git@github.com:Rozatadevosyan/my_thesis_project.git
cd my_thesis_project
create virtual environment
python3 -m venv .venv
source .venv/bin/activate
install dependencies
pip install -r requirements.txt
start sevices(Docker)
docker-compose up -d
This starts:
PostgreSQL
HDFS (if included)
Spark cluster (if configured)
Run data processing pipeline
python main.py
or
python spark_job.py
Run Streamlit application
streamlit run app.py
stop services
docker-compose down
Results
Improved performance using data partitioning
Efficient distributed processing with Spark and Dask
Scalable storage using HDFS
Machine learning-based prediction using Linear Regression
Research Contribution
This work combines theoretical and practical aspects of distributed systems and demonstrates how AI can be integrated into Big Data architectures.

