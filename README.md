# Hadoop_mastodon_project

# Data Processing and Analysis Project

## Overview

This project is designed to automate the collection, processing, and analysis of data from the Mastodon social media platform using Hadoop MapReduce, HBase, and Apache Airflow. The pipeline extracts information from Mastodon, processes it with MapReduce, stores the results in HBase, and orchestrates the entire workflow with Apache Airflow.

## Project points

- [Installation](#installation)
- [Data Extraction](#data-extraction)
- [MapReduce Processing](#mapreduce-processing)
- [HBase Storage](#hbase-storage)
- [Airflow](#Airflow)
- [License](#license)

## Installation

To set up the project, follow these steps:

- Install Hadoop : https://learnubuntu.com/install-hadoop/
- Install HBase : https://www.linkedin.com/pulse/how-install-apache-hbase-ubuntu-dr-virendra-kumar-shrivastava
- Install Airflow : https://hevodata.com/learn/install-airflow/
  
```bash
pip install -r requirements.txt
```

## Data Extraction From Mastadon 

```bash
python3 data_extraction.py
```

## MapReduce Processing

```bash
python3 mapreduce.py
```

## HBase Storage

```bash
python3 insert_hbase.py
```
## Airflow

```bash
python3 mastodon_project.py
```
## GDPR
Our project places a high priority on ensuring the privacy and GDPR compliance of Mastodon users. We only gather and handle personal data with explicit consent or when legally required, strictly for defined purposes. Robust security measures safeguard the data, and users retain the rights to access, correct, delete, or restrict processing. Our data retention practices adhere to legal standards, and our transparent privacy policy provides comprehensive details on all aspects.

## License
This project is licensed under the MIT License.




