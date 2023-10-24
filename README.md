# hadoop_mastodon_project

# Data Processing and Analysis Project

## Overview

This project is designed to automate the collection, processing, and analysis of data from the Mastodon social media platform using Hadoop MapReduce, HBase, and Apache Airflow. The pipeline extracts information from Mastodon, processes it with MapReduce, stores the results in HBase, and orchestrates the entire workflow with Apache Airflow.

## Project points

- [Installation](#installation)
- [Usage](#usage)
- [Data Extraction](#data-extraction)
- [MapReduce Processing](#mapreduce-processing)
- [HBase Storage](#hbase-storage)
- [License](#license)

## Installation

To set up the project, follow these steps:

```bash
pip install -r requirements.txt
```

## Usage

```bash
python data_extraction.py
```

## MapReduce Processing

```bash
python mapreduce.py
```
## insert to HBase

```bash
python insert_hhbase.py
```

## License
This project is licensed under the MIT License.




