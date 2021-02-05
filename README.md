# Expression Store

### Features

* Import Metadata from ICGC DCC
* Import Metadata from SRA  (Sequence Read Archive)
* Import Metadata from GDC (UNDER CONSTRUCTION)
* Based on output of [nf-core/rna-seq Version 1.4.2](https://github.com/nf-core/rnaseq/tree/1.4.2)



## Introduction

**Expression Store** is a PostgreSQL database which can be used to query gene expression data from **nf-core/rna-seq** based on metadata, which is retrieved from SRA, ICGC DCC and GDC

## Quick Start

Create a Conda environment based on **environment.yml** by running

```
conda env create -f environment.yml
```

from the base directory of this repository.

Activate the environment with

```
conda activate expression-store
```



## Running database_preprocessor.py

### Script Parameters

- Provide Path to output directory of nf-core/rnaseq

```--rnaseq <absolute path to nf-core/rnaseq base folder>``` 

- Provide Path to SRA Accession file, which is used to download SRR files using the nf-core pipeline qbic-pipelines/sradownloader

```--sra_file <absolute path do accession list (*.txt)>```

- Provide Path to ICGC DCC manifest file, which is used to download sequences using the score-client

```--dcc_manifest <absolute path to manifest files```

- Provide Path to your desired metadata destination. This directory will be used to store the downloaded metadata

```--metadata_dest <absolute path to destination directory of the downloaded metadata>```

- Provide Path to your desired database directory, where the csv-files will be stored. These can be loaded into the database later.

```--db_dest <absolute path to your desired output directory>```



## Setting Up PostgreSQL Server

### Requirements

- CentOS Linux release 7.6.1810 (Core)
- Conda

### Installing PostgreSQL Server on CentOS

1. Install PostgreSQL on CentOS 7

```
sudo yum install postgresql-server postgresql-contrib
```

2. Initialize the Database

```
sudo postgresql-setup initdb
```

3. Start and Enable PostgreSQL

```
sudo systemctl start postgresql ; sudo systemctl enable postgresql
```

## Basic Setup for PostgreSQL

Change password of automatically generated "postgres" user (in our usecase I set it to **expression**)

```
sudo passwd postgres
```

