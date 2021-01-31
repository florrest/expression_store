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

and run **database_preprocessor.py** 

```
database_preprocessor.py -i <PATH TO NF-CORE/RNA-SEQ BASE DIRECTORY>
```

**!!!!!!!!**

Accession List for SRA and Manifest files of GDC and ICGC DCC are mandatory at the moment.