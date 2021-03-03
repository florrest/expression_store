Expression Store
================

This chapter gives an introduction on how to set up your environment for the Expression Store, as well as an guideline
on how to use the ``database_preprocessor.py`` script.

Features
********
* Import Metadata from ICGC DCC
* Import Metadata from SRA  (Sequence Read Archive)
* Import Metadata from GDC (Genomic Data Commons)
* Import results from `nf-core/rnaseq Version 1.4.2 <https://nf-co.re/rnaseq/1.4.2/usage>`_

Introduction
************
Expression Store is a PostgreSQL database, which can be used to query gene expression data from nf-core/rna-seq based
on metadata, which is retrieved from SRA, ICGC DCC and GDC.

Requirements
************
* Python Version 3.7.3
* Conda environment
* only tested in **Unix-Environment** for now

How to process data
*******************

Create a Conda environment based on **scripts/environment.yml** by running the following command in the scripts
directory:

.. code-block:: bash

   conda env create -f environment.yml

Activate the environment with:

.. code-block:: bash

   conda activate expression-store

Inside of this environment, you are now able to run the ``database_preprocessor.py`` script.

.. code-block:: bash

   database_preprocessor.py
   --rnaseq
   <Path to base directory of nf-core/rnaseq>
   --sra_file
   <Path to the SRA accession list text file>
   --dcc_manifest
   <Path to the DCC manifest tsv file>
   --gdc_manifest
   <Path to the GDC manifest tsv file>
   --metadata_dest
   <Path to the directory, where the metadata should be downloaded to>
   --db_dest
   <Path to the directory, where the exported csv files should be stored>

After the **successful download and processing** of the data, you will find the csv files in your ``db_dest``
directory. The script also gives you some feedback, which step of the database is performed at the moment.

How to create the database and load data
****************************************

Once you have set up your PostgreSQL database (for reference see `here <https://www.postgresql.org/download/>`_,
youe are able to create the database using the ``create_expression_store_db.sql`` script. This script can be found in the
directory ``scripts/postgresql_scripts/``.
|br|
By running the following command as the 'postgres' user:

.. code-block:: bash

   psql -f create_expression_store_db.sql

the tables for the database are created. |br|
In the same directory you will find a script called ``populate_tables.sql``. This script is created on the basis of
``populate_tables_tamplate.sql``. The above-mentioned Python-Script parses the ``db_dest`` directory of your input,
so you do not have to manipulate that file by yourself. In order to populate the database, simply run

.. code-block:: bash

   psql -f populate_tables.sql

Now you are ready to pose queries on the database as you like. One possibility is to start psql by simply typing:

.. code-block:: bash

   psql

and then run the following query:

.. code-block:: sql

   SELECT ge.gene_id, e.tpm, s.run
   FROM gene ge, expression e, sample s, sample_has_expression she
   WHERE s.body_site = 'Pancreas'
        AND she.id = s.id
        AND she.id = e.id
        AND e.gene_id = ge.gene_id
   GROUP BY s.run, ge.gene_id,e.tpm;

This query outputs all TPM values for a specific sample, and the according gene id.

[THIS DOC WILL BE UPDATED WITH THE DATABASE SCHEMA]

.. |br| raw:: html

   <br />

