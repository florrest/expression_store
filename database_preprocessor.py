import click
import gc
import glob
import gzip
import logging
import os
import re
import sys
import time
import urllib.request
import xml.etree.ElementTree
from datetime import date, datetime
from io import StringIO

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pyranges as pr

# Create logger
logger = logging.getLogger('Expression Store Script Logger')
# Create console handler
ch = logging.StreamHandler()
# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)
logger.setLevel(logging.INFO)


@click.command()
@click.option('-i', '--inpath', prompt='path to nf-core/rna-seq base folder', help='input path', required=True)
def main(inpath):
    logger.info('Filter Projects and download metadata from ICGC DCC')
    icgc_project_codes = get_icgc_project_list(inpath)
    # download_icgc_project_metadata(icgc_project_codes, inpath)

    logger.info('Download Metadata from Sequence Read Archive (SRA)')
    sra_accessions = get_sra_accession_list(inpath)
    download_sra_metadata(sra_accessions, inpath)


def get_icgc_project_list(inpath):
    icgc_project_codes = pd.DataFrame()
    try:
        os.chdir(inpath)
        print(os.getcwd())
        icgc_project_codes = pd.read_csv('./manifest.tsv', sep='\t', usecols=['project_id/project_count'])
        icgc_project_codes = icgc_project_codes['project_id/project_count'].unique().tolist()
    except:
        print("Not a dirctory")

    return icgc_project_codes


def get_sra_accession_list(inpath):
    sra_accessions = pd.DataFrame()
    try:
        os.chdir(inpath)
        print(os.getcwd())
        sra_accessions = pd.read_csv('./acc_list.txt', header=None)
        sra_accessions = sra_accessions[0].unique().tolist()
    except:
        print("Not a dirctory")

    return sra_accessions


def download_sra_metadata(project_lst, inpath):
    for i in project_lst:
        sra_url = 'http://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?save=efetch&db=sra&rettype=runinfo&term=' + str(i)
        urllib.request.urlretrieve(sra_url, inpath + '/' + str(i) + '.csv')


def download_icgc_project_metadata(project_lst, inpath):
    for i in project_lst:
        sample_url = 'https://dcc.icgc.org/api/v1/download?fn=/current/Projects/' + i + '/sample' \
                                                                                        '.' + i + '.tsv.gz '
        donor_url = 'https://dcc.icgc.org/api/v1/download?fn=/current/Projects/' + i + '/donor' \
                                                                                       '.' + i + '.tsv.gz '
        urllib.request.urlretrieve(sample_url, inpath + '/' + str(i) + '_sample.tsv.gz')
        urllib.request.urlretrieve(donor_url, inpath + '/' + str(i) + '_donor.tsv.gz')


if __name__ == "__main__":
    sys.exit(main())
