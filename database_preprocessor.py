import click
import gc
import glob
import gzip
import json
import logging
import os
import re
import requests
import shutil
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
    #icgc_project_codes = get_icgc_project_list(inpath)
    #download_icgc_project_metadata(icgc_project_codes, inpath)

    logger.info('Download Metadata from Sequence Read Archive (SRA)')
    # sra_accessions = get_sra_accession_list(inpath)
    # download_sra_metadata(sra_accessions, inpath)

    logger.info('Download Metadata from GDC')
    # gdc_uuid_list = get_gdc_uuid_list(inpath)
    # download_gdc_metadata(gdc_uuid_list, inpath)

    # test()
    # test = concat_sra_csv(os.path.join(inpath, "metadata/sra"), sra_accessions)
    #gunzip_files(os.path.join(inpath, "metadata/icgc"))
    join_icgc_meta(concat_icgc_meta(os.path.join(inpath, "metadata/icgc")))


# Downloading metadata

def get_icgc_project_list(inpath):
    icgc_project_codes = pd.DataFrame()
    try:
        os.chdir(inpath)
        icgc_project_codes = pd.read_csv('./manifest.tsv', sep='\t', usecols=['project_id/project_count'])
        icgc_project_codes = icgc_project_codes['project_id/project_count'].unique().tolist()
    except:
        print("Not a directory")

    return icgc_project_codes


def get_sra_accession_list(inpath):
    sra_accessions = pd.DataFrame()
    try:
        os.chdir(inpath)
        sra_accessions = pd.read_csv('./acc_list.txt', header=None)
        sra_accessions = sra_accessions[0].unique().tolist()
    except:
        print("Not a dirctory")

    return sra_accessions


def get_gdc_uuid_list(inpath):
    uuids = pd.DataFrame()
    gdc_files = []
    try:
        os.chdir(inpath)
        for i in os.listdir(inpath):
            if os.path.isfile(os.path.join(inpath, i)) and "gdc_manifest" in i:
                gdc_files.append(i)

        uuids = pd.concat((pd.read_csv(txt, sep='\t') for txt in gdc_files))
        uuids = uuids['id'].unique().tolist()
    except:
        print("Not a dirctory")

    return uuids


def retrieve_data(url, destination):
    urllib.request.urlretrieve(url, destination)


def download_sra_metadata(project_lst, inpath):
    if not os.path.exists(os.path.join(inpath, 'metadata/sra/')):
        os.makedirs(os.path.join(inpath, 'metadata/sra/'))
    for i in project_lst:
        sra_url = 'http://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?save=efetch&db=sra&rettype=runinfo&term={}'.format(
            i)
        retrieve_data(sra_url, os.path.join(inpath, 'metadata/sra/') + str(i) + '.csv')


def download_icgc_project_metadata(project_lst, inpath):
    if not os.path.exists(os.path.join(inpath, 'metadata/icgc/')):
        os.makedirs(os.path.join(inpath, 'metadata/icgc/'))
    for i in project_lst:
        sample_url = 'https://dcc.icgc.org/api/v1/download?fn=/current/Projects/' + i + '/sample' \
                                                                                        '.' + i + '.tsv.gz '
        donor_url = 'https://dcc.icgc.org/api/v1/download?fn=/current/Projects/' + i + '/donor' \
                                                                                       '.' + i + '.tsv.gz '
        specimen_url = 'https://dcc.icgc.org/api/v1/download?fn=/current/Projects/' + i + '/specimen' \
                                                                                          '.' + i + '.tsv.gz '
        donor_set_url = 'https://dcc.icgc.org/api/v1/repository/files/export?' \
                        'filters=%7B%22file%22%3A%7B%22projectCode%22%3A%7B%22is%22%3A%5B%22{}%22%5D%7D%7D%7D&' \
                        'type=tsv'.format(i)
        retrieve_data(sample_url, inpath + '/metadata/icgc/sample_' + str(i) + '.tsv.gz')
        retrieve_data(donor_url, inpath + '/metadata/icgc/donor_' + str(i) + '.tsv.gz')
        retrieve_data(specimen_url, inpath + '/metadata/icgc/specimen_' + str(i) + '.tsv.gz')
        retrieve_data(donor_set_url, inpath + '/metadata/icgc/repository_' + str(i) + '.tsv')


def download_gdc_metadata(id_lst, inpath):
    if not os.path.exists(os.path.join(inpath, 'metadata/gdc/')):
        os.makedirs(os.path.join(inpath, 'metadata/gdc/'))
    for i in id_lst:
        url = 'https://api.gdc.cancer.gov/cases/' + str(i) + '?fields=submitter_id?pretty=true&format=TSV'
        retrieve_data(url, inpath + '/test/' + str(i) + '_sample.tsv')


def gunzip_files(inpath):
    gz_files = glob.glob(inpath + "/*.gz")
    for gz in gz_files:
        with gzip.open(gz, 'rb') as f_in:
            with open(gz[:-3], 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(gz)


# Preparing data

def concat_sra_csv(inpath_sra_meta_csv, used_accessions):
    sra_csv_files = glob.glob(inpath_sra_meta_csv + "/*.csv")
    sra_files_list = []
    for file in sra_csv_files:
        df = pd.read_csv(file, sep=',')
        sra_files_list.append(df)
    frame = pd.concat(sra_files_list, axis=0)
    frame = frame[frame['Run'].isin(used_accessions)]
    return frame


def concat_icgc_meta(inpath_icgc_meta):
    icgc_tsv_files = glob.glob(inpath_icgc_meta + "/*.tsv")
    icgc_donor = []
    icgc_sample = []
    icgc_specimen = []
    icgc_repository = []
    for file in icgc_tsv_files:
        if "donor" in file:
            df = pd.read_csv(file, sep='\t')
            icgc_donor.append(df)
        elif "sample" in file:
            df = pd.read_csv(file, sep='\t')
            icgc_sample.append(df)
        elif "specimen" in file:
            df = pd.read_csv(file, sep='\t')
            icgc_specimen.append(df)
        elif "repository" in file:
            df = pd.read_csv(file, sep='\t')
            icgc_repository.append(df)
    donor_frame = pd.concat(icgc_donor, axis=0)
    sample_frame = pd.concat(icgc_sample, axis=0)
    specimen_frame = pd.concat(icgc_specimen, axis=0)
    repository_frame = pd.concat(icgc_repository, axis=0)

    return donor_frame, sample_frame, specimen_frame, repository_frame


# Data Cleaning
def join_icgc_meta(df):
    donor_df = df[0]
    sample_df = df[1]
    specimen_df = df[2]
    repository_df = df[3]
    print(donor_df.head())

def test():
    fields = [
        "submitter_id",
        "disease_type",
        "project.name",
        "project.program.dbgap_accession_number",
        "project.primary_site",
        "demographic.ethnicity",
        "demographic.gender",
        "diagnoses.primary_diagnosis",
    ]
    fields = ",".join(fields)
    files_endpt = "https://api.gdc.cancer.gov/cases"
    # This set of filters is nested under an 'and' operator.
    filters = {
        "op": "and",
        "content": [
            {
                "op": "in",
                "content": {
                    "field": "files.md5sum",
                    "value": ["047ec3e3782726d19f73eef83736f84b"]
                }
            }
        ]
    }
    params = {
        "filters": filters,
        "fields": fields,
        "format": "TSV",
        "size": "2000"
    }

    # The parameters are passed to 'json' rather than 'params' in this case
    response = requests.post(files_endpt, headers={"Content-Type": "application/json"}, json=params)

    print(response.content.decode("utf-8"))


if __name__ == "__main__":
    sys.exit(main())
