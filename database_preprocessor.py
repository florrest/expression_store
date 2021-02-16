import csv
import glob
import gzip
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
import psycopg2
import pg8000

import click
import dask.dataframe as dd
import pandas as pd
import pyranges as pr
import requests
import sqlalchemy
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database


@click.command()
@click.option('--rnaseq',
              prompt='path to nf-core/rna-seq base folder',
              help='input path', required=True)
@click.option('--sra_file',
              help='path to SRA accession list')
@click.option('--dcc_manifest',
              help='path to ICGC DCC manifest.tsv')
@click.option('--gdc_manifest',
              help='path to GDC manifest.tsv')
@click.option('--metadata_dest',
              prompt='path to download destination of metadata',
              help='path to metadata destination')
@click.option('--db_dest',
              prompt='path where to store created csv files for database',
              help='path to metadata destination')
def main(rnaseq, sra_file, dcc_manifest, gdc_manifest, metadata_dest, db_dest):
    #expression_df = dd.concat(DataProcessor.stringtie_results_out(os.path.join(rnaseq, 'results/stringtieFPKM/')))
    #raw_count_df = DataProcessor.convert_countinfo(rnaseq)
    #SQLAlchemy.process_expression(DataProcessor.create_expression_table(expression_df, raw_count_df, db_dest))

    #SQLAlchemy.process_expression_bulk_csv(db_dest)

    #SQLAlchemy.process_expression_bulk_df(DataProcessor.create_expression_table(expression_df, raw_count_df, db_dest))
    #gdc_uuid_list = Helper.get_gdc_filename_list(rnaseq)
    #Downloader.download_gdc_file_metadata(gdc_uuid_list, metadata_dest)
    #DataProcessor.concat_gdc_meta(os.path.join(metadata_dest, 'gdc/'))
    #Downloader.download_gdc_file_test()

    start = time.time()

    if dcc_manifest is not None:
        Downloader.download_icgc_project_metadata(Helper.get_icgc_project_list(dcc_manifest), metadata_dest)
        Helper.gunzip_files(os.path.join(metadata_dest, "icgc"))
    else:
        Log.logger.info("No ICGC DCC manifest file provided. SKIPPING")

    if sra_file is not None:
        Downloader.download_sra_metadata(Helper.get_sra_accession_list(sra_file), metadata_dest)
    else:
        Log.logger.info("No SRA accession list provided. SKIPPING")

    if gdc_manifest is not None:
        Downloader.download_gdc_file_metadata(Helper.get_gdc_filename_list(gdc_manifest), metadata_dest)
    else:
        Log.logger.info("No GDC manifest provided. SKIPPING")
    # logger.info('Download Metadata from GDC')
    # gdc_uuid_list = get_gdc_filename_list(inpath)
    # download_gdc_metadata(gdc_uuid_list, inpath)

    Log.logger.info('Creating CSV to load into database')
    expression_df = dd.concat(DataProcessor.stringtie_results_out(os.path.join(rnaseq, 'results/stringtieFPKM/')))
    raw_count_df = DataProcessor.convert_countinfo(rnaseq)
    DataProcessor.create_gene_table(Downloader.download_and_process_ensembl_metadata(rnaseq),
                                    Downloader.download_hgnc(rnaseq),
                                    expression_df,
                                    db_dest)
    DataProcessor.create_expression_table(expression_df, raw_count_df, db_dest)
    project_db = DataProcessor.create_project_layer_tables(
        DataProcessor.concat_sra_meta(os.path.join(metadata_dest, "sra"), Helper.get_sra_accession_list(sra_file)),
        DataProcessor.join_icgc_meta(DataProcessor.concat_icgc_meta(os.path.join(rnaseq, "metadata/icgc")), rnaseq),
        DataProcessor.concat_gdc_meta(os.path.join(metadata_dest, 'gdc/')),
        db_dest)

    DataProcessor.create_countinfo_table(project_db[3], rnaseq, db_dest)
    DataProcessor.create_pipeline_table(rnaseq, db_dest)
    print(time.time() - start)
    #SQLscripts.prepare_populate_script(db_dest)


class Log:
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


class Helper:
    # Return unzipped gz files and delete gz files
    @staticmethod
    def gunzip_files(inpath):
        gz_files = glob.glob(inpath + "/*.gz")
        for gz in gz_files:
            with gzip.open(gz, 'rb') as f_in:
                with open(gz[:-3], 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(gz)

    # url request to download data
    @staticmethod
    def retrieve_data(url, destination, retry_count=0):
        try:
            urllib.request.urlretrieve(url, destination)
        except ConnectionResetError as e:
            if retry_count == 5:
                raise e
            time.sleep(0.5)
            retry_count += 1
            Log.logger.warning('There are {} tries left to download from'.format(5 - retry_count))
            Helper.retrieve_data(url, destination, retry_count)

    # extract substring from a given string if it matches a pattern otherwise return original string
    @staticmethod
    def extract_icgc_run_id(string):
        string = string.split('.')[1]
        if string.startswith("SWID"):
            new = re.sub(r'_R._00.', "", string)
            return new
        else:
            new = re.sub(r'-R.', "", string)
            return new

    # extract substring from a given string if it matches a pattern otherwise return original string
    @staticmethod
    def extract_sample_id(string):
        if string.startswith('SRR'):
            string = re.sub(
                r"_.Aligned.sortedByCoord.*.*.*",
                "",
                string)
        else:
            string = re.sub(
                r"Aligned.sortedByCoord.out.*.*.*",
                "",
                string)
        return string

    # Wrapper function to create csv from dataframe. sep=',', index=False
    @staticmethod
    def create_csv(df, file_name, db_dest):
        df.to_csv(os.path.join(db_dest, '{}.csv'.format(file_name)), sep=',', index=False)

    #
    # Get identifiers and codes for downloaded sequence data
    #

    # returns ICGC DCC project list from given manifest file
    @staticmethod
    def get_icgc_project_list(dcc_manifest):
        icgc_project_codes = pd.DataFrame()
        try:
            icgc_project_codes = pd.read_csv(dcc_manifest,
                                             sep='\t',
                                             usecols=['project_id/project_count'])
            icgc_project_codes = icgc_project_codes['project_id/project_count'] \
                .unique() \
                .tolist()
        except:
            Log.logger.warning('ICGC DCC Manifest does not exist. Check path to manifest.')

        return icgc_project_codes

    # returns list of SRA accessions based on acc_list.txt (as used by S. Lemkes SRADownloader
    @staticmethod
    def get_sra_accession_list(path_to_txt):
        try:
            sra_accessions = pd.read_csv(path_to_txt, header=None)
            sra_accessions = sra_accessions[0].unique().tolist()
        except:
            Log.logger.warning('SRA accession list does not exist. Check path to accession list.')
        return sra_accessions

    # returns list of gdc uuids from given gdc manifest file
    @staticmethod
    def get_gdc_filename_list(inpath):
        try:
            uuids = pd.read_csv(inpath, sep='\t', usecols=['filename'])
            uuids = uuids['filename'].unique().tolist()
        except:
            Log.logger.warning('GDC Manifest does not exist. Check path to manifest.')
        return uuids


class Downloader:
    # download runInfo.csv for given accession
    @staticmethod
    def download_sra_metadata(project_lst, metadata_dest):
        if not os.path.exists(os.path.join(metadata_dest, 'sra/')):
            os.makedirs(os.path.join(metadata_dest, 'sra/'))
        else:
            # Check if SRA CSV files have already been downloaded, and if so, skip download
            downloaded_files = [os.path.basename(x).split('.')[0] for x in
                                glob.glob(os.path.join(metadata_dest, 'sra/', '*.csv'))]
            project_lst = [file for file in project_lst if file not in downloaded_files]

        if len(project_lst) > 0:
            Log.logger.info('Download Metadata from Sequence Read Archive (SRA)')
            for i in project_lst:
                sra_url = 'http://trace.ncbi.nlm.nih.gov/Traces/sra/' \
                          'sra.cgi?save=efetch&db=sra&rettype=runinfo&term={}'.format(i)
                Helper.retrieve_data(sra_url, os.path.join(metadata_dest, 'sra/') + str(i) + '.csv')
        else:
            Log.logger.info('Metadata from Sequence Read Archive (SRA) already exists')

    # download sample, specimen, donor and donor_set metadata from ICGC DCC based on project code
    @staticmethod
    def download_icgc_project_metadata(project_lst, metadata_dest):
        if not os.path.exists(os.path.join(metadata_dest, 'icgc/')):
            os.makedirs(os.path.join(metadata_dest, 'icgc/'))

        for i in project_lst:
            sample_url = 'https://dcc.icgc.org/api/v1/' \
                         'download?fn=/current/Projects/{}/sample.{}.tsv.gz'.format(i, i)
            donor_url = 'https://dcc.icgc.org/api/v1/' \
                        'download?fn=/current/Projects/{}/donor.{}.tsv.gz'.format(i, i)
            specimen_url = 'https://dcc.icgc.org/api/v1/' \
                           'download?fn=/current/Projects/{}/specimen.{}.tsv.gz'.format(i, i)
            donor_set_url = 'https://dcc.icgc.org/api/v1/repository/files/export?' \
                            'filters=%7B%22file%22%3A%7B%22projectCode%22%3A%7B%22is%22%3A%5B%22{}%22%5D%7D%7D%7D&' \
                            'type=tsv'.format(i)

            if os.path.isfile(os.path.join(metadata_dest, 'icgc/sample_' + str(i) + '.tsv.gz')) \
                    or os.path.isfile(os.path.join(metadata_dest, 'icgc/sample_' + str(i) + '.tsv')):
                continue
            else:
                Helper.retrieve_data(sample_url, metadata_dest + '/icgc/sample_' + str(i) + '.tsv.gz')
            if os.path.isfile(os.path.join(metadata_dest, 'icgc/donor_' + str(i) + '.tsv.gz')) \
                    or os.path.isfile(os.path.join(metadata_dest, 'icgc/donor_' + str(i) + '.tsv')):
                continue
            else:
                Helper.retrieve_data(donor_url, metadata_dest + '/icgc/donor_' + str(i) + '.tsv.gz')
            if os.path.isfile(os.path.join(metadata_dest, 'icgc/specimen_' + str(i) + '.tsv.gz')) \
                    or os.path.isfile(os.path.join(metadata_dest, 'icgc/specimen_' + str(i) + '.tsv')):
                continue
            else:
                Helper.retrieve_data(specimen_url, metadata_dest + '/icgc/specimen_' + str(i) + '.tsv.gz')
            if os.path.isfile(os.path.join(metadata_dest, 'icgc/repository_' + str(i) + '.tsv')):
                continue
            else:
                Helper.retrieve_data(donor_set_url, metadata_dest + '/icgc/repository_' + str(i) + '.tsv')

    @staticmethod
    def download_gdc_file_metadata(gdc_uuid_list, metadata_dest):
        if not os.path.exists(os.path.join(metadata_dest, 'gdc/')):
            os.makedirs(os.path.join(metadata_dest, 'gdc/'))
        downloaded_files = [os.path.basename(x).split('.')[0] for x in
                            glob.glob(os.path.join(metadata_dest, 'gdc/', '*.csv'))]
        blacklist = re.compile('|'.join([re.escape(word) for word in downloaded_files]))
        missing = [file for file in gdc_uuid_list if not blacklist.search(file)] # REMOVE DOWNLOADED UUIDS

        if len(missing) > 0:
            Log.logger.info('Download Metadata from GDC')
            cases_endpt = 'https://api.gdc.cancer.gov/files'
            for i in missing:
                # The 'fields' parameter is passed as a comma-separated string of single names.
                fields = [
                    "access",
                    "experimental_strategy",
                    "platform",
                    "type",
                    "cases.demographic.gender",
                    "cases.diagnoses.primary_diagnosis",
                    "cases.diagnoses.tumor_grade",
                    "cases.diagnoses.tumor_stage",
                    "cases.diagnoses.vital_status",
                    "cases.diagnoses.age_at_diagnosis",
                    "cases.project.dbgap_accession_number",
                    "cases.project.disease_type",
                    "cases.project.primary_site",
                    "cases.project.project_id",
                    "cases.samples.sample_id",
                    "cases.samples.sample_type",
                    "cases.submitter_id",
                    "cases.diagnoses.last_known_disease_status",
                    "cases.diagnoses.prior_malignancy",
                    "cases.diagnoses.classification_of_tumor",
                    "cases.samples.tumor_code",
                    "cases.samples.tumor_code_id",
                    "cases.samples.tumor_descriptor",
                    "cases.samples.preservation_method",
                    "cases.samples.sample_type",
                    "cases.samples.tissue_type",
                    "cases.samples.tumor_code",
                    "cases.samples.tumor_code_id",
                ]

                fields = ','.join(fields)
                filters = {
                    "op": "in",
                    "content": {
                        "field": "file_name",
                        "value": [str(i)]
                    }
                }

                params = {
                    "filters": json.dumps(filters),
                    "fields": fields,
                    "format": "CSV",
                    "size": "100"
                }

                response = requests.get(cases_endpt, params=params)
                with open(os.path.join(metadata_dest,"gdc/" "{}.csv".format(str(i).split('_')[0])), "wb") as csv_file:
                    csv_file.write(response.content)
            else:
                Log.logger.info('Metadata from GDC already downloaded')
                    
    # download and process ensembl metadata, so it just returns relevant gene entries
    @staticmethod
    def download_and_process_ensembl_metadata(inpath):
        if os.path.isfile(os.path.join(inpath, '/metadata/ensemble/genes_GRCh37.gtf')):
            if not os.path.exists(os.path.join(inpath, 'metadata/ensembl/')):
                os.makedirs(os.path.join(inpath, 'metadata/ensembl/'))
                ensembl_url = 'ftp://ftp.ensembl.org/pub/' \
                              'grch37/current/gtf/homo_sapiens/Homo_sapiens.GRCh37.87.gtf.gz'
                Helper.retrieve_data(ensembl_url, inpath + '/metadata/ensembl/'
                                                           'Homo_sapiens.GRCh37.87.gtf.gz')
                Helper.gunzip_files(os.path.join(inpath, "metadata/ensembl"))
                cwd = os.getcwd()
                os.chdir(os.path.join(inpath, "metadata/ensembl"))
                subprocess.call(
                    '''awk -F"\t" '$3 == "gene" { print $0 }' Homo_sapiens.GRCh37.87.gtf > genes_GRCh37.gtf''',
                    shell=True)
                os.remove('Homo_sapiens.GRCh37.87.gtf')
                ensembl_df = pr.read_gtf('genes_GRCh37.gtf',
                                         as_df=True)
                ensembl_df = ensembl_df[['gene_id', 'gene_version', 'gene_name', 'gene_biotype']]
                os.chdir(cwd)
                return ensembl_df
        else:
            Log.logger.info('Ensembl metadata already downloaded. Processing existing file.')
            cwd = os.getcwd()
            os.chdir(os.path.join(inpath, "metadata/ensembl"))
            ensembl_df = pr.read_gtf('genes_GRCh37.gtf',
                                     as_df=True)
            ensembl_df = ensembl_df[['gene_id', 'gene_version', 'gene_name', 'gene_biotype']]
            os.chdir(cwd)
            return ensembl_df

    # download hgnc metadata and return a dataframe containing all relevant information
    @staticmethod
    def download_hgnc(inpath):
        if not os.path.exists(os.path.join(inpath, 'metadata/hgnc/')):
            os.makedirs(os.path.join(inpath, 'metadata/hgnc/'))
        hgnc_url = 'ftp://ftp.ebi.ac.uk/pub/databases/genenames/hgnc/tsv/hgnc_complete_set.txt'
        Helper.retrieve_data(hgnc_url, inpath + '/metadata/hgnc/hgnc_complete_set.txt')
        hgnc_df = pd.read_csv(os.path.join(inpath, 'metadata/hgnc/hgnc_complete_set.txt'),
                              sep='\t',
                              usecols=['hgnc_id', 'symbol', 'locus_group', 'locus_type', 'gene_family'])
        return hgnc_df


class DataProcessor:
    # concat metadata from downloaded SRA runInfo.csv
    @staticmethod
    def concat_sra_meta(inpath_sra_meta_csv, used_accessions):
        sra_csv_files = glob.glob(inpath_sra_meta_csv + "/*.csv")
        sra_files_list = []
        for file in sra_csv_files:
            df = pd.read_csv(file, sep=',')
            sra_files_list.append(df)
        frame = pd.concat(sra_files_list, axis=0)
        frame = frame[frame['Run'].isin(used_accessions)]
        frame = frame.rename(columns=ListDict.sra_col_dict)
        frame['portal'] = "SRA"
        return frame

    # concat metadata from downloaded ICGC DCC metadata files
    @staticmethod
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
        if len(icgc_donor) > 0:
            donor_frame = pd.concat(icgc_donor, axis=0)
            sample_frame = pd.concat(icgc_sample, axis=0)
            specimen_frame = pd.concat(icgc_specimen, axis=0)
            repository_frame = pd.concat(icgc_repository, axis=0)
            return donor_frame, sample_frame, specimen_frame, repository_frame
        else:
            return icgc_donor, icgc_sample, icgc_specimen, icgc_repository

    @staticmethod
    def concat_gdc_meta(inpath_gdc_meta):
        try:
            gdc_csv_files = glob.glob(inpath_gdc_meta + "/*.csv")
            gdc_meta = []
            for file in gdc_csv_files:
                df = pd.read_csv(file, sep=',')
                gdc_meta.append(df)
            gdc_meta_df = pd.concat(gdc_meta)
            gdc_meta_df = gdc_meta_df.rename(columns=ListDict.gdc_col_dict)
            gdc_meta_df['portal'] = 'GDC'
            return gdc_meta_df
        except:
            gdc_meta_df = pd.DataFrame(columns=ListDict.gdc_col_dict)
            return gdc_meta_df


    # join and return dataframes containing metadata about ICGC DCC sequences
    @staticmethod
    def join_icgc_meta(df, base_path):
        try:
            donor_df = df[0]
            sample_df = df[1]
            specimen_df = df[2]
            repository_df = df[3].rename(columns=ListDict.icgc_col_names_repository)
            manifest_df = pd.read_csv(os.path.join(base_path, './manifest.tsv'),
                                      sep='\t', usecols=['file_id', 'file_name'])

            merge_rep_man = pd.merge(repository_df, manifest_df,
                                     on=['file_id', 'file_name'],
                                     how='right')
            merge_rep_man['file_name'] = merge_rep_man['file_name'].apply(lambda x: Helper.extract_icgc_run_id(x))
            sample_df = pd.merge(sample_df, specimen_df,
                                 on=['icgc_specimen_id', 'icgc_donor_id', 'project_code', 'submitted_donor_id',
                                     'submitted_specimen_id', 'percentage_cellularity', 'level_of_cellularity'],
                                 how='left')

        except:
            Log.logger.info('No ICGC DCC Manifest provided')
            merge_rep_man = pd.DataFrame(columns=ListDict.rep_man_cols)
            donor_df = pd.DataFrame(columns=ListDict.donor_df_cols)
            sample_df = pd.DataFrame(columns=ListDict.sample_df_cols)

        return merge_rep_man, donor_df, sample_df

    # process stringtie output from nf-core/rna-seq pipeline and return dataframe containing expression data
    @staticmethod
    def stringtie_results_out(res_inpath):
        data = []
        files = glob.glob(res_inpath + "/*.txt")
        for file in files:
            frame = pd.read_csv(file, sep='\t')
            frame = frame.drop(['Start', 'End'], axis=1)
            # frame['Strand'] = frame['Strand'].astype('object')
            frame['run'] = Helper.extract_sample_id(file.split('/')[-1])
            frame['run'] = frame['run'].map(lambda x: re.sub(r"-R1", "", x))
            frame['run'] = frame['run'].map(lambda x: re.sub(r"-R2", "", x))
            frame = frame.rename(
                columns=ListDict.stringtie_dict)
            dd_frame = dd.from_pandas(frame, npartitions=2)
            data.append(dd_frame)
        return data

    # Create TABLES
    @staticmethod
    def create_gene_table(ensembl_df, hgnc_df, expression_df, db_dest):
        # Convert DASK DataFrame to Pandas DataFrame
        expression_df = expression_df[['gene_id', 'gene_name', 'reference', 'strand']]
        expression_df = expression_df.drop_duplicates().compute()

        gene = pd.merge(expression_df, ensembl_df,
                        on=['gene_id', 'gene_name'],
                        how='left')
        gene = pd.merge(gene, hgnc_df,
                        left_on='gene_name',
                        right_on='symbol',
                        how='left')
        Helper.create_csv(gene, 'gene', db_dest)

    @staticmethod
    def create_expression_table(expression_df, raw_count_df, db_dest):
        expression = expression_df[['run', 'gene_id', 'fpkm',
                                    'tpm', 'coverage']]
        expression = expression.groupby(['run', 'gene_id']).sum().reset_index().compute()
        expression = pd.merge(expression, raw_count_df, on=['gene_id', 'run'], how='left')
        Helper.create_csv(expression, "expression", db_dest)


    @staticmethod
    def create_project_layer_tables(sra_meta_df, icgc_meta_df, gdc_meta_df, db_dest):
        try:
            sra = sra_meta_df.rename(columns=ListDict.database_columns)
        except:
            sra = pd.DataFrame(columns=ListDict.sra_df_cols)
        rep_man = icgc_meta_df[0].rename(columns=ListDict.database_columns)
        donor = icgc_meta_df[1].rename(columns=ListDict.database_columns)
        sample = icgc_meta_df[2].rename(columns=ListDict.database_columns)


        merged_sample_donor = pd.merge(sample, donor, on=['donor_id', 'project_code', 'submitted_donor_id'], how='left')
        final_merge = pd.merge(merged_sample_donor, rep_man,
                               on=['donor_id', 'icgc_specimen_id', 'project_code', 'specimen_type', 'sample_id'],
                               how='right')
        final_merge['portal'] = 'ICGC DCC'
        concat_meta = pd.concat([final_merge, sra], axis=0)
        concat_meta = pd.concat([concat_meta, gdc_meta_df], axis=0)

        project_db = concat_meta[['project_code', 'study', 'sra_study',
                                  'dbgap_study_accession']]
        project_db = project_db.drop_duplicates()

        donor_db = concat_meta[ListDict.donor_db_list]
        donor_db = donor_db[donor_db['donor_id'].notna()]
        donor_db = donor_db.drop_duplicates()
        sample_db = concat_meta[ListDict.sample_db_list]
        sample_db = sample_db.drop_duplicates()
        sample_db['body_site'] = sample_db['body_site'] \
            .fillna(sample_db['project_code'].astype(str).map(ListDict.icgc_tissue_dict))

        countinfo_db = concat_meta[['run', 'library_name', 'library_strategy', 'library_selection',
                                    'library_source', 'library_layout']]

        Helper.create_csv(project_db, 'project', db_dest)
        Helper.create_csv(sample_db, 'sample', db_dest)
        Helper.create_csv(donor_db, 'donor', db_dest)
        return project_db, donor_db, sample_db, countinfo_db

    @staticmethod
    def create_countinfo_table(df, inpath, db_dest):
        merged_gene_counts = pd.read_csv(os.path.join(inpath, "results/featureCounts/merged_gene_counts.txt"),
                                         sep='\t')
        merged_gene_counts_series = merged_gene_counts.drop(
            ['Geneid', 'gene_name'], axis=1)
        sum_gene_counts = merged_gene_counts_series.sum(axis=0)
        countinfo = sum_gene_counts.to_frame()
        countinfo = countinfo.reset_index(level=0)
        countinfo = countinfo.rename(
            columns={
                'index': 'run',
                0: 'sum_counts'})
        countinfo['run'] = countinfo['run'].apply(lambda x: Helper.extract_sample_id(x))
        countinfo['run'] = countinfo['run'].map(lambda x: re.sub(r"-R1", "", x))
        countinfo['run'] = countinfo['run'].map(lambda x: re.sub(r"-R2", "", x))
        countinfo = pd.merge(countinfo,
                             df[['run', 'library_name', 'library_strategy',
                                 'library_selection', 'library_source', 'library_layout']],
                             on='run',
                             how='left')
        countinfo = countinfo.drop_duplicates()
        Helper.create_csv(countinfo, 'countinfo', db_dest)

    @staticmethod
    def create_pipeline_table(inpath, db_dest):
        pipeline = pd.read_csv(os.path.join(inpath, 'results/pipeline_info/software_versions.csv'),
                               sep='\t',
                               header=None).T
        pipeline = pipeline.rename(columns=pipeline.iloc[0])
        pipeline = pipeline.iloc[1:]
        pipeline = pipeline.rename(columns={'Trim Galore!': 'Trim_Galore',
                                            'Picard MarkDuplicates': 'Picard_MarkDuplicates',
                                            'nf-core/rnaseq': 'nf_core_rnaseq'})
        Helper.create_csv(pipeline, "pipeline", db_dest)

    @staticmethod
    def convert_countinfo(rnaseq):
        df = pd.read_csv(os.path.join(rnaseq, "results/featureCounts/merged_gene_counts.txt"), sep='\t')
        df.columns = list(map(lambda x: Helper.extract_sample_id(x), df.columns))
        df = df.drop(['gene_name'], axis=1).set_index(['Geneid'])
        df = df.stack().to_frame().reset_index()
        df = df.rename(columns={'Geneid': 'gene_id',
                                'level_1': 'run',
                                0: 'raw_count'})
        df['run'] = df['run'].map(lambda x: re.sub(r"-R1","", x))
        df['run'] = df['run'].map(lambda x: re.sub(r"-R2", "", x))

        return df


class ListDict:
    # Dictionaries
    with \
            open('dicts_lists/sra_col_dict.json', 'r') as sra_col, \
            open('dicts_lists/icgc_rep_dict.json', 'r') as icgc_col_names, \
            open('dicts_lists/column_db_dict.json', 'r') as db_col, \
            open('dicts_lists/icgc_tissue_dict.json', 'r') as icgc_tissue, \
            open('dicts_lists/stringtie_out_dict.json', 'r') as stringtie_dict, \
            open('dicts_lists/gdc_col_dict.json', 'r') as gdc_cols:
        sra_col_dict = json.load(sra_col)
        icgc_col_names_repository = json.load(icgc_col_names)
        database_columns = json.load(db_col)
        icgc_tissue_dict = json.load(icgc_tissue)
        stringtie_dict = json.load(stringtie_dict)
        gdc_col_dict = json.load(gdc_cols)

    # Database columns
    with \
            open('dicts_lists/icgc_repository_manifdest_col_list.json', 'r') as rep_man, \
            open('dicts_lists/donor_df_col_list.json', 'r') as don, \
            open('dicts_lists/sample_df_col_list.json', 'r') as sam, \
            open('dicts_lists/sra_df_col_list.json', 'r') as sra_df, \
            open('dicts_lists/donor_db_list.json', 'r') as donor_db, \
            open('dicts_lists/sample_db_list.json', 'r') as sample_db:
        rep_man_cols = json.load(rep_man)
        donor_df_cols = json.load(don)
        sample_df_cols = json.load(sam)
        sra_df_cols = json.load(sra_df)
        donor_db_list = json.load(donor_db)
        sample_db_list = json.load(sample_db)


class SQLscripts:
    @staticmethod
    def prepare_populate_script(db_dest):
        with open('postgresql_scripts/populate_tables_template.sql', 'r') as sql_file:
            script = sql_file.read()

        script = script.replace("$PATH$", str(db_dest))
        with open('postgresql_scripts/populate_tables.sql', 'w') as file:
            file.write(script)


class SQLAlchemy:
    @staticmethod
    def process_expression_to_sql(expression_df):

        engine = create_engine('postgresql+psycopg2://flo:test@localhost:5432/expression_store', echo=False)
        if not database_exists(engine.url):
            create_database(engine.url)
        conn = engine.connect()
        meta = MetaData()
        start = time.time()
        expression_df.to_sql('expression', conn, if_exists='replace', chunksize=5000, method='multi')
        print(time.time()-start)
        conn.close()

    @staticmethod
    def process_expression_bulk_csv(db_dest):
        Base = declarative_base()
        class Expression(Base):
            __tablename__ = 'expression'
            sample_id = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            gene_id = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            fpkm = sqlalchemy.Column(sqlalchemy.FLOAT)
            tpm = sqlalchemy.Column(sqlalchemy.FLOAT)
            coverage = sqlalchemy.Column(sqlalchemy.FLOAT)
            raw_count = sqlalchemy.Column(sqlalchemy.BIGINT)


        conn_string = "postgresql+psycopg2://flo:test@localhost:5432/expression_store"
        engine = create_engine(conn_string)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        start = time.time()
        with open(os.path.join(db_dest, 'expression.csv'), 'r') as csv_file:
            next(csv_file)
            csv_reader = csv.reader(csv_file)
            buffer = []
            for row in csv_reader:
                buffer.append({
                    'sample_id': row[0],
                    'gene_id': row[1],
                    'fpkm': row[2],
                    'tpm': row[3],
                    'coverage': row[4],
                    'raw_count': row[5]
                })
                if len(buffer) % 100000 == 0:
                    session.bulk_insert_mappings(Expression, buffer)
                    buffer = []

            session.bulk_insert_mappings(Expression, buffer)
            session.commit()
            session.close()
        print(time.time() - start)


    @staticmethod
    def process_expression_bulk_df(expression_df):
        Base = declarative_base()
        class Expression(Base):
            __tablename__ = 'expression'
            sample_id = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            gene_id = sqlalchemy.Column(sqlalchemy.String, primary_key=True)
            fpkm = sqlalchemy.Column(sqlalchemy.FLOAT)
            tpm = sqlalchemy.Column(sqlalchemy.FLOAT)
            coverage = sqlalchemy.Column(sqlalchemy.FLOAT)
            raw_count = sqlalchemy.Column(sqlalchemy.BIGINT)


        conn_string = 'postgresql+psycopg2://flo:test@localhost:5432/expression_store'
        engine = create_engine(conn_string)
        Base.metadata.create_all(engine)
        conn = engine.connect()


        df_to_write = expression_df.to_dict(orient='records')
        metadata = sqlalchemy.schema.MetaData(bind=engine, reflect=True)
        #table = sqlalchemy.Table('expression', metadata, autoload=True)
        Session = sessionmaker(bind=engine)
        session = Session()
        start = time.time()
        session.bulk_insert_mappings(Expression, df_to_write)
        #conn.execute(table.insert(), df_to_write)

        session.commit()
        session.close()

        print(time.time() - start)

if __name__ == "__main__":
    sys.exit(main())
