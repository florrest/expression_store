import argparse
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
from pathlib import Path

import dask.dataframe as dd
import pandas as pd
import pyranges as pr
import requests
from dask.diagnostics import ProgressBar

parser = argparse.ArgumentParser(description='Database Preprocessor for expression_store')
parser.add_argument('--rnaseq', type=str,
                    help='Path to base directory of nf-core/rnaseq', required=True)
parser.add_argument('--sra_file', type=str,
                    help='Path to SRA accession txt file')
parser.add_argument('--dcc_manifest', type=str,
                    help='Path to ICGC DCC manifest tsv file')
parser.add_argument('--gdc_manifest', type=str,
                    help='Path to GDC manifest tsv file')
parser.add_argument('--metadata_dest', type=str,
                    help='Path to directory, where metadata should be downloaded to', required=True)
parser.add_argument('--db_dest', type=str,
                    help='Path to directory, where csv files for database should be stored', required=True)


def main():
    """ This is the main class, which performs all necessary steps to successfully download, process and export metadata
    and data for the database

    :param rnaseq: Path to the base directory of nf-core/rnaseq
    :param sra_file: Path to the SRA accession list txt-file
    :param dcc_manifest: Path to the manifest file of ICGC DCC
    :param gdc_manifest: Path to the manifest file of GDC
    :param metadata_dest: Path to a previously created directory, to store downloaded metadata
    :param db_dest: Path to the destination directory of the created csv-files, which are imported to the database
    :return: None
    """
    args = parser.parse_args()
    rnaseq = args.rnaseq
    sra_file = args.sra_file
    dcc_manifest = args.dcc_manifest
    gdc_manifest = args.gdc_manifest
    metadata_dest = args.metadata_dest
    db_dest = args.db_dest

    Log.logger.info("Downloading Data")
    if dcc_manifest is not None:
        Downloader.download_icgc_project_metadata(Helper.get_icgc_project_list(dcc_manifest), metadata_dest)
        Helper.gunzip_files(os.path.join(metadata_dest, "icgc"))
    else:
        Log.logger.info("No ICGC DCC manifest file provided or Metadata already downloaded. SKIPPING")

    if sra_file is not None:
        Downloader.download_sra_metadata(Helper.get_sra_accession_list(sra_file), metadata_dest)
    else:
        Log.logger.info("No SRA accession list provided or Metadata already downloaded. SKIPPING")

    if gdc_manifest is not None:
        Downloader.download_gdc_file_metadata(Helper.get_gdc_filename_list(gdc_manifest), metadata_dest)
    else:
        Log.logger.info("No GDC manifest provided or Metadata already downloaded. SKIPPING")

    Downloader.download_ensembl_metadata(metadata_dest)
    Downloader.download_hgnc(metadata_dest)

    Log.logger.info("Creating ProjectLayer csv files")
    project_db = DataProcessor.create_project_layer_tables(
        DataProcessor.concat_sra_meta(os.path.join(metadata_dest, "sra"), Helper.get_sra_accession_list(sra_file)),
        DataProcessor.concat_icgc_meta(metadata_dest, dcc_manifest),
        DataProcessor.concat_gdc_meta(os.path.join(metadata_dest, 'gdc/')),
        db_dest)

    Log.logger.info("Creating ExpressionLayer csv files")
    raw_count_df = DataProcessor.create_countinfo_raw_count_table(project_db[3], rnaseq, db_dest)

    expression_df = dd.concat(DataProcessor.stringtie_results_out(os.path.join(rnaseq, 'results/stringtieFPKM/')))

    DataProcessor.create_gene_table(DataProcessor.process_ensembl_metadata(metadata_dest),
                                    DataProcessor.process_hgnc_metadata(metadata_dest),
                                    expression_df,
                                    db_dest)

    DataProcessor.create_expression_table(expression_df,
                                          raw_count_df,
                                          db_dest)

    DataProcessor.create_pipeline_table(rnaseq, db_dest)

    SQLscripts.prepare_populate_script(db_dest)


class Log:
    logger = logging.getLogger('Expression Store Script Logger')
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(logging.INFO)


class Helper:
    @staticmethod
    def gunzip_files(directory):
        """This method unzips all .gz files in a given directory, and deletes the .gz files afterwards.

        :param directory: Path to directory, which contains files to be unzipped
        :return: None
        """
        gz_files = glob.glob(directory + "/*.gz")
        for gz in gz_files:
            with gzip.open(gz, 'rb') as f_in:
                with open(gz[:-3], 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(gz)

    @staticmethod
    def retrieve_data(url, destination, retry_count=0):
        """This method downloads files from a remote source and saves them in the destination directory.

        :param url: URL to file, which has to be downloaded
        :param destination: Path to directory, where the files should be saved
        :param retry_count: When retry_count is 0, stop script
        :return:
        """
        try:
            urllib.request.urlretrieve(url, destination)
        except ConnectionResetError as e:
            if retry_count == 5:
                raise e
            retry_count += 1
            Log.logger.warning('There are {} tries left to download from'.format(5 - retry_count))
            time.sleep(1)
            Helper.retrieve_data(url, destination, retry_count)

    @staticmethod
    def extract_icgc_run_id(string):
        """This method splits a specific string, created by ICGC.The split truncates the Read suffix.

        :param string: Any given string, which might needs be truncated.
        :return: splitted String
        """
        string = string.split('.')[1]
        if string.startswith("SWID"):
            new = re.sub(r'_R._00.', "", string)
            return new
        else:
            new = re.sub(r'-R.', "", string)
            return new

    @staticmethod
    def extract_sample_id(string):
        """This method splits sample_id strings, and truncates the substrings generated by StringTie

        :param string: Any given string, which might needs be truncated.
        :return: splitted String
        """
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

    @staticmethod
    def create_csv(df, file_name, db_dest):
        """This method exports a DataFrame to a csv-File with no Index

        :param df: DataFrame to be exported
        :param file_name: File name for the exported csv-file
        :param db_dest: Destination Directory, where csv should be saved
        :return: None
        """
        Log.logger.info("Writing csv from {} dataframe".format(file_name))
        df.to_csv(os.path.join(db_dest, '{}.csv'.format(file_name)), sep=',', index=False)

    @staticmethod
    def create_csv_from_dask(ddf, file_name, db_dest):
        """This method exports a DASK DataFrame to a csv-File with no Index

        :param ddf: dask DataFrame
        :param file_name: File name for the exported csv-file
        :param db_dest: Destination Directory, where csv should be saved
        :return:
        """
        Log.logger.info("Writing csv from {} dask dataframe".format(file_name))
        start = time.time()
        with ProgressBar():
            ddf.to_csv(os.path.join(db_dest, '{}.csv'.format(file_name)), single_file=True, index=False)
        Log.logger.info("Writing {}.csv has taken {} seconds".format(file_name, time.time() - start))

    @staticmethod
    def get_icgc_project_list(path_to_dcc_manifest):
        """This method creates a unique list of ICGC DCC projects, for which metadata has to be downloaded

        :param path_to_dcc_manifest: Path to ICGC DCC manifest file
        :return: list of unique ICGC DCC project codes
        """
        icgc_project_codes = pd.DataFrame()
        try:
            icgc_project_codes = pd.read_csv(path_to_dcc_manifest,
                                             sep='\t',
                                             usecols=['project_id/project_count'])
            icgc_project_codes = icgc_project_codes['project_id/project_count'] \
                .unique() \
                .tolist()
        except:
            Log.logger.warning('ICGC DCC Manifest does not exist. Check path to manifest.')

        return icgc_project_codes

    @staticmethod
    def get_sra_accession_list(path_to_sra_acc_list):
        """This method creates a unique list of SRA accessions, for which metadata has to be downloaded

        :param path_to_sra_acc_list: path to SRA Accession List
        :return: list of unique SRA accesssions
        """
        try:
            sra_accessions = pd.read_csv(path_to_sra_acc_list, header=None)
            sra_accessions = sra_accessions[0].unique().tolist()
        except:
            Log.logger.warning('SRA accession list does not exist. Check path to accession list.')
        return sra_accessions

    @staticmethod
    def get_gdc_filename_list(path_to_gdc_manifest):
        """This method creates a list of GDC files, for which metadata has to be downloaded

        :param path_to_gdc_manifest: path to GDC manifest file
        :return: returns a unique list of GDC file_names
        """
        try:
            gdc_file_names = pd.read_csv(path_to_gdc_manifest, sep='\t', usecols=['filename'])
            gdc_file_names = gdc_file_names['filename'].unique().tolist()
        except:
            Log.logger.warning('GDC Manifest does not exist. Check path to manifest.')
        return gdc_file_names


class Downloader:
    @staticmethod
    def download_sra_metadata(sra_acc_lst, metadata_dest):
        """This method downloads runInfo.csv from SRA depending on a list of SRA accessions

        :param sra_acc_lst: List containing unique SRA Run accession
        :param metadata_dest: Path to directory, where metadata should be downloaded to
        :return:
        """
        sra_meta_dir = os.path.join(metadata_dest, 'sra/')

        if not os.path.exists(sra_meta_dir):
            os.makedirs(sra_meta_dir)
        else:
            # Check if SRA CSV files have already been downloaded, and if so, skip download
            downloaded_files = [os.path.basename(x).split('.')[0] for x in
                                glob.glob(os.path.join(sra_meta_dir, '*.csv'))]
            sra_acc_lst = [file for file in sra_acc_lst if file not in downloaded_files]

        if len(sra_acc_lst) > 0:
            Log.logger.info('Download Metadata from Sequence Read Archive (SRA)')
            for i in sra_acc_lst:
                sra_url = 'http://trace.ncbi.nlm.nih.gov/Traces/sra/' \
                          'sra.cgi?save=efetch&db=sra&rettype=runinfo&term={}'.format(i)
                Helper.retrieve_data(sra_url, sra_meta_dir + str(i) + '.csv')
        else:
            Log.logger.info('Metadata from Sequence Read Archive (SRA) already exists')

    @staticmethod
    def download_icgc_project_metadata(icgc_project_lst, metadata_dest):
        """This method downloads all necessary metadata from ICGC DCC

        :param icgc_project_lst: Unique List of Project Codes from ICGC DCC
        :param metadata_dest: Path to directory, where metadata should be downloaded tod
        :return:
        """

        icgc_meta_dest = os.path.join(metadata_dest, 'icgc/')
        if not os.path.exists(icgc_meta_dest):
            os.makedirs(icgc_meta_dest)
        Log.logger.info("Downloading Metadata from ICGC DCC, if not already exists.")
        for i in icgc_project_lst:
            sample_url = 'https://dcc.icgc.org/api/v1/' \
                         'download?fn=/current/Projects/{}/sample.{}.tsv.gz'.format(i, i)
            donor_url = 'https://dcc.icgc.org/api/v1/' \
                        'download?fn=/current/Projects/{}/donor.{}.tsv.gz'.format(i, i)
            specimen_url = 'https://dcc.icgc.org/api/v1/' \
                           'download?fn=/current/Projects/{}/specimen.{}.tsv.gz'.format(i, i)
            donor_set_url = 'https://dcc.icgc.org/api/v1/repository/files/export?' \
                            'filters=%7B%22file%22%3A%7B%22projectCode%22%3A%7B%22is%22%3A%5B%22{}%22%5D%7D%7D%7D&' \
                            'type=tsv'.format(i)

            if os.path.isfile(os.path.join(icgc_meta_dest, 'sample_' + str(i) + '.tsv.gz')) \
                    or os.path.isfile(os.path.join(icgc_meta_dest, 'sample_' + str(i) + '.tsv')):
                pass
            else:
                Helper.retrieve_data(sample_url, icgc_meta_dest + 'sample_' + str(i) + '.tsv.gz')
            if os.path.isfile(os.path.join(icgc_meta_dest, 'donor_' + str(i) + '.tsv.gz')) \
                    or os.path.isfile(os.path.join(icgc_meta_dest, 'donor_' + str(i) + '.tsv')):
                pass
            else:
                Helper.retrieve_data(donor_url, icgc_meta_dest + 'donor_' + str(i) + '.tsv.gz')
            if os.path.isfile(os.path.join(icgc_meta_dest, 'specimen_' + str(i) + '.tsv.gz')) \
                    or os.path.isfile(os.path.join(icgc_meta_dest, 'specimen_' + str(i) + '.tsv')):
                pass
            else:
                Helper.retrieve_data(specimen_url, icgc_meta_dest + 'specimen_' + str(i) + '.tsv.gz')
            if os.path.isfile(os.path.join(icgc_meta_dest, 'repository_' + str(i) + '.tsv')):
                pass
            else:
                Helper.retrieve_data(donor_set_url, icgc_meta_dest + 'repository_' + str(i) + '.tsv')

    @staticmethod
    def download_gdc_file_metadata(gdc_file_name_list, metadata_dest):
        """This method downloads all required Metadata from GDC

        :param gdc_file_name_list: Unique list of GDC file names
        :param metadata_dest: Path to directory, where metadata should be downloaded to
        :return:
        """
        gdc_meta_dest = os.path.join(metadata_dest, 'gdc/')

        if not os.path.exists(gdc_meta_dest):
            os.makedirs(gdc_meta_dest)
        downloaded_files = [os.path.basename(x).split('.')[0] for x in
                            glob.glob(os.path.join(gdc_meta_dest, '/*.csv'))]

        missing = [file for file in gdc_file_name_list
                   if not any(downloaded in file for downloaded in downloaded_files)]
        print(missing)
        if len(missing) > 0:
            Log.logger.info('Download Metadata from GDC')
            cases_endpt = 'https://api.gdc.cancer.gov/files'
            for i in missing:
                fields = ListDict.gdc_attributes
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
                with open(os.path.join(gdc_meta_dest, "{}.csv".format(str(i).split('_')[0])), "wb") as csv_file:
                    csv_file.write(response.content)
            else:
                Log.logger.info('Metadata from GDC already downloaded')

    @staticmethod
    def download_ensembl_metadata(metadata_dest):
        """This method downloads data from Ensembl!

        :param metadata_dest: Path to directory, where metadata has been downloaded to
        :return:
        """
        ensembl_meta_dest = os.path.join(metadata_dest, 'ensembl/')
        if not os.path.exists(ensembl_meta_dest):
            os.makedirs(ensembl_meta_dest)
        if not os.path.isfile(os.path.join(ensembl_meta_dest, 'genes_GRCh37.gtf')):
            Log.logger.info("Downloading Ensembl Metadata")
            ensembl_url = 'ftp://ftp.ensembl.org/pub/' \
                          'grch37/current/gtf/homo_sapiens/Homo_sapiens.GRCh37.87.gtf.gz'
            Helper.retrieve_data(ensembl_url, ensembl_meta_dest + '/Homo_sapiens.GRCh37.87.gtf.gz')
            Helper.gunzip_files(ensembl_meta_dest)
            cwd = os.getcwd()
            os.chdir(ensembl_meta_dest)
            subprocess.call(
                '''awk -F"\t" '$3 == "gene" { print $0 }' Homo_sapiens.GRCh37.87.gtf > genes_GRCh37.gtf''',
                shell=True)
            os.remove('Homo_sapiens.GRCh37.87.gtf')
            os.chdir(cwd)
        else:
            Log.logger.info("Ensembl Metadata already downloaded and processed")

    @staticmethod
    def download_hgnc(metadata_dest):
        """This method downloads metadata from HGNC

        :param metadata_dest: Path to directory, where metadata should be saved
        :return: DataFrame containing information from HGNC
        """
        hgnc_meta_dest = os.path.join(metadata_dest, 'hgnc/')
        if not os.path.exists(hgnc_meta_dest):
            os.makedirs(hgnc_meta_dest)
        if not os.path.isfile(os.path.join(hgnc_meta_dest, 'hgnc_complete_set.txt')):
            Log.logger.info("Downloading HGNC Metadata")
            hgnc_url = 'ftp://ftp.ebi.ac.uk/pub/databases/genenames/hgnc/tsv/hgnc_complete_set.txt'
            Helper.retrieve_data(hgnc_url, hgnc_meta_dest + '/hgnc_complete_set.txt')
        else:
            Log.logger.info("HGNC Metadata already downloaded")


class DataProcessor:
    @staticmethod
    def process_hgnc_metadata(metadata_dest):
        """This method processes the metadata from HGNC

        :param metadata_dest: Path to directory, where metadata has been downloaded to
        :return: DataFrame containing information about HGNC
        """
        path_to_hgnc_txt = os.path.join(metadata_dest, 'hgnc/hgnc_complete_set.txt')
        hgnc_df = pd.read_csv(path_to_hgnc_txt,
                              sep='\t',
                              usecols=['hgnc_id', 'symbol', 'locus_group', 'locus_type', 'gene_family'])
        return hgnc_df

    @staticmethod
    def process_ensembl_metadata(metadata_dest):
        """This method processes ensembl metadata from the processed gtf file

        :param metadata_dest: Path to directory, where metadata has been downloaded to
        :return: DataFrame containing information about Ensembl
        """
        path_to_ensembl_gtf = os.path.join(metadata_dest, 'ensembl/genes_GRCh37.gtf')
        ensembl_df = pr.read_gtf(path_to_ensembl_gtf,
                                 as_df=True)
        ensembl_df = ensembl_df[['gene_id', 'gene_version', 'gene_name', 'gene_biotype']]
        return ensembl_df

    @staticmethod
    def concat_sra_meta(inpath_sra_meta_csv, used_accessions):
        """This method concatenates all downloaded metadata files from SRA, if they are in scope of the SRA files,
        which have been processed with nf-core/rnaseq

        :param inpath_sra_meta_csv: Path to directory, where the SRA metadata has been saved
        :param used_accessions: SRA accessions, which have been used for analysis with rnaseq
        :return: DataFrame containing all processed metadata from SRA
        """
        sra_csv_files = glob.glob(inpath_sra_meta_csv + "/*.csv")
        sra_files_list = []
        for file in sra_csv_files:
            df = pd.read_csv(file, sep=',')
            sra_files_list.append(df)
        df = pd.concat(sra_files_list, axis=0)
        df = df[df['Run'].isin(used_accessions)]
        df = df.rename(columns=ListDict.sra_col_dict)
        df['portal'] = "SRA"
        return df

    @staticmethod
    def concat_gdc_meta(inpath_gdc_meta):
        """This method concatenates metadata, downloaded from GDC

        :param inpath_gdc_meta: Path to directory, where GDC metadata has been saved
        :return: DataFrame containing information about GDC metadata
        """
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

    @staticmethod
    def concat_icgc_meta(metadata_dest, path_to_dcc_manifest):
        """This method joins metadata from ICGC DCC (Donor, Sample, Project, Repository)
        based on their matching columns

        :param metadata_dest: Path to directory, where metadata has been downloaded to
        :param path_to_dcc_manifest: Path to ICGC DCC manifest file
        :return: returns DataFrames merged repository and manifest, sample, donor
        """
        icgc_meta_path = os.path.join(metadata_dest, 'icgc')
        try:
            icgc_tsv_files = glob.glob(icgc_meta_path + "/*.tsv")
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

            donor_df = pd.concat(icgc_donor, axis=0)
            sample_df = pd.concat(icgc_sample, axis=0)
            specimen_df = pd.concat(icgc_specimen, axis=0)
            repository_df = pd.concat(icgc_repository, axis=0)
            repository_df = repository_df.rename(columns=ListDict.icgc_col_names_repository)

            manifest_df = pd.read_csv(path_to_dcc_manifest,
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

    @staticmethod
    def stringtie_results_out(res_inpath):
        """This method extracts information from nf-core/rnaseq's output

        :param res_inpath: Path to rna-seq results folder
        :return: returns list of DataFrames containing information from StringTie
        """

        ddf_data = []
        files = glob.glob(res_inpath + "/*.txt")
        for file in files:
            dd_frame = dd.read_csv(file, sep='\t', dtype={'Reference': 'object'}).rename(
                columns=ListDict.stringtie_dict)
            dd_frame = dd_frame.drop(['start_pos', 'end_pos'], axis=1)
            dd_frame['strand'] = dd_frame['strand'].astype('object')
            dd_frame['reference'] = dd_frame['reference'].astype('object')
            dd_frame['run'] = Helper.extract_sample_id(file.split('/')[-1])
            dd_frame['run'] = dd_frame['run'].map(lambda x: re.sub(r"-R1", "", x))
            dd_frame['run'] = dd_frame['run'].map(lambda x: re.sub(r"-R2", "", x))
            ddf_data.append(dd_frame)
        return ddf_data

    @staticmethod
    def create_gene_table(ensembl_df, hgnc_df, expression_df, db_dest):
        """This method creates the gene_table csv files

        :param ensembl_df: Ensembl DataFrame
        :param hgnc_df: HUGO DataFrame
        :param expression_df: Expression DataFrame
        :param db_dest: Path to directory, where cdv files should be saved
        :return:
        """
        Log.logger.info("Starting to create gene table")
        start = time.time()
        expression_ddf = expression_df[['gene_id', 'gene_name', 'reference', 'strand']]
        expression_ddf = expression_ddf.drop_duplicates()

        gene = dd.merge(expression_ddf, ensembl_df,
                        on=['gene_id', 'gene_name'],
                        how='left')
        gene = dd.merge(gene, hgnc_df,
                        left_on='gene_name',
                        right_on='symbol',
                        how='left')
        Log.logger.info("Gene table created in {} seconds".format(time.time() - start))

        Helper.create_csv_from_dask(gene, "gene", db_dest)

    @staticmethod
    def create_expression_table(expression_df, raw_count_df, db_dest):
        """This method creates the expression csv file

        :param expression_df: expression DataFrame
        :param raw_count_df: raw_count DataFrame from featureCounts
        :param db_dest: Path to directory, where cdv files should be saved
        :return:
        """
        # expression = expression_df.drop(['reference', 'strand'], axis=1)
        Log.logger.info("Starting to create expression table. This might take a while.")
        start = time.time()
        expression = expression_df[['run', 'gene_id', 'fpkm',
                                    'tpm', 'coverage']]
        expression = expression.groupby(['run', 'gene_id']).sum().reset_index()

        expression = expression.merge(raw_count_df, on=['gene_id', 'run'], how='left')
        Log.logger.info("Expression table created in {} seconds".format(time.time() - start))
        Helper.create_csv_from_dask(expression, "expression", db_dest)

    @staticmethod
    def create_project_layer_tables(sra_meta_df, icgc_meta_df, gdc_meta_df, db_dest):
        """This method creates tables for ProjectLayer

        :param sra_meta_df: Metadata DataFrame from SRA
        :param icgc_meta_df: Metadata DataFrame from ICGC DCC
        :param gdc_meta_df: Metadata DataFrame from GDC
        :param db_dest: Path to directory, where cdv files should be saved
        :return:
        """
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
    def create_countinfo_raw_count_table(df, rnaseq, db_dest):
        """This method creates the countinfo table

        :param df: DataFrame containing some f the countinfo information
        :param rnaseq: Path to reuslts of nf-core/rnaseq
        :param db_dest: Path do directory where files for database should be stored
        :return:
        """
        # TODO: CLEAN UP CODE
        merged_gene_counts = pd.read_csv(os.path.join(rnaseq, "results/featureCounts/merged_gene_counts.txt"),
                                         sep='\t')

        raw_counts = merged_gene_counts
        raw_counts.columns = list(map(lambda x: Helper.extract_sample_id(x), raw_counts.columns))
        raw_counts = raw_counts.drop(['gene_name'], axis=1).set_index(['Geneid'])
        raw_counts = raw_counts.stack().to_frame().reset_index()
        raw_counts = raw_counts.rename(columns={'Geneid': 'gene_id',
                                                'level_1': 'run',
                                                0: 'raw_count'})
        raw_counts['run'] = raw_counts['run'].map(lambda x: re.sub(r"-R1", "", x))
        raw_counts['run'] = raw_counts['run'].map(lambda x: re.sub(r"-R2", "", x))

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
        return raw_counts

    @staticmethod
    def create_pipeline_table(rnaseq, db_dest):
        """This method creates the pipeline table for the database

        :param rnaseq: Path to reuslts of nf-core/rnaseq
        :param db_dest: Path do directory where files for database should be stored
        :return: None
        """
        pipeline = pd.read_csv(os.path.join(rnaseq, 'results/pipeline_info/software_versions.csv'),
                               sep='\t',
                               header=None).T
        pipeline = pipeline.rename(columns=pipeline.iloc[0])
        pipeline = pipeline.iloc[1:]
        pipeline = pipeline.rename(columns={'Trim Galore!': 'Trim_Galore',
                                            'Picard MarkDuplicates': 'Picard_MarkDuplicates',
                                            'nf-core/rnaseq': 'nf_core_rnaseq'})
        Helper.create_csv(pipeline, "pipeline", db_dest)


class ListDict:
    # Dictionaries
    base_path = Path(__file__).parent
    current_dir = os.getcwd()
    with \
            open(os.path.join(base_path, 'dicts_lists/sra_col_dict.json'), 'r') as sra_col, \
            open(os.path.join(base_path, 'dicts_lists/icgc_rep_dict.json'), 'r') as icgc_col_names, \
            open(os.path.join(base_path, 'dicts_lists/column_db_dict.json'), 'r') as db_col, \
            open(os.path.join(base_path, 'dicts_lists/icgc_tissue_dict.json'), 'r') as icgc_tissue, \
            open(os.path.join(base_path, 'dicts_lists/stringtie_out_dict.json'), 'r') as stringtie_dict, \
            open(os.path.join(base_path, 'dicts_lists/gdc_col_dict.json'), 'r') as gdc_cols:
        sra_col_dict = json.load(sra_col)
        icgc_col_names_repository = json.load(icgc_col_names)
        database_columns = json.load(db_col)
        icgc_tissue_dict = json.load(icgc_tissue)
        stringtie_dict = json.load(stringtie_dict)
        gdc_col_dict = json.load(gdc_cols)

    # Database columns
    with \
            open(os.path.join(base_path, 'dicts_lists/icgc_repository_manifdest_col_list.json'), 'r') as rep_man, \
            open(os.path.join(base_path, 'dicts_lists/donor_df_col_list.json'), 'r') as don, \
            open(os.path.join(base_path, 'dicts_lists/sample_df_col_list.json'), 'r') as sam, \
            open(os.path.join(base_path, 'dicts_lists/sra_df_col_list.json'), 'r') as sra_df, \
            open(os.path.join(base_path, 'dicts_lists/donor_db_list.json'), 'r') as donor_db, \
            open(os.path.join(base_path, 'dicts_lists/sample_db_list.json'), 'r') as sample_db, \
            open(os.path.join(base_path, 'dicts_lists/gdc_attr_list.json'), 'r') as gdc_attr:
        rep_man_cols = json.load(rep_man)
        donor_df_cols = json.load(don)
        sample_df_cols = json.load(sam)
        sra_df_cols = json.load(sra_df)
        donor_db_list = json.load(donor_db)
        sample_db_list = json.load(sample_db)
        gdc_attributes = json.load(gdc_attr)


class SQLscripts:
    @staticmethod
    def prepare_populate_script(db_dest):
        """This method creates a populate.sql script based on the given input of db_dest
        
        :param db_dest: Path do directory where files for database are stored
        :return: None
        """
        with open('postgresql_scripts/populate_tables_template.sql', 'r') as sql_file:
            script = sql_file.read()

        script = script.replace("$PATH$", str(db_dest))
        with open('postgresql_scripts/populate_tables.sql', 'w') as file:
            file.write(script)

if __name__ == "__main__":
    sys.exit(main())
