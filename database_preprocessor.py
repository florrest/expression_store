import click
import glob
import gzip
import logging
import os
import re
import requests
import shutil
import subprocess
import sys
import time
import urllib.request

import dask.dataframe as dd
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
    start = time.time()
    logger.info('Filter Projects and download metadata from ICGC DCC')
    download_icgc_project_metadata(get_icgc_project_list(inpath), inpath)
    gunzip_files(os.path.join(inpath, "metadata/icgc"))
    icgc_meta_df = join_icgc_meta(concat_icgc_meta(os.path.join(inpath, "metadata/icgc")), inpath)

    logger.info('Download Metadata from Sequence Read Archive (SRA)')
    download_sra_metadata(get_sra_accession_list(inpath), inpath)

    # logger.info('Download Metadata from GDC')
    # gdc_uuid_list = get_gdc_uuid_list(inpath)
    # download_gdc_metadata(gdc_uuid_list, inpath)

    logger.info('Creating CSV to load into database')
    expression_df = dd.concat(stringtie_results_out(os.path.join(inpath, 'results/stringtieFPKM/')))
    create_gene_table(download_and_process_ensembl_metadata(inpath),
                      download_hgnc(inpath),
                      expression_df)
    create_expression_table(expression_df)
    create_project_layer_tables(concat_sra_meta(os.path.join(inpath, "metadata/sra"), get_sra_accession_list(inpath)),
                                join_icgc_meta(concat_icgc_meta(os.path.join(inpath, "metadata/icgc")), inpath))

    print(time.time() - start)


#
# HELPER FUNCTIONS
#

# Return unzipped gz files and delete gz files
def gunzip_files(inpath):
    gz_files = glob.glob(inpath + "/*.gz")
    for gz in gz_files:
        with gzip.open(gz, 'rb') as f_in:
            with open(gz[:-3], 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(gz)


# url request to download data
def retrieve_data(url, destination):
    urllib.request.urlretrieve(url, destination)


# extract substring from a given string if it matches a pattern otherwise return original string
def extract_run_id(string):
    string = string.split('.')[1]
    if string.startswith("SWID"):
        new = re.sub(r'_R._00.', "", string)
        return new
    else:
        return string

# extract substring from a given string if it matches a pattern otherwise return original string
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

#
# Get identifiers and codes for downloaded sequence data
#

# returns ICGC DCC project list from given manifest file
def get_icgc_project_list(inpath):
    icgc_project_codes = pd.DataFrame()
    try:
        os.chdir(inpath)
        icgc_project_codes = pd.read_csv('./manifest.tsv', sep='\t', usecols=['project_id/project_count'])
        icgc_project_codes = icgc_project_codes['project_id/project_count'].unique().tolist()
    except:
        print("Not a directory")

    return icgc_project_codes


# returns list of SRA accessions based on acc_list.txt (as used by S. Lemkes SRADownloader
def get_sra_accession_list(inpath):
    sra_accessions = pd.DataFrame()
    try:
        os.chdir(inpath)
        sra_accessions = pd.read_csv('./acc_list.txt', header=None)
        sra_accessions = sra_accessions[0].unique().tolist()
    except:
        print("Not a dirctory")

    return sra_accessions


# returns list of gdc uuids from given gdc manifest file
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


#
# Download data
#

# download runInfo.csv for given accession
def download_sra_metadata(project_lst, inpath):
    if not os.path.exists(os.path.join(inpath, 'metadata/sra/')):
        os.makedirs(os.path.join(inpath, 'metadata/sra/'))
    for i in project_lst:
        sra_url = 'http://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?save=efetch&db=sra&rettype=runinfo&term={}'.format(
            i)
        retrieve_data(sra_url, os.path.join(inpath, 'metadata/sra/') + str(i) + '.csv')


# download sample, specimen, donor and donor_set metadata from ICGC DCC based on project code
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
        time.sleep(0.01)
        retrieve_data(sample_url, inpath + '/metadata/icgc/sample_' + str(i) + '.tsv.gz')
        time.sleep(0.01)
        retrieve_data(donor_url, inpath + '/metadata/icgc/donor_' + str(i) + '.tsv.gz')
        time.sleep(0.01)
        retrieve_data(specimen_url, inpath + '/metadata/icgc/specimen_' + str(i) + '.tsv.gz')
        time.sleep(0.01)
        retrieve_data(donor_set_url, inpath + '/metadata/icgc/repository_' + str(i) + '.tsv')
        time.sleep(0.01)

### UNDER CONSTRUCTION
def download_gdc_metadata(id_lst, inpath):
    if not os.path.exists(os.path.join(inpath, 'metadata/gdc/')):
        os.makedirs(os.path.join(inpath, 'metadata/gdc/'))
    for i in id_lst:
        url = 'https://api.gdc.cancer.gov/cases/' + str(i) + '?fields=submitter_id?pretty=true&format=TSV'
        retrieve_data(url, inpath + '/test/' + str(i) + '_sample.tsv')

### UNDER CONSTRUCTION
def download_gdc_metadata_test():
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


# download and process ensembl metadata, so it just returns relevant gene entries
def download_and_process_ensembl_metadata(inpath):
    try:
        if not os.path.exists(os.path.join(inpath, 'metadata/ensemble/')):
            os.makedirs(os.path.join(inpath, 'metadata/ensembl/'))
    except:
        logger.info('exists')

    ensembl_url = 'ftp://ftp.ensembl.org/pub/' \
                  'grch37/current/gtf/homo_sapiens/Homo_sapiens.GRCh37.87.gtf.gz'
    retrieve_data(ensembl_url, inpath + '/metadata/ensembl/'
                                        'Homo_sapiens.GRCh37.87.gtf.gz')
    gunzip_files(os.path.join(inpath, "metadata/ensembl"))
    cwd = os.getcwd()
    os.chdir(os.path.join(inpath, "metadata/ensembl"))
    subprocess.call('''awk -F"\t" '$3 == "gene" { print $0 }' Homo_sapiens.GRCh37.87.gtf > genes_GRCh37.gtf''',
                    shell=True)
    os.remove('Homo_sapiens.GRCh37.87.gtf')
    ensembl_df = pr.read_gtf('genes_GRCh37.gtf',
                             as_df=True)
    ensembl_df = ensembl_df[['gene_id', 'gene_version', 'gene_name', 'gene_biotype']]
    os.chdir(cwd)
    return ensembl_df


# download hgnc metadata and return a dataframe containing all relevant information
def download_hgnc(inpath):
    if not os.path.exists(os.path.join(inpath, 'metadata/hgnc/')):
        os.makedirs(os.path.join(inpath, 'metadata/hgnc/'))
    hgnc_url = 'ftp://ftp.ebi.ac.uk/pub/databases/genenames/hgnc/tsv/hgnc_complete_set.txt'
    retrieve_data(hgnc_url, inpath + '/metadata/hgnc/hgnc_complete_set.txt')
    hgnc_df = pd.read_csv(os.path.join(inpath, 'metadata/hgnc/hgnc_complete_set.txt'),
                          sep='\t',
                          usecols=['hgnc_id', 'symbol', 'locus_group', 'locus_type', 'gene_family'])
    return hgnc_df

#
# Preparing data
#

# concat metadata from downloaded SRA runInfo.csv
def concat_sra_meta(inpath_sra_meta_csv, used_accessions):
    sra_csv_files = glob.glob(inpath_sra_meta_csv + "/*.csv")
    sra_files_list = []
    for file in sra_csv_files:
        df = pd.read_csv(file, sep=',')
        sra_files_list.append(df)
    frame = pd.concat(sra_files_list, axis=0)
    frame = frame[frame['Run'].isin(used_accessions)]
    frame = frame.rename(columns=sra_col_dict)
    return frame


# concat metadatafrom downloaded ICGC DCC metadata files
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


# join and return dataframes containing metadata about ICGC DCC sequences
def join_icgc_meta(df, base_path):
    donor_df = df[0]
    sample_df = df[1]
    specimen_df = df[2]
    repository_df = df[3].rename(columns=icgc_col_names_repository)

    manifest_df = pd.read_csv(os.path.join(base_path, './manifest.tsv'),
                              sep='\t', usecols=['file_id', 'file_name'])

    merge_rep_man = pd.merge(repository_df, manifest_df,
                             on=['file_id', 'file_name'],
                             how='right')
    merge_rep_man['file_name'] = merge_rep_man['file_name'].apply(lambda x: extract_run_id(x))
    sample_df = pd.merge(sample_df, specimen_df,
                         on=['icgc_specimen_id', 'icgc_donor_id', 'project_code', 'submitted_donor_id',
                             'submitted_specimen_id', 'percentage_cellularity', 'level_of_cellularity'],
                         how='left')

    return merge_rep_man, donor_df, sample_df


# process stringtie output from nf-core/rna-seq pipeline and return dataframe containing expression data
def stringtie_results_out(res_inpath):
    data = []
    files = glob.glob(res_inpath + "/*.txt")
    for file in files:
        frame = pd.read_csv(file, sep='\t')
        # frame['Feature'] = frame['Feature'].astype('object')
        frame['Strand'] = frame['Strand'].astype('object')
        # frame = frame[(frame['Coverage'] != 0.0) & (
        #    frame['TPM'] != 0.0) & (frame['FPKM'] != 0.0)]
        frame['sample_id'] = extract_sample_id(file.split('/')[-1])
        # frame['id'] = frame['sample_id'].map(sample_id_dict)
        frame = frame.rename(
            columns={
                'Gene ID': 'gene_id',
                'Gene Name': 'gene_name',
                'FPKM': 'fpkm',
                'TPM': 'tpm',
                'Coverage': 'coverage',
                'Start': 'start_pos',
                'End': 'end_pos',
                'Reference': 'reference',
                'Strand': 'strand'})
        dd_frame = dd.from_pandas(frame, npartitions=2)
        data.append(dd_frame)

    return data


# Create TABLES
def create_gene_table(ensembl_df, hgnc_df, expression_df):
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
    return gene


def create_expression_table(expression_df):
    expression = expression_df[['sample_id', 'gene_id', 'fpkm',
                                'tpm', 'coverage']]
    expression = expression.groupby(['sample_id', 'gene_id']).sum().reset_index().compute()
    return expression


def create_project_layer_tables(sra_meta_df, icgc_meta_df):
    sra = sra_meta_df.rename(columns=database_columns)
    rep_man = icgc_meta_df[0].rename(columns=database_columns)
    donor = icgc_meta_df[1].rename(columns=database_columns)
    sample = icgc_meta_df[2].rename(columns=database_columns)

    merged_sample_donor = pd.merge(sample, donor, on=['donor_id', 'project_code', 'submitted_donor_id'], how='left')
    final_merge = pd.merge(merged_sample_donor, rep_man,
                           on=['donor_id', 'icgc_specimen_id', 'project_code', 'specimen_type', 'sample_id'],
                           how='right')

    concat_meta = pd.concat([final_merge, sra], axis=0)

    project_db = concat_meta[['project_code', 'study', 'Study', 'sra_study', 'project_id',
                              'study_pubmed_id', 'dbgap_study_accession']]
    project_db = project_db.drop_duplicates()

    donor_db = concat_meta[['project_code', 'donor_id', 'submitted_donor_id', 'donor_sex',
                            'donor_vital_status', 'disease_status_last_followup', 'donor_relapse_type',
                            'donor_age_at_diagnosis', 'donor_age_at_enrollment', 'donor_age_at_last_followup',
                            'donor_relapse_interval', 'donor_diagnosis_icd10',
                            'donor_tumour_staging_system_at_diagnosis', 'donor_tumour_stage_at_diagnosis',
                            'donor_tumour_stage_at_diagnosis_supplemental', 'donor_survival_time',
                            'donor_interval_of_last_followup', 'prior_malignancy', 'cancer_type_prior_malignancy',
                            'cancer_history_first_degree_relative', 'subject_id']]

    sample_db = concat_meta[['run', 'sample_id', 'project_code', 'submitted_sample_id', 'icgc_specimen_id',
                             'submitted_specimen_id', 'donor_id', 'submitted_donor_id', 'analyzed_sample_interval',
                             'percentage_cellularity', 'level_of_cellularity', 'study_specimen_involved_in',
                             'specimen_type', 'specimen_type_other', 'specimen_interval',
                             'specimen_donor_treatment_type', 'specimen_donor_treatment_type_other',
                             'specimen_processing', 'specimen_processing_other', 'specimen_storage',
                             'specimen_storage_other', 'tumour_confirmed', 'specimen_biobank',
                             'specimen_biobank_id', 'specimen_available', 'tumour_histological_type',
                             'tumour_grading_system', 'tumour_grade', 'tumour_grade_supplemental',
                             'tumour_stage_system', 'tumour_stage', 'tumour_stage_supplemental',
                             'digital_image_of_stained_section', 'study_donor_involved_in',
                             'consent', 'repository', 'experiemntal_strategy', 'assembly_name',
                             'experiment', 'project_id', 'sample', 'sample_type',
                             'sample_name', 'source', 'disease', 'tumour', 'affection_status', 'analyte_type',
                             'histological_type', 'body_site', 'center_name', 'submission']]
    sample_db['body_site'] = sample_db['body_site'] \
        .fillna(sample_db['project_code'].astype(str).map(icgc_tissue_dict))

    countinfo_db = concat_meta[['library_name', 'library_strategy', 'library_selection',
                                'library_source', 'library_layout']]

    return project_db, donor_db, sample_db, countinfo_db


# Dictionaries
icgc_col_names_repository = {'Access': 'access',
                             'File ID': 'file_id',
                             'Object ID': 'object_id',
                             'File Name': 'file_name',
                             'ICGC Donor': 'icgc_donor_id',
                             'Specimen ID': 'icgc_specimen_id',
                             'Specimen Type': 'specimen_type',
                             'Sample ID': 'icgc_sample_id',
                             'Repository': 'repository',
                             'Project': 'project_code',
                             'Data Type': 'data_type',
                             'Experimental Strategy': 'experiemntal_strategy',
                             'Format': 'format',
                             'Size (bytes)': 'size_bytes'}
sra_col_dict = {'Run': 'run',
                'ReleaseDate': 'release_date',
                'LoadDate': 'load_date',
                'avgLength': 'avg_length',
                'AssemblyName': 'assembly_name',
                'Experiment': 'experiment',
                'LibraryName': 'library_name',
                'LibraryStrategy': 'library_strategy',
                'LibrarySelection': 'library_selection',
                'LibrarySource': 'library_source',
                'LibraryLayout': 'library_layout',
                'InsertSize': 'insert_size',
                'InsertDev': 'insert_dev',
                'Platform': 'plattform',
                'Model': 'model',
                'SRAStudy': 'sra_study',
                'BioProject': 'project_code',
                'Study_Pubmed_id': 'study_pubmed_id',
                'ProjectID': 'project_id',
                'Sample': 'sample',
                'BioSample': 'sample_id',
                'SampleType': 'sample_type',
                'SampleName': 'sample_name',
                'Subject_ID': 'subject_id',
                'Sex': 'sex',
                'Disease': 'disease',
                'Tumor': 'tumour',
                'Affection_Status': 'affection_status',
                'Analyte_Type': 'analyte_type',
                'Histological_Type': 'histological_type',
                'Body_Site': 'body_site',
                'CenterName': 'center_name',
                'Submission': 'submission',
                'Consent': 'consent',
                'RunHash': 'run_hash',
                'ReadHash': 'read_hash'}

database_columns = {'icgc_donor_id': 'donor_id',
                    'icgc_sample_id': 'sample_id',
                    'sex': 'donor_sex',
                    'bio_sample': 'sample_id',
                    'bio_project': 'project_id',
                    'access': 'consent',
                    'file_name': 'run'
                    }
icgc_tissue_dict = {
    'MALY-DE': 'Blood',
    'PACA-CA': 'Pancreas'
}
if __name__ == "__main__":
    sys.exit(main())
