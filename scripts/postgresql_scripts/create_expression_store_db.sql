DROP
DATABASE IF EXISTS expression_store;
CREATE
DATABASE expression_store;

/*
 DROPPING TABLES IF THEY EXIST
 */
DROP TABLE IF EXISTS project CASCADE;
DROP TABLE IF EXISTS donor CASCADE;
DROP TABLE IF EXISTS sample CASCADE;
DROP TABLE IF EXISTS expression CASCADE;
DROP TABLE IF EXISTS gene CASCADE;
DROP TABLE IF EXISTS countinfo CASCADE;
DROP TABLE IF EXISTS pipeline CASCADE;

/*
 CREATING TABLES
 */
CREATE TABLE project
(
    project_code          VARCHAR(30) NOT NULL,
    study                 VARCHAR(30) NULL,
    sra_study             VARCHAR(30) NULL,
    dbgap_study_accession VARCHAR(10) NULL
);

CREATE TABLE donor
(
    project_code                                 VARCHAR(30) NOT NULL,
    donor_id                                     VARCHAR(25) NOT NULL,
    donor_sex                                    VARCHAR(25) NULL,
    donor_vital_status                           VARCHAR(25) NULL,
    disease_status_last_followup                 VARCHAR(25) NULL,
    donor_relapse_type                           VARCHAR(25) NULL,
    donor_age_at_diagnosis                       VARCHAR(25) NULL,
    donor_age_at_enrollment                      VARCHAR(25) NULL,
    donor_age_at_last_followup                   VARCHAR(25) NULL,
    donor_relapse_interval                       VARCHAR(25) NULL,
    donor_diagnosis_icd10                        VARCHAR(25) NULL,
    donor_tumour_staging_system_at_diagnosis     VARCHAR(25) NULL,
    donor_tumour_stage_at_diagnosis              VARCHAR(25) NULL,
    donor_tumour_stage_at_diagnosis_supplemental VARCHAR(25) NULL,
    donor_survival_time                          VARCHAR(25) NULL,
    donor_interval_of_last_followup              VARCHAR(25) NULL,
    prior_malignancy                             VARCHAR(25) NULL,
    cancer_type_prior_malignancy                 VARCHAR(25) NULL,
    cancer_history_first_degree_relative         VARCHAR(25) NULL
);
CREATE TABLE sample
(
    run                                 VARCHAR(100),
    sample_id                           VARCHAR(30) NULL,
    project_code                        VARCHAR(30) NOT NULL,
    icgc_specimen_id                    VARCHAR(30) NULL,
    donor_id                            VARCHAR(30) NULL,
    body_site                           VARCHAR(100) NULL,
    study_specimen_involved_in          VARCHAR(30) NULL,
    specimen_type                       VARCHAR(30) NULL,
    specimen_type_other                 VARCHAR(30) NULL,
    specimen_donor_treatment_type       VARCHAR(30) NULL,
    specimen_donor_treatment_type_other VARCHAR(30) NULL,
    tumour_confirmed                    VARCHAR(30) NULL,
    tumour_grading_system               VARCHAR(30) NULL,
    tumour_grade                        VARCHAR(30) NULL,
    tumour_grade_supplemental           VARCHAR(30) NULL,
    tumour_stage_system                 VARCHAR(30) NULL,
    tumour_stage                        VARCHAR(30) NULL,
    tumour_stage_supplemental           VARCHAR(30) NULL,
    consent                             VARCHAR(30) NULL,
    repository                          VARCHAR(30) NULL,
    sra_experiment                      VARCHAR(30) NULL,
    sra_sample                          VARCHAR(30) NULL,
    sample_type                         VARCHAR(30) NULL,
    sample_name                         VARCHAR(30) NULL,
    source                              VARCHAR(30) NULL,
    disease                             VARCHAR(30) NULL,
    center_name                         VARCHAR(30) NULL,
    sra_submission                          VARCHAR(30) NULL,
    portal                              VARCHAR(20) NOT NULL
);

CREATE TABLE expression
(
    run           VARCHAR(100),
    gene_id       VARCHAR(25),
    fpkm          REAL,
    tpm           REAL,
    coverage      REAL,
    raw_count     INTEGER
);

CREATE TABLE gene
(
    gene_id       VARCHAR(25),
    gene_name     VARCHAR(25),
    reference     VARCHAR(30),
    strand        VARCHAR(5),
    gene_version  VARCHAR(30),
    gene_biotype  VARCHAR(30),
    hgnc_id       VARCHAR(20),
    symbol        VARCHAR(30),
    locus_group   VARCHAR(20),
    locus_type    VARCHAR(30),
    gene_family   VARCHAR(1000)
);

CREATE TABLE countinfo
(
    run               VARCHAR(100),
    sum_counts        INTEGER,
    library_name      VARCHAR(100),
    library_strategy  VARCHAR(25),
    library_selection VARCHAR(25),
    library_source    VARCHAR(25),
    library_layout    VARCHAR(25)
);

CREATE TABLE pipeline
(
    nf_core_rnaseq        VARCHAR(10),
    Nextflow              VARCHAR(10),
    FastQC                VARCHAR(10),
    Cutadapt              VARCHAR(10),
    Trim_Galore           VARCHAR(10),
    SortMeRNA             VARCHAR(10),
    STAR                  VARCHAR(20),
    HISAT2                VARCHAR(10),
    Picard_MarkDuplicates VARCHAR(10),
    Samtools              VARCHAR(10),
    featureCounts         VARCHAR(10),
    Salmon                VARCHAR(10),
    StringTie             VARCHAR(10),
    Preseq                VARCHAR(10),
    deepTools             VARCHAR(10),
    RSeQC                 VARCHAR(10),
    dupRadar              VARCHAR(10),
    edgeR                 VARCHAR(10),
    Qualimap              VARCHAR(20),
    MultiQC               VARCHAR(20)
);