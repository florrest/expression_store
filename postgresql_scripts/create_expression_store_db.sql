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
DROP TABLE IF EXISTS sample_has_expression CASCADE;

/*
 CREATING TABLES
 */
CREATE TABLE project
(
    project_id            SERIAL,
    project_code          VARCHAR(30) NOT NULL,
    study                 VARCHAR(30) NULL,
    sra_study             VARCHAR(30) NULL,
    dbgap_study_accession VARCHAR(10) NULL,
    PRIMARY KEY (project_id)
);

CREATE TABLE donor
(
    project_id                                   INTEGER,
    project_code                                 VARCHAR(30) NOT NULL,
    donor_id                                     VARCHAR(25) NOT NULL,
    submitted_donor_id                           VARCHAR(25) NULL,
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
    cancer_history_first_degree_relative         VARCHAR(25) NULL,
    PRIMARY KEY (donor_id)
);
CREATE TABLE sample
(
    run_id                              INTEGER,
    project_id                          INTEGER,
    run                                 VARCHAR(100),
    sample_id                           VARCHAR(30) NULL,
    project_code                        VARCHAR(30) NOT NULL,
    submitted_sample_id                 VARCHAR(30) NULL,
    icgc_specimen_id                    VARCHAR(30) NULL,
    submitted_specimen_id               VARCHAR(30) NULL,
    donor_id                            VARCHAR(30) NULL,
    body_site                           VARCHAR(100) NULL,
    submitted_donor_id                  VARCHAR(30) NULL,
    study_specimen_involved_in          VARCHAR(30) NULL,
    specimen_type                       VARCHAR(30) NULL,
    specimen_type_other                 VARCHAR(30) NULL,
    specimen_interval                   VARCHAR(30) NULL,
    specimen_donor_treatment_type       VARCHAR(30) NULL,
    specimen_donor_treatment_type_other VARCHAR(30) NULL,
    specimen_processing                 VARCHAR(50) NULL,
    specimen_processing_other           VARCHAR(30) NULL,
    specimen_storage                    VARCHAR(30) NULL,
    specimen_storage_other              VARCHAR(30) NULL,
    tumour_confirmed                    VARCHAR(30) NULL,
    specimen_biobank                    VARCHAR(30) NULL,
    specimen_biobank_id                 VARCHAR(30) NULL,
    specimen_available                  VARCHAR(30) NULL,
    tumour_histological_type            VARCHAR(30) NULL,
    tumour_grading_system               VARCHAR(30) NULL,
    tumour_grade                        VARCHAR(30) NULL,
    tumour_grade_supplemental           VARCHAR(30) NULL,
    tumour_stage_system                 VARCHAR(30) NULL,
    tumour_stage                        VARCHAR(30) NULL,
    tumour_stage_supplemental           VARCHAR(30) NULL,
    study_donor_involved_in             VARCHAR(30) NULL,
    consent                             VARCHAR(30) NULL,
    repository                          VARCHAR(30) NULL,
    experiemntal_strategy               VARCHAR(30) NULL,
    assembly_name                       VARCHAR(30) NULL,
    experiment                          VARCHAR(30) NULL,
    sample                              VARCHAR(30) NULL,
    sample_type                         VARCHAR(30) NULL,
    sample_name                         VARCHAR(30) NULL,
    source                              VARCHAR(30) NULL,
    disease                             VARCHAR(30) NULL,
    tumour                              VARCHAR(30) NULL,
    analyte_type                        VARCHAR(30) NULL,
    histological_type                   VARCHAR(30) NULL,
    center_name                         VARCHAR(30) NULL,
    portal                              VARCHAR(20) NOT NULL
);

CREATE TABLE expression
(
    run_id        INTEGER,
    run           VARCHAR(100),
    gene_id       VARCHAR(25),
    gene_index_id INTEGER,
    fpkm          REAL,
    tpm           REAL,
    coverage      REAL,
    raw_count     INTEGER
);

CREATE TABLE gene
(
    gene_index_id SERIAL,
    gene_id       VARCHAR(24),
    gene_name     VARCHAR(24),
    reference     VARCHAR(100),
    strand        VARCHAR(100),
    gene_version  VARCHAR(100),
    gene_biotype  VARCHAR(100),
    hgnc_id       VARCHAR(100),
    symbol        VARCHAR(100),
    locus_group   VARCHAR(100),
    locus_type    VARCHAR(100),
    gene_family   VARCHAR(1000),
    PRIMARY KEY (gene_index_id)
);

CREATE TABLE countinfo
(
    run_id            INTEGER,
    run               VARCHAR(100),
    sum_counts        VARCHAR(100),
    library_name      VARCHAR(100),
    library_strategy  VARCHAR(100),
    library_selection VARCHAR(100),
    library_source    VARCHAR(100),
    library_layout    VARCHAR(100),
    PRIMARY KEY (run)
);

CREATE TABLE pipeline
(
    pipeline_id           SERIAL,
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
    MultiQC               VARCHAR(20),
    PRIMARY KEY (pipeline_id)
);

CREATE TABLE sample_has_expression
(
    run_id      SERIAL,
    pipeline_id INTEGER,
    run         VARCHAR(100),
    PRIMARY KEY (run_id)
);

/*
DEFINING CONSTRAINTS
*/
ALTER TABLE donor
    ADD CONSTRAINT fk_donor
        FOREIGN KEY (project_id)
            REFERENCES project (project_id);

ALTER TABLE sample
    ADD CONSTRAINT fk_sample_donor
        FOREIGN KEY (donor_id)
            REFERENCES donor (donor_id);

ALTER TABLE sample
    ADD CONSTRAINT fk_sample_project
        FOREIGN KEY (project_id)
            REFERENCES project (project_id);

ALTER TABLE expression
    ADD CONSTRAINT fk_expression_gene
        FOREIGN KEY (gene_index_id)
            REFERENCES gene (gene_index_id);

ALTER TABLE expression
    ADD CONSTRAINT fk_expression_she
        FOREIGN KEY (run_id)
            REFERENCES sample_has_expression (run_id);

ALTER TABLE sample
    ADD CONSTRAINT fk_sample_she
        FOREIGN KEY (run_id)
            REFERENCES sample_has_expression (run_id);

ALTER TABLE countinfo
    ADD CONSTRAINT fk_countinfo_she
        FOREIGN KEY (run_id)
            REFERENCES sample_has_expression (run_id);
