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
    project_code          VARCHAR(30),
    study                 VARCHAR(30) NULL,
    sra_study             VARCHAR(30) NULL,
    project_id            VARCHAR(30) NULL,
    study_pubmed_id       FLOAT NULL,
    dbgap_study_accession VARCHAR(10) NULL,
    PRIMARY KEY (project_code)
);

CREATE TABLE donor
(
    project_code                                 VARCHAR(100),
    donor_id                                     VARCHAR(100),
    submitted_donor_id                           VARCHAR(100),
    donor_sex                                    VARCHAR(100),
    donor_vital_status                           VARCHAR(100),
    disease_status_last_followup                 VARCHAR(100),
    donor_relapse_type                           VARCHAR(100),
    donor_age_at_diagnosis                       VARCHAR(100),
    donor_age_at_enrollment                      VARCHAR(100),
    donor_age_at_last_followup                   VARCHAR(100),
    donor_relapse_interval                       VARCHAR(100),
    donor_diagnosis_icd10                        VARCHAR(100),
    donor_tumour_staging_system_at_diagnosis     VARCHAR(100),
    donor_tumour_stage_at_diagnosis              VARCHAR(100),
    donor_tumour_stage_at_diagnosis_supplemental VARCHAR(100),
    donor_survival_time                          VARCHAR(100),
    donor_interval_of_last_followup              VARCHAR(100),
    prior_malignancy                             VARCHAR(100),
    cancer_type_prior_malignancy                 VARCHAR(100),
    cancer_history_first_degree_relative         VARCHAR(100),
    PRIMARY KEY (donor_id)
);
CREATE TABLE sample
(
    run_id                              INTEGER,
    run                                 VARCHAR(100),
    sample_id                           VARCHAR(100),
    project_code                        VARCHAR(100),
    submitted_sample_id                 VARCHAR(100),
    icgc_specimen_id                    VARCHAR(100),
    submitted_specimen_id               VARCHAR(100),
    donor_id                            VARCHAR(100),
    submitted_donor_id                  VARCHAR(100),
    analyzed_sample_interval            VARCHAR(100),
    percentage_cellularity              VARCHAR(100),
    level_of_cellularity                VARCHAR(100),
    study_specimen_involved_in          VARCHAR(100),
    specimen_type                       VARCHAR(100),
    specimen_type_other                 VARCHAR(100),
    specimen_interval                   VARCHAR(100),
    specimen_donor_treatment_type       VARCHAR(100),
    specimen_donor_treatment_type_other VARCHAR(100),
    specimen_processing                 VARCHAR(100),
    specimen_processing_other           VARCHAR(100),
    specimen_storage                    VARCHAR(100),
    specimen_storage_other              VARCHAR(100),
    tumour_confirmed                    VARCHAR(100),
    specimen_biobank                    VARCHAR(100),
    specimen_biobank_id                 VARCHAR(100),
    specimen_available                  VARCHAR(100),
    tumour_histological_type            VARCHAR(100),
    tumour_grading_system               VARCHAR(100),
    tumour_grade                        VARCHAR(100),
    tumour_grade_supplemental           VARCHAR(100),
    tumour_stage_system                 VARCHAR(100),
    tumour_stage                        VARCHAR(100),
    tumour_stage_supplemental           VARCHAR(100),
    digital_image_of_stained_section    VARCHAR(100),
    study_donor_involved_in             VARCHAR(100),
    consent                             VARCHAR(100),
    repository                          VARCHAR(100),
    experiemntal_strategy               VARCHAR(100),
    assembly_name                       VARCHAR(100),
    experiment                          VARCHAR(100),
    project_id                          VARCHAR(100),
    sample                              VARCHAR(100),
    sample_type                         VARCHAR(100),
    sample_name                         VARCHAR(100),
    source                              VARCHAR(100),
    disease                             VARCHAR(100),
    tumour                              VARCHAR(100),
    affection_status                    VARCHAR(100),
    analyte_type                        VARCHAR(100),
    histological_type                   VARCHAR(100),
    body_site                           VARCHAR(100),
    center_name                         VARCHAR(100),
    submission                          VARCHAR(100),
    PRIMARY KEY (run)
);

CREATE TABLE expression
(
    run_id    INTEGER,
    sample_id VARCHAR(100),
    gene_id   VARCHAR(25),
    fpkm      REAL,
    tpm       REAL,
    coverage  REAL,
    raw_count INTEGER,
    PRIMARY KEY (sample_id, gene_id)
);

CREATE TABLE gene
(
    gene_id      VARCHAR(100),
    gene_name    VARCHAR(100),
    reference    VARCHAR(100),
    strand       VARCHAR(100),
    gene_version VARCHAR(100),
    gene_biotype VARCHAR(100),
    hgnc_id      VARCHAR(100),
    symbol       VARCHAR(100),
    locus_group  VARCHAR(100),
    locus_type   VARCHAR(100),
    gene_family  VARCHAR(1000),
    PRIMARY KEY (gene_id)
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
    nf_core_rnaseq        VARCHAR(100),
    Nextflow              VARCHAR(100),
    FastQC                VARCHAR(100),
    Cutadapt              VARCHAR(100),
    Trim_Galore           VARCHAR(100),
    SortMeRNA             VARCHAR(100),
    STAR                  VARCHAR(100),
    HISAT2                VARCHAR(100),
    Picard_MarkDuplicates VARCHAR(100),
    Samtools              VARCHAR(100),
    featureCounts         VARCHAR(100),
    Salmon                VARCHAR(100),
    StringTie             VARCHAR(100),
    Preseq                VARCHAR(100),
    deepTools             VARCHAR(100),
    RSeQC                 VARCHAR(100),
    dupRadar              VARCHAR(100),
    edgeR                 VARCHAR(100),
    Qualimap              VARCHAR(100),
    MultiQC               VARCHAR(100),
    PRIMARY KEY (pipeline_id)
);

CREATE TABLE sample_has_expression
(
    run_id      SERIAL,
    pipeline_id SERIAL,
    run         VARCHAR(100),
    PRIMARY KEY (run_id)
);

/*
DEFINING CONSTRAINTS
*/
ALTER TABLE donor
    ADD CONSTRAINT fk_donor
        FOREIGN KEY (project_code)
            REFERENCES project (project_code);

ALTER TABLE sample
    ADD CONSTRAINT fk_sample_donor
        FOREIGN KEY (donor_id)
            REFERENCES donor (donor_id);

ALTER TABLE sample
    ADD CONSTRAINT fk_sample_project
        FOREIGN KEY (project_code)
            REFERENCES project (project_code);

ALTER TABLE expression
    ADD CONSTRAINT fk_expression_gene
        FOREIGN KEY (gene_id)
            REFERENCES gene (gene_id);

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
