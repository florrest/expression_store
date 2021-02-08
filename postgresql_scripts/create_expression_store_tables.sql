/*
 DROPPING TABLES IF THEY EXIST
 */
DROP TABLE IF EXISTS project CASCADE;
DROP TABLE IF EXISTS expression CASCADE;
DROP TABLE IF EXISTS sample CASCADE;
DROP TABLE IF EXISTS sample_has_expression CASCADE;


/*
 CREATING TABLES
 */
 CREATE TABLE project (
     project_code VARCHAR(30) PRIMARY KEY,
     study VARCHAR(30) NULL,
     sra_study VARCHAR(30) NULL,
     project_id VARCHAR(30) NULL,
     study_pubmed_id FLOAT NULL,
     dbgap_study_accession VARCHAR(10) NULL
 );

CREATE TABLE expression (
    sample_id VARCHAR (100),
    gene_id VARCHAR(25),
    fpkm REAL,
    tpm REAL,
    coverage REAL,
    raw_count INTEGER
);

CREATE TABLE sample (
    run VARCHAR(100),
    sample_id VARCHAR(100),
    project_code VARCHAR(100),
    submitted_sample_id VARCHAR(100),
    icgc_specimen_id VARCHAR(100),
    submitted_specimen_id VARCHAR(100),
    donor_id VARCHAR(100),
    submitted_donor_id VARCHAR(100),
    analyzed_sample_interval VARCHAR(100),
    percentage_cellularity VARCHAR(100),
    level_of_cellularity VARCHAR(100),
    study_specimen_involved_in VARCHAR(100),
    specimen_type VARCHAR(100),
    specimen_type_other VARCHAR(100),
    specimen_interval VARCHAR(100),
    specimen_donor_treatment_type VARCHAR(100),
    specimen_donor_treatment_type_other VARCHAR(100),
    specimen_processing VARCHAR(100),
    specimen_processing_other VARCHAR(100),
    specimen_storage VARCHAR(100),
    specimen_storage_other VARCHAR(100),
    tumour_confirmed VARCHAR(100),
    specimen_biobank VARCHAR(100),
    specimen_biobank_id VARCHAR(100),
    specimen_available VARCHAR(100),
    tumour_histological_type VARCHAR(100),
    tumour_grading_system VARCHAR(100),
    tumour_grade VARCHAR(100),
    tumour_grade_supplemental VARCHAR(100),
    tumour_stage_system VARCHAR(100),
    tumour_stage VARCHAR(100),
    tumour_stage_supplemental VARCHAR(100),
    digital_image_of_stained_section VARCHAR(100),
    study_donor_involved_in VARCHAR(100),
    consent VARCHAR(100),
    repository VARCHAR(100),
    experiemntal_strategy VARCHAR(100),
    assembly_name VARCHAR(100),
    experiment VARCHAR(100),
    project_id VARCHAR(100),
    sample VARCHAR(100),
    sample_type VARCHAR(100),
    sample_name VARCHAR(100),
    source VARCHAR(100),
    disease VARCHAR(100),
    tumour VARCHAR(100),
    affection_status VARCHAR(100),
    analyte_type VARCHAR(100),
    histological_type VARCHAR(100),
    body_site VARCHAR(100),
    center_name VARCHAR(100),
    submission VARCHAR(100)
);

CREATE TABLE sample_has_expression (
    id SERIAL,
    run VARCHAR(100)
)