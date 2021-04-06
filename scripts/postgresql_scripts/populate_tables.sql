DROP TABLE IF EXISTS sample_has_expression CASCADE;

ALTER TABLE project
    DROP COLUMN project_id CASCADE;

ALTER TABLE gene
    DROP COLUMN gene_index_id CASCADE;

ALTER TABLE pipeline
    DROP COLUMN pipeline_id CASCADE;

ALTER TABLE donor
    DROP COLUMN project_id CASCADE;

ALTER TABLE sample
    DROP COLUMN run_id,
    DROP COLUMN project_id CASCADE;

ALTER TABLE expression
    DROP COLUMN run_id,
    DROP COLUMN gene_index_id CASCADE;

ALTER TABLE countinfo
    DROP COLUMN run_id CASCADE;

CREATE TABLE tmp_gene (LIKE gene INCLUDING ALL);
CREATE TABLE tmp_project (LIKE project INCLUDING ALL);
CREATE TABLE tmp_donor (LIKE donor INCLUDING ALL);
CREATE TABLE tmp_sample (LIKE sample INCLUDING ALL);
CREATE TABLE tmp_expression (LIKE expression INCLUDING ALL);
CREATE TABLE tmp_countinfo (LIKE countinfo INCLUDING ALL);
CREATE TABLE tmp_pipeline (LIKE pipeline INCLUDING ALL);
/*
 Inserting into expression table, if data not already exists
*/
COPY tmp_gene (gene_id,gene_name,reference,strand,gene_version,
    gene_biotype,hgnc_id,symbol,locus_group,locus_type,gene_family)
    FROM '/home/flo/Schreibtisch/test_folder/gene.csv'
    DELIMITER ','
    CSV HEADER;
/*
 Inserting into PROJECT table, if data not already exists
*/
COPY tmp_project(project_code,study,sra_study,dbgap_study_accession)
    FROM '/home/flo/Schreibtisch/test_folder/project.csv'
    DELIMITER ','
    CSV HEADER;
/*
 Inserting into DONOR table, if data not already exists
*/
COPY tmp_donor(project_code,donor_id,donor_sex,donor_vital_status,disease_status_last_followup,donor_relapse_type,
           donor_age_at_diagnosis,donor_age_at_enrollment,donor_age_at_last_followup,donor_relapse_interval,
           donor_diagnosis_icd10,donor_tumour_staging_system_at_diagnosis,donor_tumour_stage_at_diagnosis,
           donor_tumour_stage_at_diagnosis_supplemental,donor_survival_time,donor_interval_of_last_followup,
           prior_malignancy,cancer_type_prior_malignancy,cancer_history_first_degree_relative)
    FROM '/home/flo/Schreibtisch/test_folder/donor.csv'
    DELIMITER ','
    CSV HEADER;

/*
 Inserting into sample table, if data not already exists
*/

COPY tmp_sample(run,sample_id,project_code,icgc_specimen_id,donor_id,body_site,study_specimen_involved_in,specimen_type,
            specimen_type_other,specimen_donor_treatment_type,specimen_donor_treatment_type_other,tumour_confirmed,
            tumour_grading_system,tumour_grade,tumour_grade_supplemental,tumour_stage_system,tumour_stage,
            tumour_stage_supplemental,consent,repository,sra_experiment,sra_sample,sample_type,sample_name,source,
            disease,center_name,sra_submission,portal)
    FROM '/home/flo/Schreibtisch/test_folder/sample.csv'
    DELIMITER ','
    CSV HEADER;

/*
 Inserting into expression table, if data not already exists
*/

COPY tmp_expression(run,gene_id,fpkm,tpm,coverage,raw_count)
    FROM '/home/flo/Schreibtisch/test_folder/expression.csv'
    DELIMITER ','
    CSV HEADER;


/*
 Inserting into expression table, if data not already exists
*/

COPY tmp_countinfo(run,sum_counts,library_name,library_strategy,library_selection,library_source,library_layout)
    FROM '/home/flo/Schreibtisch/test_folder/countinfo.csv'
    DELIMITER ','
    CSV HEADER;

/*
 Inserting into pipeline table, if data not already exists
*/

COPY tmp_pipeline(nf_core_rnaseq,Nextflow,FastQC,Cutadapt,Trim_Galore,SortMeRNA,STAR,HISAT2,Picard_MarkDuplicates,
    Samtools,featureCounts,Salmon,StringTie,Preseq,deepTools,RSeQC,dupRadar,edgeR,Qualimap,MultiQC)
    FROM '/home/flo/Schreibtisch/test_folder/pipeline.csv'
    DELIMITER ','
    CSV HEADER;

INSERT INTO gene
SELECT *
FROM tmp_gene
EXCEPT
SELECT *
FROM gene;

INSERT INTO project
SELECT *
FROM tmp_project
EXCEPT
SELECT *
FROM project;

INSERT INTO donor
SELECT *
FROM tmp_donor
EXCEPT
SELECT *
FROM donor;

INSERT INTO sample
SELECT *
FROM tmp_sample
EXCEPT
SELECT *
FROM sample;

INSERT INTO expression
SELECT *
FROM tmp_expression
EXCEPT
SELECT *
FROM expression;

INSERT INTO countinfo
SELECT *
FROM tmp_countinfo
EXCEPT
SELECT *
FROM countinfo;

INSERT INTO pipeline
SELECT *
FROM tmp_pipeline
EXCEPT
SELECT *
FROM pipeline;

DROP TABLE IF EXISTS tmp_gene CASCADE;
DROP TABLE IF EXISTS tmp_project CASCADE;
DROP TABLE IF EXISTS tmp_donor CASCADE;
DROP TABLE IF EXISTS tmp_sample CASCADE;
DROP TABLE IF EXISTS tmp_expression CASCADE;
DROP TABLE IF EXISTS tmp_countinfo CASCADE;
DROP TABLE IF EXISTS tmp_pipeline CASCADE;