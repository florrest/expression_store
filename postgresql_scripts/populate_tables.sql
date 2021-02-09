DROP TABLE IF EXISTS sample_has_expression CASCADE;

/*
 Inserting into PROJECT table, if data not already exists
*/

COPY project(project_code,study,sra_study,project_id,study_pubmed_id,dbgap_study_accession)
    FROM '/home/flo/Schreibtisch/test_folder/project.csv'
    DELIMITER ','
    CSV HEADER;
/*
 Inserting into DONOR table, if data not already exists
*/
COPY donor(project_code,donor_id,submitted_donor_id,donor_sex,donor_vital_status,disease_status_last_followup,
    donor_relapse_type,donor_age_at_diagnosis,donor_age_at_enrollment,donor_age_at_last_followup,donor_relapse_interval,
    donor_diagnosis_icd10,donor_tumour_staging_system_at_diagnosis,donor_tumour_stage_at_diagnosis,
    donor_tumour_stage_at_diagnosis_supplemental,donor_survival_time,donor_interval_of_last_followup,
    prior_malignancy,cancer_type_prior_malignancy,cancer_history_first_degree_relative)
    FROM '/home/flo/Schreibtisch/test_folder/donor.csv'
    DELIMITER ','
    CSV HEADER;

/*
 Inserting into sample table, if data not already exists
*/

COPY sample(run,sample_id,project_code,submitted_sample_id,icgc_specimen_id,submitted_specimen_id,donor_id,
    submitted_donor_id,analyzed_sample_interval,percentage_cellularity,level_of_cellularity,study_specimen_involved_in,
    specimen_type,specimen_type_other,specimen_interval,specimen_donor_treatment_type,
    specimen_donor_treatment_type_other,specimen_processing,specimen_processing_other,specimen_storage,
    specimen_storage_other,tumour_confirmed,specimen_biobank,specimen_biobank_id,specimen_available,
    tumour_histological_type,tumour_grading_system,tumour_grade,tumour_grade_supplemental,
    tumour_stage_system,tumour_stage,tumour_stage_supplemental,digital_image_of_stained_section,
    study_donor_involved_in,consent,repository,experiemntal_strategy,assembly_name,experiment,project_id,sample,
    sample_type,sample_name, source,disease,tumour,affection_status,analyte_type,histological_type,body_site,
    center_name,submission)
    FROM '/home/flo/Schreibtisch/test_folder/sample.csv'
    DELIMITER ','
    CSV HEADER;

/*
 Inserting into expression table, if data not already exists
*/

COPY expression (sample_id,gene_id,fpkm,tpm,coverage,raw_count)
    FROM '/home/flo/Schreibtisch/test_folder/expression.csv'
    DELIMITER ','
    CSV HEADER;

/*
 Inserting into expression table, if data not already exists
*/

COPY gene (gene_id,gene_name,reference,strand,gene_version,
    gene_biotype,hgnc_id,symbol,locus_group,locus_type,gene_family)
    FROM '/home/flo/Schreibtisch/test_folder/gene.csv'
    DELIMITER ','
    CSV HEADER;

/*
 Inserting into expression table, if data not already exists
*/

COPY countinfo (run,sum_counts,library_name,library_strategy,library_selection,library_source,library_layout)
    FROM '/home/flo/Schreibtisch/test_folder/countinfo.csv'
    DELIMITER ','
    CSV HEADER;

CREATE TABLE sample_has_expression
(
    id  SERIAL PRIMARY KEY,
    run VARCHAR(100)
);

INSERT INTO sample_has_expression(run)
SELECT DISTINCT e.sample_id
FROM expression e;

UPDATE expression
SET id = she.id FROM sample_has_expression she
WHERE she.run = expression.sample_id;

UPDATE sample
SET id = she.id FROM sample_has_expression she
WHERE she.run = sample.run;

UPDATE countinfo
SET id = she.id FROM sample_has_expression she
WHERE she.run = countinfo.run;


/*
DEFINING CONSTRAINTS
*/
ALTER TABLE donor
    ADD CONSTRAINT project_donor
        FOREIGN KEY (project_code)
            REFERENCES project (project_code);

ALTER TABLE sample
    ADD CONSTRAINT donor_sample
        FOREIGN KEY (donor_id)
            REFERENCES donor (donor_id);

ALTER TABLE sample
    ADD CONSTRAINT sample_project
        FOREIGN KEY (project_code)
            REFERENCES project (project_code);

ALTER TABLE expression
    ADD CONSTRAINT gene_expression
        FOREIGN KEY (gene_id)
            REFERENCES gene (gene_id);

ALTER TABLE expression
ADD CONSTRAINT she_exp
FOREIGN KEY (id)
REFERENCES sample_has_expression (id);

ALTER TABLE sample
ADD CONSTRAINT she_sam
FOREIGN KEY (id)
REFERENCES sample_has_expression (id);

ALTER TABLE countinfo
ADD CONSTRAINT she_count
FOREIGN KEY (id)
REFERENCES sample_has_expression (id);
