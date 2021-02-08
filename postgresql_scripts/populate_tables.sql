COPY project(project_code,study,sra_study,project_id,study_pubmed_id,dbgap_study_accession)
FROM '/home/flo/Schreibtisch/test_folder/project.csv'
DELIMITER ','
CSV HEADER;

COPY expression(sample_id,gene_id,fpkm,tpm,coverage,raw_count)
FROM '/home/flo/Schreibtisch/test_folder/expression.csv'
DELIMITER ','
CSV HEADER;

COPY sample(run,sample_id,project_code,submitted_sample_id,icgc_specimen_id,submitted_specimen_id,donor_id,submitted_donor_id,analyzed_sample_interval,percentage_cellularity,level_of_cellularity,study_specimen_involved_in,specimen_type,specimen_type_other,specimen_interval,specimen_donor_treatment_type,specimen_donor_treatment_type_other,specimen_processing,specimen_processing_other,specimen_storage,specimen_storage_other,tumour_confirmed,specimen_biobank,specimen_biobank_id,specimen_available,tumour_histological_type,tumour_grading_system,tumour_grade,tumour_grade_supplemental,tumour_stage_system,tumour_stage,tumour_stage_supplemental,digital_image_of_stained_section,study_donor_involved_in,consent,repository,experiemntal_strategy,assembly_name,experiment,project_id,sample,sample_type,sample_name,source,disease,tumour,affection_status,analyte_type,histological_type,body_site,center_name,submission)
FROM '/home/flo/Schreibtisch/test_folder/sample.csv'
DELIMITER ','
CSV HEADER;

INSERT INTO sample_has_expression(run)
SELECT DISTINCT e.sample_id
FROM expression e;

ALTER TABLE expression
ADD column id INTEGER;

UPDATE expression
SET id = she.id
FROM sample_has_expression she
WHERE she.run = expression.sample_id;
