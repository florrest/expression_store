/*
 Inserting into PROJECT table, if data not already exists
*/
/*
 Inserting into expression table, if data not already exists
*/

COPY gene (gene_id,gene_name,reference,strand,gene_version,
    gene_biotype,hgnc_id,symbol,locus_group,locus_type,gene_family)
    FROM '/home/flo/Schreibtisch/test_folder/gene.csv'
    DELIMITER ','
    CSV HEADER;

COPY project(project_code,study,sra_study,dbgap_study_accession)
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

COPY sample(run,sample_id,project_code,submitted_sample_id,icgc_specimen_id,submitted_specimen_id,donor_id,body_site,
    submitted_donor_id,study_specimen_involved_in,
    specimen_type,specimen_type_other,specimen_interval,specimen_donor_treatment_type,
    specimen_donor_treatment_type_other,specimen_processing,specimen_processing_other,specimen_storage,
    specimen_storage_other,tumour_confirmed,specimen_biobank,specimen_biobank_id,specimen_available,
    tumour_histological_type,tumour_grading_system,tumour_grade,tumour_grade_supplemental,
    tumour_stage_system,tumour_stage,tumour_stage_supplemental,
    study_donor_involved_in,consent,repository,experiemntal_strategy,assembly_name,experiment,sample,
    sample_type,sample_name, source,disease,tumour,analyte_type,histological_type,
    center_name,portal)
    FROM '/home/flo/Schreibtisch/test_folder/sample.csv'
    DELIMITER ','
    CSV HEADER;

/*
 Inserting into expression table, if data not already exists
*/
\timing on
COPY expression(run,gene_id,fpkm,tpm,coverage,raw_count)
    FROM '/home/flo/Schreibtisch/test_folder/expression.csv'
    DELIMITER ','
    CSV HEADER;


/*
 Inserting into expression table, if data not already exists
*/

COPY countinfo(run,sum_counts,library_name,library_strategy,library_selection,library_source,library_layout)
    FROM '/home/flo/Schreibtisch/test_folder/countinfo.csv'
    DELIMITER ','
    CSV HEADER;

/*
 Inserting into pipeline table, if data not already exists
*/

COPY pipeline(nf_core_rnaseq,Nextflow,FastQC,Cutadapt,Trim_Galore,SortMeRNA,STAR,HISAT2,Picard_MarkDuplicates,
    Samtools,featureCounts,Salmon,StringTie,Preseq,deepTools,RSeQC,dupRadar,edgeR,Qualimap,MultiQC)
    FROM '/home/flo/Schreibtisch/test_folder/pipeline.csv'
    DELIMITER ','
    CSV HEADER;



INSERT INTO sample_has_expression(run)
SELECT DISTINCT e.run
FROM expression e;

UPDATE donor
SET project_id = p.project_id from project p
WHERE p.project_code = donor.project_code;

UPDATE sample
SET project_id = p.project_id FROM project p
WHERE p.project_code = sample.project_code;

UPDATE expression
SET run_id = she.run_id FROM sample_has_expression she
WHERE she.run = expression.run;

UPDATE expression
SET gene_index_id = g.gene_index_id FROM gene g
WHERE g.gene_id = expression.gene_id;

UPDATE sample_has_expression
SET pipeline_id = p.pipeline_id FROM pipeline p;

UPDATE sample
SET run_id = she.run_id FROM sample_has_expression she
WHERE she.run = sample.run;

UPDATE countinfo
SET run_id = she.run_id FROM sample_has_expression she
WHERE she.run = countinfo.run;




