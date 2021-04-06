CREATE TABLE sample_has_expression
(
    run         VARCHAR(100),
    run_id SERIAL,
    pipeline_id INTEGER
);
ALTER TABLE project
    ADD COLUMN project_id SERIAL;

ALTER TABLE gene
    ADD COLUMN gene_index_id SERIAL;

ALTER TABLE pipeline
    ADD COLUMN pipeline_id SERIAL;

ALTER TABLE donor
    ADD COLUMN project_id INTEGER;

ALTER TABLE sample
    ADD COLUMN run_id INTEGER,
    ADD COLUMN project_id INTEGER;

ALTER TABLE expression
    ADD COLUMN run_id INTEGER,
    ADD COLUMN gene_index_id INTEGER;

ALTER TABLE countinfo
    ADD COLUMN run_id INTEGER;


INSERT INTO sample_has_expression(run)
SELECT DISTINCT e.run
FROM expression e;

UPDATE expression
SET run_id = she.run_id,
    gene_index_id = g.gene_index_id
FROM sample_has_expression she, gene g
WHERE she.run = expression.run AND expression.gene_id = g.gene_id;

UPDATE sample
SET run_id = she.run_id,
    project_id = prj.project_id
FROM sample_has_expression she, project prj
WHERE she.run = sample.run AND prj.project_code = sample.project_code;

UPDATE countinfo
SET run_id = she.run_id FROM sample_has_expression she
WHERE she.run = countinfo.run;

UPDATE sample_has_expression
SET pipeline_id = pip.pipeline_id FROM pipeline pip;

ALTER TABLE expression
    ADD PRIMARY KEY (run_id, gene_index_id);

ALTER TABLE sample_has_expression
    ADD PRIMARY KEY (run_id);

ALTER TABLE pipeline
    ADD PRIMARY KEY (pipeline_id);

ALTER TABLE countinfo
    ADD PRIMARY KEY (run);

ALTER TABLE gene
    ADD PRIMARY KEY (gene_index_id);

ALTER TABLE donor
    ADD PRIMARY KEY (donor_id);

ALTER TABLE project
    ADD PRIMARY KEY (project_id);

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
