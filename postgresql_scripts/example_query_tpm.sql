\timing on
SELECT g.gene_id, e.tpm, s.run
FROM gene g, expression e, sample s, sample_has_expression she
WHERE s.body_site = 'Pancreas' AND she.id = s.id AND she.id = e.id AND e.gene_id = g.gene_id
GROUP BY s.run, g.gene_id,e.tpm;