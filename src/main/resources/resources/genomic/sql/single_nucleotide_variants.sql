select
snv.SNV_DATA_ID,
snv.UPDATE_DATETIME,
snv.PANEL,
snv.CLI_RRPT_ID,
snv.MUT_TYPE,
snv.SMP_ID,
snv.PRJ_ID,
snv.SEC_ID,
snv.GENE_CONCEPT_ID,
snv.ENS_ID,
snv.MUTATION_STATUS,
snv.CHROMOSOME,
snv.REF,
snv.VAR,
snv.VARIANT_CLASS,
snv.VARIANT_TYPE,
snv.HGVSC,
snv.HGVSP,
snv.HGVSP_DB,
snv.DBSNP,
snv.START AS ST,
snv.END AS EN,
snv.T_TOTAL_DEPTH,
snv.T_REF_DEPTH,
snv.T_VAR_DEPTH,
snv.N_TOTAL_DEPTH,
snv.N_REF_DEPTH,
snv.N_VAR_DEPTH,
snv.ALLELE_FREQ,
snv.STRAND,
snv.EOXN,
snv.INTRON,
snv.SIFT,
snv.POLYPHEN,
snv.DOMAIN,
snv.HRD,
snv.MMR,
snv.ZYGOSITY,
snv.TRANSCRIPTRANK,
snv.DIAGNOSIS,
snv.DRUG,
snv.DRUGABLE,
snv.WHITELIST,
snv.SPECIMEN_ID
FROM @CDM_schema.genomic_single_nucleotide_variants snv
@WHERE_condition
