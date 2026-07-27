"""Per-category 'problem' transforms. One module per problem group.

Most Data Quality problems are enforced in the medallion foundation
(raw_ingest + clean_silver via lib.dq_rules). These modules add the dedicated
monitors and demonstrations that don't belong in the linear medallion flow.
"""
