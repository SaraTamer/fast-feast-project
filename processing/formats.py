import core.logger as logger
from config.format_pattern import FORMAT_PATTERNS
from processing.error_batch_writer import ErrorBatchWriter
 
 
class FormatChecker:
    """
    checks format of specific columns
    """
 
    def __init__(self):
        self.audit_logger = logger.AuditLogger()
        self.error_writer = ErrorBatchWriter()
 
    def separate(self, relation, columns_meta, table_name=None, batch_id=None, primary_key=None):

        format_columns = [
            m for m in columns_meta
            if m.get("format") and m["format"] in FORMAT_PATTERNS
        ]
 
        if not format_columns:
            return relation, None
 
        fail_checks, fail_flags = self._build_checks(format_columns)
 
        if not fail_checks:
            return relation, None
 
        any_failure = " OR ".join(fail_checks)
 
        fail_reason = (
            "LIST_FILTER(LIST_VALUE("
            + ", ".join(fail_flags)
            + "), x -> x IS NOT NULL)"
        )
 
        # BAD ROWS 
        bad_rows_df = (
            relation
            .filter(any_failure)
            .project(f"*, {fail_reason} AS __failed_format_columns")
            .df()
        )
 
        # CLEAN ROWS 
        clean_relation = relation.filter(f"NOT ({any_failure})")
 
        if bad_rows_df is not None and not bad_rows_df.empty:
            self.audit_logger.log(
                event="format_check",
                detail={
                    "bad_row_count": len(bad_rows_df),
                    "checked_columns": [m["column"] for m in format_columns],
                }
            )


            # Only quarantine if full context is provided
            if table_name and batch_id and primary_key:

                rows = []

                for _, row in bad_rows_df.iterrows():

                    # ── Build event_id from PK ──
                    if isinstance(primary_key, list):  # composite PK
                        if any(pd.isna(row[pk]) for pk in primary_key):
                            raise ValueError(f"Missing PK in row: {row}")

                        event_id = "_".join(str(row[pk]) for pk in primary_key)

                    else:  # single PK
                        if pd.isna(row[primary_key]):
                            raise ValueError(f"Missing PK in row: {row}")

                        event_id = str(row[primary_key])

                    # Optional: make globally unique
                    event_id = f"{table_name}:{event_id}"

                    # ── Extract failing columns ──
                    failed_cols = row.get("__failed_format_columns", [])
                    error_column = ", ".join(failed_cols) if failed_cols else "unknown"

                    rows.append((
                        event_id,
                        row.to_dict()
                    ))

                try:
                    self.error_writer.write_batch(
                        table_name=table_name,
                        batch_id=batch_id,
                        rows=rows,
                        error_type="FORMAT_ERROR",
                        error_column=error_column,
                        fk_table=None,
                        is_retryable=True
                    )

                    self.audit_logger.log_msg(
                        f"Quarantined {len(rows)} FORMAT_ERROR rows for {table_name} (batch={batch_id})"
                    )

                except Exception as e:
                    self.audit_logger.log_err(f"Failed to quarantine format errors: {e}")
 
        return clean_relation, bad_rows_df
    
 
    # HELPER Methods
    def _build_checks(self, format_columns):
        """
        Returns:
            fail_checks : the conditions in the where statement to check failures
            fail_flags  : the cols that falied these conditions
        """
        fail_checks = []
        fail_flags  = []
 
        for m in format_columns:
            col     = m["column"]
            pattern = FORMAT_PATTERNS[m["format"]]
 
            # A row is bad when the value is present but doesn't match the pattern
            fail_checks.append(
                f'("{col}" IS NOT NULL AND NOT regexp_matches("{col}", \'{pattern}\'))'
            )
 
            fail_flags.append(f"""
                CASE
                    WHEN "{col}" IS NOT NULL
                     AND NOT regexp_matches("{col}", '{pattern}')
                    THEN '{col}'
                    ELSE NULL
                END
            """)
 
        return fail_checks, fail_flags
 