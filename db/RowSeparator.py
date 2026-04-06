import duckdb
import core.logger as logger
from config.type_mapping import YAML_TO_DUCKDB


class RowSeparator:
    """
    Takes a DuckDB relation + list of mismatched columns.
    Separates good rows from bad rows using TRY_CAST.
    """

    def __init__(self):
        self.audit_logger = logger.AuditLogger()

    def separate(self, relation, mismatched_columns):
        """
        Returns:
            (clean_relation, bad_rows_df)
        """

        fail_checks, fail_flags = self._build_checks(mismatched_columns)

        if not fail_checks:
            return relation, None

        any_failure = " OR ".join(fail_checks)

        fail_reason = (
            "LIST_FILTER(LIST_VALUE("
            + ", ".join(fail_flags)
            + "), x -> x IS NOT NULL)"
        )

        # ───────────────────────────────
        # BAD ROWS
        # ───────────────────────────────
        bad_rows_df = relation.filter(any_failure).project(
            f"*, {fail_reason} AS __failed_columns"
        ).df()

        # ───────────────────────────────
        # CLEAN ROWS
        # ───────────────────────────────
        cast_select = self._build_cast_select(relation, mismatched_columns)

        clean_relation = relation.filter(f"NOT ({any_failure})").project(
            cast_select
        )

        return clean_relation, bad_rows_df

    # ───────────────────────────────
    # HELPERS
    # ───────────────────────────────

    def _build_checks(self, mismatched_columns):
        fail_checks = []
        fail_flags = []

        for m in mismatched_columns:
            col = m["column"]
            target = YAML_TO_DUCKDB.get(m["expected_yaml"])

            if not target:
                continue

            fail_checks.append(
                f'("{col}" IS NOT NULL AND TRY_CAST("{col}" AS {target}) IS NULL)'
            )

            fail_flags.append(f"""
                CASE
                    WHEN "{col}" IS NOT NULL
                     AND TRY_CAST("{col}" AS {target}) IS NULL
                    THEN '{col}'
                    ELSE NULL
                END
            """)

        return fail_checks, fail_flags

    def _build_cast_select(self, relation, mismatched_columns):
        all_columns = relation.columns
        mismatch_map = {m["column"]: m for m in mismatched_columns}

        expressions = []

        for col in all_columns:
            if col in mismatch_map:
                target = YAML_TO_DUCKDB.get(
                    mismatch_map[col]["expected_yaml"]
                )

                if target:
                    expressions.append(
                        f'TRY_CAST("{col}" AS {target}) AS "{col}"'
                    )
                else:
                    expressions.append(f'"{col}"')
            else:
                expressions.append(f'"{col}"')

        return ", ".join(expressions)