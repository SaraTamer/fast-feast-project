import re

from config.config_loader import Config

class QualityCheck:

    EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
    PHONE_REGEX = re.compile(r"^\+?[0-9]{8,15}$")

    def __init__(self, quarantine_db, temp_db):
        self.EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
        self.PHONE_REGEX = re.compile(r"^\+?[0-9]{8,15}$")
        self.quarantine_db = quarantine_db
        self.temp_db = temp_db
        self._processed_files = set()
        self._quarantine_buffer = []
        self.config = Config().load_the_yaml() or {}
        self.rules = self.config.get("rules", {})
        # Config-driven thresholds and defaults.
        self.orphan_threshold = float(self.rules.get("orphan_rate_threshold", 0.10))
        self.numeric_ranges = self.rules.get(
            "numeric_ranges",
            {
                "rating": (0, 5),
                "amount": (0, float("inf")),
                "refund_amount": (0, float("inf")),
            },
        )
        self.required_columns = self.rules.get("required_columns", {})
        self.allowed_values = self.rules.get("allowed_values", {})
        self.duplicate_keys = self.rules.get(
            "duplicate_keys",
            {"orders": ["order_id"], "tickets": ["ticket_id"], "ticket_events": ["event_id"]},
        )
        self.orphan_fk_rules = self.rules.get(
            "orphan_fk_rules",
            {
                "orders": {
                    "customer_id": "customers",
                    "restaurant_id": "restaurants",
                    "driver_id": "drivers",
                },
                "tickets": {"order_id": "orders", "agent_id": "agents"},
            },
        )
        self.metrics = {
            "total_records_processed": 0,
            "quarantined_records": 0,
            "duplicate_records": 0,
            "orphan_records": 0,
            "duplicate_rate": 0.0,
            "orphan_rate": 0.0,
            "referential_integrity_rate": 1.0,
            "null_percentage_per_column": {},
        }

    def check_duplicate_filename(self, file_name: str):
        """
        Returns True if file has already been processed, else False.
        """
        file_key = file_name
        already_seen = file_key in self._processed_files
        if already_seen:
            return True
        self._processed_files.add(file_key)

        return False

    def check_duplicates(self, df):
        """
        Remove duplicate business keys.
        Returns (clean_df, duplicate_df, metrics).
        """
        # if empty df
        if df is None or getattr(df, "empty", False):
            return df, self._new_empty_frame_like(df), {"duplicate_count": 0, "duplicate_rate": 0.0}
        entity = self._get_entity_name(df)
        key_cols = self._get_existing_columns(df, self.duplicate_keys.get(entity, []))
        # if not matching primary keys return empty df
        #TODO: insert in quarantine instead
        if not key_cols:
            key_cols = self._fallback_id_columns(df)
        if not key_cols:
            # Without a reliable key, avoid dropping data silently.
            return df, self._new_empty_frame_like(df), {"duplicate_count": 0, "duplicate_rate": 0.0}
        # identify duplicates
        duplicate_mask = df.duplicated(subset=key_cols, keep="first")
        duplicate_df = df[duplicate_mask].copy()
        # clean duplication
        # TODO: insert duplicate_df in quarantine
        clean_df = df[~duplicate_mask].copy()
        duplicate_count = int(duplicate_mask.sum())
        total = int(len(df))
        duplicate_rate = (duplicate_count / total) if total else 0.0
        # calculate some metrics
        self.metrics["total_records_processed"] += total
        self.metrics["duplicate_records"] += duplicate_count
        self.metrics["duplicate_rate"] = (
            self.metrics["duplicate_records"] / self.metrics["total_records_processed"]
            if self.metrics["total_records_processed"]
            else 0.0
        )
        return clean_df, duplicate_df, {
            "duplicate_count": duplicate_count,
            "duplicate_rate": round(duplicate_rate, 6),
        }

    def check_orphans(self, df):
        pass

    def check_date_formats(self, df):
        pass

# ---------- Helper methods ----------
    @staticmethod
    def _get_existing_columns(df, candidates: list[str]):
        return [c for c in candidates if c in df.columns]
    @staticmethod
    def _fallback_id_columns(df):
        id_cols = [c for c in df.columns if c.lower() == "id" or c.lower().endswith("_id")]
        return id_cols[:1]
    @staticmethod
    def _new_empty_frame_like(df):
        if df is None:
            return None
        return df.iloc[0:0].copy()
    @staticmethod
    def _get_entity_name(df):
        # use known primary key names to infer the dataset.
        cols = {c.lower() for c in df.columns}
        if "order_id" in cols and "ticket_id" not in cols:
            return "orders"
        if "ticket_id" in cols:
            return "tickets"
        if "event_id" in cols:
            return "ticket_events"
        return "generic"
