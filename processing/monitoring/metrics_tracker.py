# processing/monitoring/metrics_tracker.py
from datetime import datetime
from typing import Dict, List, Any
import pandas as pd
import json
from db.connections import SnowflakeConnection
from db.warehouse_manager import WarehouseManager
from core.logger import AuditLogger


class MetricsTracker:
    """Tracks all quality metrics for batch and stream processing."""

    def __init__(self, database="FASTFEASTDWH", schema="PUBLIC"):
        # Core counters
        self.total_records = 0
        self.duplicates = 0
        self.orphans = 0
        self.null_records = 0
        self.quarantined_records = 0
        self.clean_records = 0
        self.files_processed = 0
        self.files_failed = 0

        # Detailed metrics
        self.null_percentages: Dict[str, Dict[str, float]] = {}
        self.referential_integrity_rate: Dict[str, float] = {}
        self.sla_consistency_rate: Dict[str, float] = {}
        self.processing_latencies: List[float] = []

        # Per-batch metrics storage
        self.batch_metrics: List[Dict[str, Any]] = []

        # Start time for latency tracking
        self.batch_start_time: Dict[str, datetime] = {}

        # Snowflake configuration
        self.database = database
        self.schema = schema
        self.snowflake_connection = SnowflakeConnection()
        self.warehouse_manager = WarehouseManager(self.snowflake_connection.conn, "COMPUTE_WH")
        self.logger = AuditLogger()

        # Ensure metrics table exists
        self._ensure_metrics_table()

    def _ensure_metrics_table(self):
        """Create quality_metrics table if it doesn't exist."""
        with self.warehouse_manager.auto_manage():
            cursor = self.snowflake_connection.conn.cursor()
            try:
                cursor.execute(f"USE DATABASE {self.database}")
                cursor.execute(f"USE SCHEMA {self.schema}")

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS quality_metrics (
                        batch_id VARCHAR,
                        metric_name VARCHAR,
                        metric_value FLOAT,
                        metric_details VARIANT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                    )
                """)
                self.logger.log_msg("Quality metrics table verified/created")
            except Exception as e:
                self.logger.log_err(f"Failed to create quality_metrics table: {e}")
            finally:
                cursor.close()

    def start_batch(self, batch_id: str, table_name: str):
        """Record the start time of a batch/stream processing."""
        self.batch_start_time[f"{table_name}_{batch_id}"] = datetime.now()

    def end_batch(self, batch_id: str, table_name: str, file_path: str = None):
        """Record the end time and calculate latency."""
        key = f"{table_name}_{batch_id}"
        if key in self.batch_start_time:
            latency = (datetime.now() - self.batch_start_time[key]).total_seconds()
            self.processing_latencies.append(latency)
            self.log_metric("processing_latency_seconds", latency, table_name, batch_id)

    def update_duplicates(self, count: int):
        self.duplicates += count

    def update_records(self, count: int):
        self.total_records += count

    def update_orphans(self, count: int):
        self.orphans += count

    def update_null_records(self, count: int):
        self.null_records += count

    def update_quarantined(self, count: int):
        self.quarantined_records += count

    def update_clean_records(self, count: int):
        self.clean_records += count

    def increment_files_processed(self):
        self.files_processed += 1

    def increment_files_failed(self):
        self.files_failed += 1

    def update_null_percentages(self, table_name: str, null_summary: Dict):
        """Store null percentages per column."""
        if table_name not in self.null_percentages:
            self.null_percentages[table_name] = {}
        for col, stats in null_summary.items():
            self.null_percentages[table_name][col] = stats['null_percentage']

    def update_referential_integrity(self, table_name: str, total_records: int, valid_records: int):
        """Calculate referential integrity rate (FK validation pass rate)."""
        rate = (valid_records / total_records * 100) if total_records > 0 else 100.0
        self.referential_integrity_rate[table_name] = rate

    def update_sla_consistency(self, table_name: str, consistent_count: int, total_count: int):
        """Calculate SLA calculation consistency rate."""
        rate = (consistent_count / total_count * 100) if total_count > 0 else 100.0
        self.sla_consistency_rate[table_name] = rate

    def log_metric(self, metric_name: str, value: Any, table_name: str = None, batch_id: str = None):
        """Log a metric to the batch metrics list."""
        self.batch_metrics.append({
            'metric_name': metric_name,
            'value': value,
            'table_name': table_name,
            'batch_id': batch_id,
            'timestamp': datetime.now().isoformat()
        })

    def get_metrics(self) -> Dict:
        """Get current metrics summary."""
        return {
            'total_records': self.total_records,
            'duplicates': self.duplicates,
            'orphans': self.orphans,
            'null_records': self.null_records,
            'quarantined_records': self.quarantined_records,
            'clean_records': self.clean_records,
            'files_processed': self.files_processed,
            'files_failed': self.files_failed,
            'duplicate_rate': (self.duplicates / self.total_records * 100) if self.total_records > 0 else 0,
            'orphan_rate': (self.orphans / self.total_records * 100) if self.total_records > 0 else 0,
            'null_rate': (self.null_records / self.total_records * 100) if self.total_records > 0 else 0,
            'referential_integrity_rate': self.referential_integrity_rate,
            'sla_consistency_rate': self.sla_consistency_rate,
            'avg_processing_latency_seconds': sum(self.processing_latencies) / len(
                self.processing_latencies) if self.processing_latencies else 0,
            'file_success_rate': ((
                                          self.files_processed - self.files_failed) / self.files_processed * 100) if self.files_processed > 0 else 100
        }

    def get_full_report(self) -> Dict:
        """Get comprehensive quality report."""
        metrics = self.get_metrics()
        return {
            'summary': metrics,
            'null_percentages': self.null_percentages,
            'batch_metrics': self.batch_metrics,
            'processing_latencies': self.processing_latencies
        }

    def reset(self):
        """Reset all metrics."""
        self.total_records = 0
        self.duplicates = 0
        self.orphans = 0
        self.null_records = 0
        self.quarantined_records = 0
        self.clean_records = 0
        self.files_processed = 0
        self.files_failed = 0
        self.null_percentages = {}
        self.referential_integrity_rate = {}
        self.sla_consistency_rate = {}
        self.processing_latencies = []
        self.batch_metrics = []
        self.batch_start_time = {}

    def save_to_snowflake(self, batch_id: str):
        """Save metrics to Snowflake quality_metrics table."""
        metrics = self.get_metrics()

        with self.warehouse_manager.auto_manage():
            cursor = self.snowflake_connection.conn.cursor()
            try:
                cursor.execute(f"USE DATABASE {self.database}")
                cursor.execute(f"USE SCHEMA {self.schema}")

                # Ensure table exists with correct schema
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS quality_metrics (
                        batch_id VARCHAR,
                        metric_name VARCHAR,
                        metric_value FLOAT,
                        metric_details_string VARCHAR,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                    )
                """)

                # Also add column if it exists but missing (for backward compatibility)
                try:
                    cursor.execute("""
                        ALTER TABLE quality_metrics 
                        ADD COLUMN IF NOT EXISTS metric_details_string VARCHAR
                    """)
                except:
                    pass  # Column might already exist

                # Insert summary metrics
                for key, value in metrics.items():
                    if isinstance(value, (int, float)) and not isinstance(value, bool):
                        cursor.execute("""
                            INSERT INTO quality_metrics (batch_id, metric_name, metric_value)
                            VALUES (%s, %s, %s)
                        """, (batch_id, key, float(value)))

                # Insert JSON data as strings
                if self.null_percentages:
                    cursor.execute("""
                        INSERT INTO quality_metrics (batch_id, metric_name, metric_details_string)
                        VALUES (%s, %s, %s)
                    """, (batch_id, 'null_percentages', json.dumps(self.null_percentages, default=str)))

                if self.referential_integrity_rate:
                    cursor.execute("""
                        INSERT INTO quality_metrics (batch_id, metric_name, metric_details_string)
                        VALUES (%s, %s, %s)
                    """, (batch_id, 'referential_integrity_rate',
                          json.dumps(self.referential_integrity_rate, default=str)))

                if self.sla_consistency_rate:
                    cursor.execute("""
                        INSERT INTO quality_metrics (batch_id, metric_name, metric_details_string)
                        VALUES (%s, %s, %s)
                    """, (batch_id, 'sla_consistency_rate', json.dumps(self.sla_consistency_rate, default=str)))

                if self.processing_latencies:
                    cursor.execute("""
                        INSERT INTO quality_metrics (batch_id, metric_name, metric_details_string)
                        VALUES (%s, %s, %s)
                    """, (batch_id, 'processing_latencies', json.dumps(self.processing_latencies, default=str)))

                self.snowflake_connection.conn.commit()
                self.logger.log_msg(f"Saved metrics to Snowflake for batch {batch_id}")

            except Exception as e:
                self.snowflake_connection.conn.rollback()
                self.logger.log_err(f"Failed to save metrics to Snowflake: {e}")
            finally:
                cursor.close()

    def print_summary(self):
        """Print a summary of metrics to console."""
        metrics = self.get_metrics()

        print("\n" + "=" * 60)
        print("QUALITY METRICS SUMMARY")
        print("=" * 60)
        print(f"Total Records Processed: {metrics['total_records']:,}")
        print(f"Clean Records: {metrics['clean_records']:,}")
        print(f"Duplicates: {metrics['duplicates']:,} ({metrics['duplicate_rate']:.2f}%)")
        print(f"Orphans: {metrics['orphans']:,} ({metrics['orphan_rate']:.2f}%)")
        print(f"Null Records: {metrics['null_records']:,} ({metrics['null_rate']:.2f}%)")
        print(f"Quarantined Records: {metrics['quarantined_records']:,}")
        print(f"Files Processed: {metrics['files_processed']}")
        print(f"Files Failed: {metrics['files_failed']}")
        print(f"File Success Rate: {metrics['file_success_rate']:.2f}%")
        print(f"Average Processing Latency: {metrics['avg_processing_latency_seconds']:.2f} seconds")
        print("=" * 60)