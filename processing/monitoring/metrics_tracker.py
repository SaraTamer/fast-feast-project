class MetricsTracker:

    def __init__(self):
        self.total_records = 0
        self.duplicates = 0
        self.orphans = 0
        self.null_records = 0
        self.quarantined_records = 0
        self.clean_records = 0
        self.files_processed = 0

    def update_duplicates(self, count):
        self.duplicates += count

    def update_records(self, count):
        self.total_records += count

    def update_orphans(self, count):
        self.orphans += count

    def update_null_records(self, count):
        self.null_records += count

    def update_quarantined(self, count):
        self.quarantined_records += count

    def update_clean_records(self, count):
        self.clean_records += count

    def increment_files_processed(self):
        self.files_processed += 1

    def get_metrics(self):
        return {
            'total_records': self.total_records,
            'duplicates': self.duplicates,
            'orphans': self.orphans,
            'null_records': self.null_records,
            'quarantined_records': self.quarantined_records,
            'clean_records': self.clean_records,
            'files_processed': self.files_processed
        }

    def reset(self):
        self.total_records = 0
        self.duplicates = 0
        self.orphans = 0
        self.null_records = 0
        self.quarantined_records = 0
        self.clean_records = 0
        self.files_processed = 0