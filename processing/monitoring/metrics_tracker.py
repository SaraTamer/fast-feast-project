class MetricsTracker:

    def __init__(self):
        self.total_records = 0
        self.duplicates = 0
        self.orphans = 0

    def update_duplicates(self, count):
        self.duplicates += count

    def update_records(self, count):
        self.total_records += count

    def update_orphans(self, count):
        self.orphans += count