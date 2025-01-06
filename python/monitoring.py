from prometheus_client import Counter, Gauge

class MonitoringModule:
    def __init__(self, prefix):
        """
        Initialize the monitoring module.
        """
        self.metrics = {}
        self.base_prefix = prefix

    def create_counter(self, name: str, description: str):
        """Create and register a new Prometheus Counter."""
        fqn = f"{self.base_prefix}_{name}"
        if fqn not in self.metrics:
            self.metrics[fqn] = Counter(fqn, description)

    def create_gauge(self, name: str, description: str):
        """Create and register a new Prometheus Gauge."""
        fqn = f"{self.base_prefix}_{name}"
        if fqn not in self.metrics:
            self.metrics[fqn] = Gauge(fqn, description)

    def increment_counter(self, name: str, amount: int = 1):
        """Increment a Prometheus Counter by a specified amount."""
        fqn = f"{self.base_prefix}_{name}"
        if fqn in self.metrics:
            self.metrics[fqn].inc(amount)

    def set_gauge(self, name: str, value: float):
        """Set the value of a Prometheus Gauge."""
        fqn = f"{self.base_prefix}_{name}"
        if fqn in self.metrics:
            self.metrics[fqn].set(value)