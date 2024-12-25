from prometheus_client import Counter, Gauge

class MonitoringModule:
    def __init__(self):
        """
        Initialize the monitoring module.
        """
        self.metrics = {}

    def create_counter(self, name: str, description: str):
        """Create and register a new Prometheus Counter."""
        if name not in self.metrics:
            self.metrics[name] = Counter(name, description)

    def create_gauge(self, name: str, description: str):
        """Create and register a new Prometheus Gauge."""
        if name not in self.metrics:
            self.metrics[name] = Gauge(name, description)

    def increment_counter(self, name: str, amount: int = 1):
        """Increment a Prometheus Counter by a specified amount."""
        if name in self.metrics:
            self.metrics[name].inc(amount)

    def set_gauge(self, name: str, value: float):
        """Set the value of a Prometheus Gauge."""
        if name in self.metrics:
            self.metrics[name].set(value)