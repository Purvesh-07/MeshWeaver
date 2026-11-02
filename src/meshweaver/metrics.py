import os


def _read_stat():
    with open('/proc/stat') as f:
        vals = [int(x) for x in f.readline().split()[1:]]
    return sum(vals), vals[3] + vals[4]       # total time, idle time


class Metrics:
    def __init__(self):
        try:
            self.total, self.idle = _read_stat()
        except OSError:
            self.total, self.idle = 0, 0

    def cpu_percent(self):
        # CPU use since the previous call
        try:
            total, idle = _read_stat()
        except OSError:                       # not Linux: use the load average
            try:
                return min(100.0, os.getloadavg()[0] / (os.cpu_count() or 1) * 100)
            except (AttributeError, OSError):
                return 0.0
        dt, di = total - self.total, idle - self.idle
        self.total, self.idle = total, idle
        return 0.0 if dt <= 0 else 100.0 * (1 - di / dt)

    def mem_percent(self):
        try:
            info = {}
            with open('/proc/meminfo') as f:
                for line in f:
                    name, rest = line.split(':', 1)
                    info[name] = int(rest.split()[0])
            return 100.0 * (1 - info['MemAvailable'] / info['MemTotal'])
        except (OSError, KeyError, ValueError):
            return 0.0
