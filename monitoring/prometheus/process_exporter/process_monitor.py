import logging
import psutil
import time

from prometheus_client import Gauge

class ProcessMonitor():
    def __init__(self, config, debug=False):
        self.processes = config["processes"]

        self.runtime, self.runtime_gauges = [], []
        for process in self.processes:
            self.runtime.append(0)

            process_name = process["name"].replace(".", "_")
            if not debug:
                self.runtime_gauges.append(Gauge(f"process_{process_name}_runtime", f"Runtime in seconds for {process_name}"))

    def find_process_runtime(self, idx, name, required_processes):
        self.runtime[idx] = 0

        # Iterate over all the running processes
        processes_found = 0
        for proc in psutil.process_iter(["pid", "ppid", "name", "cmdline", "create_time"]):
            try:
                # Check if process name contains the given name string and return its runtime
                if name.lower() in proc.info["name"].lower() or name.lower() in proc.info["cmdline"]:
                    logging.debug(f"Found process {name} ({proc.info['pid']}:{proc.info['ppid']})")

                    # Parent PID == 1: master process
                    if proc.info['ppid'] == 1:
                        self.runtime[idx] = int(time.time() - proc.info["create_time"])
                        if required_processes == 1:
                            return
                    # Not a master process, but we may not find one, so take the maximum time
                    else:
                        self.runtime[idx] = max(self.runtime[idx], int(time.time() - proc.info["create_time"]))

                    # Count number of processes found with this name
                    processes_found += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass

        # If not enough (sub-)processes were found, assume the process is down
        if required_processes > 1 and processes_found < required_processes:
            self.runtime[idx] = 0

    def collect(self):
        for idx, process in enumerate(self.processes):
            self.find_process_runtime(idx, process["name"], process["amount"])

    def save(self, debug=False):
        for i, (process, runtime) in enumerate(zip(self.processes, self.runtime)):
            if debug:
                logging.debug(f"{process['name']} has a runtime of {runtime} seconds")
            else:
                self.runtime_gauges[i].set(runtime)
