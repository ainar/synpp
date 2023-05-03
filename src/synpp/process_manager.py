import multiprocessing as mp


class ProcessManager:
    def __init__(self, available_processors=mp.cpu_count()):
        self.pending = []
        self.started = set()
        self.available_processors = available_processors

    def add(self, process, proc_usage=1, dependencies=None):
        """
        Add a new process to the list of processes to execute.

        :param process: the process to execute
        :param proc_usage: the processing usage of the process
        :param dependencies: the dependencies (must be present in the manager)
        """
        assert proc_usage <= self.available_processors, (
            "Process usage to high "
            + f"({proc_usage} asked, {self.available_processors} available)"
        )

        if dependencies is None:
            dependencies = []
        else:
            assert set(dependencies).issubset(
                set([p for p, _, _ in self.pending]).union(self.started)
            ), "Dependencies must be already added in the ProcessManager"

        self.pending.append((process, dependencies, proc_usage))

    def _poll(self, process):
        return not process.is_alive() and process in self.started

    def start(self):
        """Start processes wrt dependencies."""
        while len(self.pending) > 0:
            # Find all processes with already done dependencies.
            available_processors = self.available_processors
            for process, dependencies, proc_usage in self.pending:
                if available_processors >= proc_usage:
                    if all(self._poll(dep) for dep in dependencies):
                        self.started.add(process)
                        process.start()
                        available_processors -= proc_usage

            # Delete from pending processes and from dependencies of the other
            # processes.
            self.pending = [
                (
                    process,
                    [dep for dep in deps if dep not in self.started],
                    usage,
                )
                for process, deps, usage in self.pending
                if process not in self.started
            ]

            # Wait for processes.
            for process in self.started:
                process.join()
            if self.callback is not None:
                self.callback()
