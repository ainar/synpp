import multiprocessing as mp


class ProcessManager:
    def __init__(self, available_processors=mp.cpu_count()):
        self.pending = []
        self.started = set()
        self.done = set()
        self.available_processors = available_processors
        self._count = 0

    def add(self, name, target, args, proc_usage=1, dependencies=None):
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
                set([name for name, _, _, _, _ in self.pending]).union(
                    self.started
                )
            ), "Dependencies must be already added in the ProcessManager"

        self.pending.append((name, target, args, dependencies, proc_usage))

    def _poll(self, name):
        return name in self.done

    def start(self):
        """Start processes wrt dependencies."""
        while len(self.pending) > 0:
            running = []
            # Find all processes with already done dependencies.
            available_processors = self.available_processors
            for name, target, args, dependencies, proc_usage in self.pending:
                if available_processors >= proc_usage:
                    if all(self._poll(dep) for dep in dependencies):
                        process = mp.Process(target=target, args=args)
                        process.start()
                        running.append(process)
                        self.started.add(name)
                        available_processors -= proc_usage

            # Delete from pending processes and from dependencies of the other
            # processes.
            self.pending = [
                (
                    name,
                    target,
                    args,
                    [dep for dep in deps if dep not in self.started],
                    proc_usage,
                )
                for name, target, args, deps, proc_usage in self.pending
                if name not in self.started
            ]

            # Wait for processes.
            for process in running:
                process.join()
                process.close()
                self.done = self.started
                self.started = set()
            if self.callback is not None:
                self.callback()
