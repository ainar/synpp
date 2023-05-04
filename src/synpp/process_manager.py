import multiprocessing as mp
import time


class ProcessManager:
    def __init__(self, logger, total_cpu=mp.cpu_count()):
        self.pending = dict()
        self.started = set()
        self.done = set()
        self.logger = logger
        self.total_cpu = total_cpu
        self._count = 0
        self.running_processes = dict()

    def add(self, name, target, args, proc_usage=1, dependencies=None):
        """
        Add a new process to the list of processes to execute.

        :param process: the process to execute
        :param proc_usage: the processing usage of the process
        :param dependencies: the dependencies (must be present in the manager)
        """
        assert proc_usage <= self.total_cpu, (
            "Process usage to high "
            + f"({proc_usage} asked, {self.total_cpu} available)"
        )

        if dependencies is None:
            dependencies = []
        else:
            assert set(dependencies).issubset(
                set(self.pending.keys()).union(self.started)
            ), "Dependencies must be already added in the ProcessManager"

        self.pending[name] = target, args, dependencies, proc_usage
        self._count += 1

    def _poll(self, name):
        return name in self.done

    def _free_cpu(self):
        used = sum(
            cpu_usage for _p, cpu_usage in self.running_processes.values()
        )
        return self.total_cpu - used

    def start(self):
        """Start processes wrt dependencies."""
        while len(self.pending) > 0:
            # Find all processes with already done dependencies.
            newly_started = set()
            for name, (
                target,
                args,
                dependencies,
                cpu_usage,
            ) in self.pending.items():
                if self._free_cpu() >= cpu_usage:
                    if all(self._poll(dep) for dep in dependencies):
                        process = mp.Process(target=target, args=args)
                        process.start()
                        self.running_processes[name] = process, cpu_usage
                        self.started.add(name)
                        newly_started.add(name)

            for name in newly_started:
                del self.pending[name]

            # Wait for the first process to join.
            process_done = False
            while True:
                for name, (
                    process,
                    cpu_usage,
                ) in self.running_processes.items():
                    process.join(0.1)
                    if process.exitcode == 0:
                        process.close()
                        del self.running_processes[name]
                        self.done.add(name)
                        process_done = True
                        break
                    else:
                        assert process.exitcode is None, "An error occured."
                if process_done:
                    break

            if self.callback is not None:
                self.callback()
            self._show_progress()

    def _show_progress(self):
        self.logger.info(
            "Pipeline progress: %d/%d (%.2f%%)"
            % (
                len(self.done),
                self._count,
                100 * len(self.done) / self._count,
            )
        )
