# coding: utf-8


def _get_job_running_path(self, job):
        return self._zk_root + '/' + str(job.id)
