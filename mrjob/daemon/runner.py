from multiprocessing import Process, Queue
import os

from mrjob.util import temp_log_to_stream


def wd_path(wd, job_name, *more_path):
    full_path = os.path.join(wd, job_name, *more_path)
    base_path = os.path.split(full_path)[0]
    if not os.path.exists(base_path):
        os.makedirs(base_path)
    return full_path


def run_job(job_cls, args, wd):
    job = job_cls(args=args)

    info_queue = Queue()
    process = Process(target=job_runner,
                      args=(job, info_queue, wd))

    process.start()

    return process, info_queue


def job_runner(job, info_queue, wd):
    with job.make_runner() as runner:
        with open(wd_path(wd, runner._job_name, 'stderr'), 'w') as log_file:
            with temp_log_to_stream(name='mrjob', stream=log_file):
                info_queue.put(runner._job_name)
                runner.run()

                with open(wd_path(wd, runner._job_name, 'stdout'), 'w') \
                    as out_file:
                    for line in runner.stream_output():
                        out_file.write(line)
