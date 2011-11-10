from multiprocessing import Process, Queue


def import_from_dotted_path(path):
    items = path.split('.')

    mod_path = '.'.join(items[:-1])
    mod = __import__(mod_path, globals(), locals(), [])
    for sub_item in items[1:-1]:
        try:
            mod = getattr(mod, sub_item)
        except AttributeError:
            raise AttributeError("Module %r has no attribute %r" % (mod, name))
    try:
        mod = getattr(mod, items[-1])
    except AttributeError:
        raise AttributeError("Module %r has no attribute %r" % (mod, items[-1]))
    return mod


def run_job(module_path, args):
    job_cls = import_from_dotted_path(module_path)
    job = job_cls(args=args)

    queue = Queue()
    process = Process(target=job_runner, args=(job, queue))

    process.start()

    return process, queue


def job_runner(job, queue):
    with job.make_runner() as runner:
        queue.put(runner._job_name)
        runner.run()

        for line in runner.stream_output():
            queue.put(line)
    queue.put(None)
