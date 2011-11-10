


def import_from_dotted_path(module_path):
    mod = __import__(module_path, globals(), locals(), [])
    sub_items = module_path.split('.')[1:]
    for sub_item in sub_items:
        try:
            mod = getattr(mod, sub_item)
        except AttributeError:
            raise AttributeError("Module %r has no attribute %r" % (mod, name))
    return mod


def run_job(module_path, args):
    job_cls = import_from_dotted_path(module_path)

    job = job_cls(*args)

    runner = job.make_runner()
