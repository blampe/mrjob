import cmd
import os
import inspect
import optparse

from mrjob.emr import EMRJobRunner

from mrjob.tools.emr import fetch_logs
from mrjob.tools.emr import get_status


FETCH_LOGS = 'fetch_logs'
GET_STATUS = 'get_status'
SET_JOB = 'set_job'

# could do this programatically by inspecting method argspecs
# map modules to their callable methods
module_methods = {
        FETCH_LOGS: (
            fetch_logs.cat_all,
            fetch_logs.cat_relevant,
            fetch_logs.list_all,
            fetch_logs.list_relevant,
        ),

        GET_STATUS: (
            get_status.get_job_status,
        ),

        'test': (
            get_status.test,
        ),
    }

class EvaluationError(Exception):
    error_prefix = '***'

    def __str__(self):
        return self.error_prefix + super(EvaluationError, self).__repr__()

class NeedMoreInputError(EvaluationError):
    pass

class Shell(cmd.Cmd):
    """
    A read-eval-print-loop class for dispatching commands on job flows.

    Example usage:

        XXX

    """

    # shorter command names for convenience
    aliases = dict(
            logs=FETCH_LOGS,
            status=GET_STATUS,
            job=SET_JOB,
    )


    # todo: option

    def __init__(self, job_flow_id=None, prompt='> ', *args, **kwargs):
        cmd.Cmd.__init__(self, *args, **kwargs)
        self.prompt = prompt
        self.job_flow_id = job_flow_id
        self.runner = None

        # add `do_script_name` methods to ourselves
        for module_name in module_methods:
            self.build_tool_handlers(module_name)

        # create methods for aliases
        for aliased_name, true_method_name in self.aliases.iteritems():
            true_method = getattr(self, 'do_%s' % true_method_name)
            setattr(self, 'do_%s' % aliased_name, true_method)

    def build_tool_handlers(self, module_name):
        methods_to_handle = module_methods[module_name]

        # if the module has only 1 exposed method, then the module's
        # name (or alias) will invoke it.
        if len(methods_to_handle) == 1:
            setattr(self, 'do_%s' % module_name, self.run_tool_method(list(methods_to_handle)[0]))
            return

        # otherwise add all the methods under this module
        setattr(self, 'do_%s' % module_name, lambda *args, **kwargs: None)
        for method in methods_to_handle:
            setattr(self, 'do_%s' % method.__name__, lambda *args, **kwargs: None)

    def run_tool_method(self, method):
        def do_this_tool(args):
            """docstring"""
            # inspect the callable to determine how to call it
            arg_spec = inspect.getargspec(method)

            # we only know how to pass a flow_id and/or runner
            valid_arg_filter = lambda arg_name: (
                arg_name.endswith('flow_id') or 'runner' in arg_name
            )

            unhandled_arg_names = [
                arg_name for arg_name in arg_spec.args
                if not valid_arg_filter(arg_name)
            ]

            if unhandled_arg_names:
                raise NotImplementedError(unhandled_arg_names)

            runner_arg_name = self._get_arg_name(arg_spec, __contains__='runner')

            job_flow_arg_name = self._get_arg_name(
                arg_spec, endswith='flow_id'
            )

            kwargs = {}
            if runner_arg_name:
                self._ensure_runner_set()
                kwargs[runner_arg_name] = self.runner
            if job_flow_arg_name:
                self._ensure_job_flow_set()
                kwargs[job_flow_arg_name] = self.job_flow_id

            return method(**kwargs)

        return do_this_tool

    def onecmd(self, line):
        try:
            cmd.Cmd.onecmd(self, line)
        except EvaluationError, e:
            self._write_line(unicode(e))

    def do_set_job(self, job_flow_id):
        self.job_flow_id = job_flow_id

    def do_print(self, arg_string):
        self._write_line(self.job_flow_id)

    def do_EOF(self, arg_string):
        quit(0)

    def do_poo(self, arg_string):
        print arg_string

    def _write_line(self, string):
        self.stdout.write(unicode(string) + os.linesep)

    def _get_arg_name(self, arg_spec, **string_ops):
        for arg_name in arg_spec.args:
            for string_op, passed_arg in string_ops.iteritems():
                if not getattr(arg_name, string_op)(passed_arg):
                    continue
                return arg_name
        return None

    def _ensure_job_flow_set(self):
        if self.job_flow_id is None:
            raise NeedMoreInputError('job_flow_id')

    def _ensure_runner_set(self):
        self._ensure_job_flow_set()
        self.runner = EMRJobRunner(emr_job_flow_id=self.job_flow_id)


if __name__ == '__main__':
    option_parser = optparse.OptionParser()
    option_parser.add_option('--job-flow-id', dest='job_flow_id', default=None)
    options, _ = option_parser.parse_args()

    Shell(job_flow_id=options.job_flow_id).cmdloop()

