import cmd
import os
import inspect
import optparse
import tempfile
import shutil

from mrjob.emr import EMRJobRunner
from mrjob.emr import LogFetchException
from mrjob.daemon.api import MRJobDaemonAPI
from mrjob.daemon.api import MRJobAPIException

from mrjob.tools.emr import fetch_logs
from mrjob.tools.emr import get_status
from mrjob.tools.emr import mrboss

from boto.exception import EmrResponseError

FETCH_LOGS = 'fetch_logs'
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

        'test': (
            get_status.test,
        ),
    }

class EvaluationError(Exception):
    pass

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
            job=SET_JOB,
    )


    # todo: option

    def __init__(self, job_flow_id=0, prompt='> ', *args, **kwargs):
        cmd.Cmd.__init__(self, *args, **kwargs)
        self.prompt = prompt
        self.runner = None
        self.job_names = []

        self.do_set_job(job_flow_id)

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
        except (EvaluationError, MRJobAPIException, LogFetchException), e:
            self._write_line('***Error: ' + unicode(e))
        except EmrResponseError, e:
            self._write_line('***Error: ' + e.error_message)

    def do_set_job(self, job_flow_id):
        self.job_flow_id = job_flow_id
        self._ensure_runner_set()

    def do_start(self, args):
        """Start an MR job.

            > start mrjob.examples.mr_word_freq_count.MRWordFreqCount -r local /nail/home/bryce/pg/mrjob/README.rst

            TODO: Tab completion would be great!
        """
        job_runner, job_args = self._get_command_and_args(args, require_args=True)

        self._ensure_runner_set()

        self._write_line('Submitting job...')
        self.job_names.append(self.api.run_job(job_runner, job_args.split()))
        self._write_line('Submitted job \'%s\'' % self.job_names[-1])

#    def do_complete(self, text):
#       pass

    def do_jobs(self, arg_string):
        """Show currently tracked jobs."""
        self._write_line('Current job flow id is: %s' % self.job_flow_id)

        if not self.job_names:
            return

        self._write_line('Submitted jobs are:')
        for job_name in self.job_names:
            self._write_line('\t' + job_name)

    def do_shell(self, arg_string):
        """Run a shell command on all nodes.

            > !ps -ef
        """
        self._ensure_runner_set()
        tmp_dir = tempfile.mkdtemp(prefix='mrboss-')
        self._write_line('Running \'%s\' on all nodes...' % arg_string)
        mrboss.run_on_all_nodes(self.runner, tmp_dir, arg_string.split())
        self._write_line('Output saved to %s.' % tmp_dir)
#        shutil.rmtree(tmp_dir)

    def do_status(self, job_name):
        if not job_name:
            job_name = self.job_names[-1]

        self._write_line('Fetching status for \'%s\'' % job_name)
        self._ensure_runner_set()

        status = self.api.get_status(job_name)

        if self.job_flow_id != status['job_flow_id']:
            self.job_flow_id = status['job_flow_id']

        self._ensure_runner_set()

    def do_EOF(self, arg_string):
        self.do_quit(arg_string)

    def do_quit(self, arg_string):
        quit(0)

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
        pass
        #if self.job_flow_id is None: # not using this
         #   raise NeedMoreInputError('job_flow_id=%s' % self.job_flow_id)

    def _ensure_runner_set(self):
        self._ensure_job_flow_set()
        if not self.runner or (self.runner and self.runner._emr_job_flow_id != self.job_flow_id):
            self.runner = EMRJobRunner(emr_job_flow_id=self.job_flow_id)

        self.api = MRJobDaemonAPI('http://' +
            self.runner._opts['daemon_host'] + ':' + self.runner._opts['daemon_port']
        )
        self.api.debug = True

    def _get_command_and_args(self, arg_string, require_args=False):
        split_args = arg_string.split(' ', 1)
        if require_args and len(split_args) < 2:
            raise NeedMoreInputError(arg_string)

        command_name = split_args[0]

        if len(split_args) > 1:
            command_args = split_args[1]
        else:
            command_args = ''

        return command_name, command_args

if __name__ == '__main__':
    option_parser = optparse.OptionParser()
    option_parser.add_option('--job-flow-id', dest='job_flow_id', default=0)
    options, _ = option_parser.parse_args()

    shell = Shell(job_flow_id=options.job_flow_id)

    try:
        shell.cmdloop()
    except KeyboardInterrupt:
        shell.do_quit('')

