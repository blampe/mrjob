import cmd
import os

FETCH_LOGS = 'fetch_logs'
GET_STATUS = 'get_status'

class Shell(cmd.Cmd):
    available_tools = set([
        'fetch_logs',
        'get_status',
    ])

    # shorter command names for convenience
    aliases = dict(
            logs=FETCH_LOGS,
            status=GET_STATUS,
    )

    def __init__(self, prompt='> ', *args, **kwargs):
        cmd.Cmd.__init__(self, *args, **kwargs)
        self.prompt = prompt
        self.jobflow_id = None

        # add `do_script_name` methods to ourselves
        for tool_name in self.available_tools:
            setattr(self, 'do_%s' % tool_name, self.run_tool_method(tool_name))

        # create methods for aliases
        for aliased_name, true_method_name in self.aliases.iteritems():
            true_method = getattr(self, 'do_%s' % true_method_name)
            setattr(self, 'do_%s' % aliased_name, true_method)

    def run_tool_method(self, tool_name):
        def do_this_tool(args):
            """docstring"""
            self._writeline(tool_name)
            self._writeline(args)

            tool_module = __import__('emr.%s' % tool_name, globals(), locals(), [tool_name], -1)
            tool_module.main(self.jobflow_id)

        return do_this_tool

    def precmd(self, line):
        stripped_line = line.strip()
        # do anything else?
        return stripped_line

    def do_job(self, jobflow_id):
        self.jobflow_id = jobflow_id

    def do_print(self, line):
        self._writeline(self.jobflow_id)

    def do_EOF(self, line):
        quit(0)

    def _writeline(self, string):
        self.stdout.write(string + os.linesep)

if __name__ == '__main__':
    Shell().cmdloop()

