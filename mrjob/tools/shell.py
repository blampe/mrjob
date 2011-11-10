import cmd

class Shell(cmd.Cmd):

	def __init__(self, prompt='> ', *args, **kwargs):
		super(Shell, self).__init__(*args, **kwargs)
		self.prompt = prompt
		self.jobflow_id = None

	def preloop(self):
		super(Shell, self).preloop()

	def postloop(self):
		super(Shell, self).preloop()

	def precmd(line):
		if self.jobflow_id is None:
			print 'Please specify a jobflow id'
			return
		print
		return line.strip()

	def postcmd(self, stop, line):
		return super(Shell, self).postcmd(stop, line)

	def do_job(self, jobflow_id):
		self.jobflow_id = jobflow_id

