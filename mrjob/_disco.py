import os
import sys

import disco
from disco.core import Job, result_iterator
from mrjob.emr import EMRJobRunner


def map_init(input_iter, params):
	pass

def reduce_init(input_iter, params):
	pass

def mapper_runner(line_in, params):
	import Queue

	os.write(params['stdin_fds'][1], line_in)
	os.write(params['stdin_fds'][1], '\n')
	#print >> stderr2, 'did write.'
	#stderr2.flush()
	# print >> params['stdin'], line_in

	#print >> stderr2, 'about to readline.'
	#stderr2.flush()
	try:
		while True:
			line = params['queue'].get_nowait()
			#print >> stderr2, 'did readline:', line
			#stderr2.flush()
			k, v = line.split('\t')
			yield k, v
	except Queue.Empty:
		pass

def mapper_wrapper(fd, url, size, params):
	from subprocess import Popen, PIPE
	import os
	import os.path
	import Queue
	import thread

	def my_thread(params):
		while True:
			line = params['stdout'].readline()
			params['queue'].put(line)

        # run the process
	args = params['mapper_args']

	read_stdin, write_stdin = os.pipe()
	params['stdin_fds'] = (read_stdin, write_stdin)

	read_stdout, write_stdout = os.pipe()
	params['stdout_fds'] = (read_stdout, write_stdout)

	params['queue'] = Queue.Queue()

        proc = Popen(args, stdin=read_stdin, stdout=write_stdout, stderr=PIPE)

	# params['stdin'] = os.fdopen(write_stdin, 'w')
	params['stdout'] = os.fdopen(read_stdout, 'r')
        params['proc'] = proc

	thread.start_new_thread(my_thread, (params,))

	for line in fd:
		yield fd

	# TODO: Now close
	import wingdbstub; wingdbstub.debugger.Break()

def reducer_runner(*args, **kwargs):
	pass

class DiscoJobRunner(EMRJobRunner):
	def __init__(self, job, *args, **kwargs):
		self._job = job
		super(DiscoJobRunner, self).__init__(*args, **kwargs)

	def _create_s3_temp_bucket_if_needed(self):
		return

	def _run(self):
		steps = self._job.steps()
		steps = steps[0] # only support one for now


		self._setup_input()

		inputs = []
		s3_conn = self.make_s3_conn()
		for s3_input in self._s3_input_uris:
			key = self.get_s3_key(s3_input, s3_conn)
			url = key.generate_url(600, force_http=True)
			inputs.append(url)

		all_files = [each_file['path'] for each_file in self._files]
		all_files.append('/home/bchess/tricks/wingdbstub.py')
		all_files.extend(self._list_all_files('/home/bchess/disco/mrjob/mrjob'))

		wrapper_args = [self._opts['python_bin']]
		if self._wrapper_script:
			wrapper_args = [self._opts['python_bin'],
			self._wrapper_script['name']] + wrapper_args

		# specify the steps
		disco_jobs = []
		for i, step in enumerate(self._get_steps()):
			job_params = {}

			job_params['sys.path'] = sys.path
			if 'M' in step:
				job_params['mapper_args'] = wrapper_args + [self._script['path'],
					'--step-num=%d' % i, '--mapper'] + self._mr_job_extra_args()

			if 'R' in step:
				job_params['reducer_args'] = wrapper_args + [self._script['path'],
					'--step-num=%d' % i, '--reducer'] + self._mr_job_extra_args()

			job = Job().run(input=inputs,
				map_init=map_init,
				map=mapper_runner,
				reduce_init=reduce_init,
				reduce=reducer_runner,
				required_files=all_files,
				required_modules=[],
				sorted=True,
				map_input_stream=[
					disco.worker.classic.func.map_input_stream,
					disco.worker.classic.func.gzip_line_reader,
					mapper_wrapper
					],
				params=job_params
			)
			disco_jobs.append(job)

		for word, count in result_iterator(job.wait(show=True)):
			print word, count

	def _list_all_files(self, path):
		for dirpath, dirnames, filenames in os.walk(path):
			for each_filename in filenames:
				yield os.path.join(dirpath, each_filename)


