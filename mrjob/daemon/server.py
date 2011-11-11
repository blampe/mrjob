# Copyright 2009-2011 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from imp import load_source
import json
from multiprocessing import Process, Queue
import optparse
import sys

try:
    from flask import abort, Flask, redirect, request, Response, url_for
except ImportError:
    print >> sys.stderr, "mrjobd requires Flask."
    sys.exit(1)

from mrjob.daemon.runner import run_job
from mrjob.daemon.util import runner_to_json
from mrjob.local import LocalMRJobRunner
from mrjob.util import log_to_stream


app = Flask(__name__)


wd = None
processes = set()


def queue_iter(queue):
    """Yield queue values until None is encountered"""
    while True:
        x = queue.get()
        if x is None:
            break
        else:
            yield x


def import_from_dotted_path(path):
    """For string 'x.y.z', return module z"""
    items = path.split('.')

    mod_path = '.'.join(items[:-1])
    mod = __import__(mod_path, globals(), locals(), [])
    for sub_item in items[1:]:
        try:
            mod = getattr(mod, sub_item)
        except AttributeError:
            raise AttributeError("Module %r has no attribute %r" % (mod, name))
    return mod


def import_from_system_path(path, classname):
    mod = load_source('module.name', path)
    try:
        return getattr(mod, classname)
    except AttributeError:
        raise AttributeError("Module %r has no attribute %r" % (mod, classname))


## URL handlers


def json_response(data):
    return Response(json.dumps(data), mimetype='text/json')


@app.route('/', methods=['GET'])
def index():
    return redirect(url_for('jobs'))


@app.route('/run_job', methods=['GET', 'POST'])
def jobs():
    if request.method == 'POST':
        args = json.loads(request.form['args'])
        path = request.form['path']

        if '/' in path:
            items = path.split(' ')
            path = ' '.join(items[:-1])
            classname = items[-1]
            process, info_queue = run_job(
                import_from_system_path(path, classname), args, wd)
        else:
            process, info_queue = run_job(
                import_from_dotted_path(path), args, wd)
        processes.add(process)

        job_name = info_queue.get()

        data = {
            'status': 'OK',
            'job_name': job_name,
        }
        return json_response(data)
    else:
        client_put.put({
            'command': 'list_jobs',
        })
        data = {
            'jobs': client_get.get(),
            'status': 'OK',
        }
        return json_response(data)


## Starting the process


def make_parser():
    parser = optparse.OptionParser()

    parser.add_option(
        '-c', '--conf-path', dest='conf_path', default=None,
        help="Path to alternate mrjob.conf file to read from")

    parser.add_option(
        '--daemon-host', dest='daemon_host', default=None,
        help="Host to listen on.")

    parser.add_option(
        '--daemon-port', dest='daemon_port', default=None,
        help="Port number to listen on.")

    parser.add_option(
        '--daemon-working-directory', dest='daemon_working_directory',
        default=None,
        help="Directory in which to store job state and output.")

    parser.add_option(
        '--debug', dest='debug', default=False, action='store_true',
        help="Turn on debugging")

    parser.add_option(
        '-v', '--verbose', dest='verbose', default=False, action='store_true',
        help="Verbose logging")
    return parser


def main():
    global wd

    parser = make_parser()
    options, args = parser.parse_args()
    if len(args) > 0:
        raise optparse.OptionError('Unknown options: %s' % args)

    runner_kwargs = options.__dict__.copy()

    del runner_kwargs['debug']
    del runner_kwargs['verbose']

    log_to_stream(name='mrjob', debug=options.verbose)
    runner = LocalMRJobRunner(**runner_kwargs)

    wd = runner._opts['daemon_working_directory']

    app.run(host=runner._opts['daemon_host'],
            port=runner._opts['daemon_port'],
            debug=options.debug)

    for process in processes:
        process.join()


if __name__ == '__main__':
    main()
