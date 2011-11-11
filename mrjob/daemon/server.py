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


server = None
client_put, client_get = None, None


def queue_iter(queue):
    while True:
        x = queue.get()
        if x is None:
            break
        else:
            yield x


def server_func(server_get, server_put):
    processes = set()

    def cmd_run_job_from_module(path, args):
        process, info_queue = run_job(path, args)
        processes.add(process)

        job_name = info_queue.get()
        server_put.put(job_name)

    def cmd_list_jobs():
        server_put.put(list(states.values()))


    commands = {
        'run_job_from_module': cmd_run_job_from_module,
        'list_jobs': cmd_list_jobs,
    }

    for cmd_dict in queue_iter(server_get):
        if cmd_dict is None:
            break

        cmd = cmd_dict['command']
        del cmd_dict['command']

        commands[cmd](**cmd_dict)

    for process in processes:
        process.join()


## web requests


def json_response(data):
    return Response(json.dumps(data), mimetype='text/json')


@app.route('/', methods=['GET'])
def index():
    return redirect(url_for('jobs'))


@app.route('/jobs', methods=['GET', 'POST'])
def jobs():
    if request.method == 'POST':
        client_put.put({
            'command': 'run_job_from_module',
            'args': json.loads(request.form['args']),
            'path': request.form['path'],
        })

        job_name = client_get.get()

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
    parser.add_option('-c', '--conf-path', dest='conf_path', default=None,
                      help="Path to alternate mrjob.conf file to read from")
    parser.add_option('--debug', dest='debug', default=False, action='store_true',
                      help="Turn on debugging")
    parser.add_option(
        '--daemon-host', dest='daemon_host', default=None,
        help="Host to listen on.")

    parser.add_option(
        '--daemon-port', dest='daemon_port', default=None,
        help="Port number to listen on.")
    parser.add_option('-v', '--verbose', dest='verbose', default=False,
                      action='store_true',
                      help="Verbose logging")
    return parser


def main():
    global manager, server, client_put, client_get

    parser = make_parser()
    options, args = parser.parse_args()
    if len(args) > 0:
        raise optparse.OptionError('Unknown options: %s' % args)

    client_put = Queue()
    client_get = Queue()
    server = Process(target=server_func, args=(client_put, client_get))
    server.start()

    runner_kwargs = options.__dict__.copy()

    del runner_kwargs['debug']
    del runner_kwargs['verbose']

    log_to_stream(name='mrjob', debug=options.verbose)
    runner = LocalMRJobRunner(**runner_kwargs)

    app.run(host=runner._opts['daemon_host'],
            port=runner._opts['daemon_port'],
            debug=options.debug)

    server.join()


if __name__ == '__main__':
    main()
