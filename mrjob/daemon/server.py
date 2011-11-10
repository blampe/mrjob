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
import sys

try:
    from flask import abort, Flask, redirect, request, Response, url_for
except ImportError:
    print >> sys.stderr, "mrjobd requires Flask."
    sys.exit(1)

from mrjob.daemon.runner import run_job
from mrjob.daemon.util import runner_to_json


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
    processes = {}
    queues = {}
    states = {}

    for cmd in queue_iter(server_get):
        if cmd is None:
            break
        if cmd['command'] == 'run_job':
            process, queue = run_job(cmd['path'], cmd['args'])

            job_name = queue.get()

            processes[job_name] = process
            queues[job_name] = queue
            server_put.put(job_name)

            for line in queue_iter(queue):
                sys.stdout.write(line)

            del queues[job_name]
            del processes[job_name]


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
            'command': 'run_job',
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
        data = {
            'jobs': dict((name, runner_to_json(runner))
                         for name, runner in runners.iteritems()),
            'status': 'OK',
        }
        return json_response(data)


## Starting the process


def main():
    global server, client_put, client_get
    client_put = Queue()
    client_get = Queue()
    server = Process(target=server_func, args=(client_put, client_get))
    server.start()

    app.debug = True
    app.run()

    server.join()


if __name__ == '__main__':
    main()
