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
import sys

try:
    from flask import abort, Flask, redirect, request, Response, url_for
except ImportError:
    print >> sys.stderr, "mrjobd requires Flask."
    sys.exit(1)

from util import runner_to_json


app = Flask(__name__)


runners = {}


def json_response(data):
    return Response(json.dumps(data), mimetype='text/json')


@app.route('/', methods=['GET'])
def index():
    return redirect(url_for('jobs'))


@app.route('/jobs', methods=['GET', 'POST'])
def jobs():
    if request.method == 'POST':
        args_string = request.form['args']
        # make a job
        data = {'status': 'OK'}
        return json_response(data)
    else:
        data = {
            'jobs': dict((k, v) for k, v in runners.iteritems()),
            'status': 'OK',
        }
        return json_response(data)


## Starting the process


def main():
    app.debug = True
    app.run()


if __name__ == '__main__':
    main()
