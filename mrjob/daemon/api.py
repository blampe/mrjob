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
import os
import time

import requests


class MRJobAPIException(Exception):
    pass


class APIBase(object):

    def __init__(self, base='http://127.0.0.1:5000'):
        self.base = base
        self.debug = False

    def get(self, cmd):
        r = requests.get(self.base + cmd)
        if self.debug:
            print r.content
        if not r.content:
            raise MRJobAPIException('No content at %s%s' % (self.base, cmd))

        try:
            data = json.loads(r.content)
            if data['status'] != 'OK':
                raise MRJobAPIException(data['error'])
        except:
            raise MRJobAPIException(r.content)

        return data

    def post(self, cmd, data):
        r = requests.post(self.base + cmd, data=data)
        if self.debug:
            print r.content
        if not r.content:
            raise MRJobAPIException('No content at %s%s' % (self.base, cmd))

        try:
            data = json.loads(r.content)
            if data['status'] != 'OK':
                raise MRJobAPIException(data['error'])
        except:
            raise MRJobAPIException(r.content)

        return data


class MRJobDaemonAPI(APIBase):

    def run_job(self, path, args):
        if '/' in path:
            # this actually does not work because of the class name being
            # included in the path.
            # fixme
            return self.post('/run_job',
                             data={
                                 'args': json.dumps(args),
                                 'path': os.path.abspath(path),
                             })['job_name']
        else:
            return self.post('/run_job',
                             data={
                                 'args': json.dumps(args),
                                 'path': path,
                             })['job_name']

    def get_stdout(self, job_name):
            return self.get('/%s/stdout' % job_name)['stdout']

    def get_stderr(self, job_name):
            return self.get('/%s/stderr' % job_name)['stderr']

    def get_status(self, job_name):
            return json.loads(self.get('/%s/status' % job_name)['job_status'])


if __name__ == '__main__':
    api = MRJobDaemonAPI('http://127.0.0.1:5000')

    job_name = api.run_job('mrjob.examples.mr_word_freq_count.MRWordFreqCount',
                           ['-r', 'emr', '--emr-job-flow-id', 'j-3LD0PP3C9B0S9',
                            '/nail/home/sjohnson/pg/mrjob/README.rst'])

    status = dict(in_progress=True)

    while status is None or status['in_progress']:
        time.sleep(1)
        status = api.get_status(job_name)
        print status

    print api.get_stdout(job_name)
