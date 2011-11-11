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
        data = json.loads(r.content)
        if data['status'] != 'OK':
            raise MRJobAPIException(data['error'])
        return data

    def post(self, cmd, data):
        r = requests.post(self.base + cmd, data=data)
        if self.debug:
            print r.content
        if not r.content:
            raise MRJobAPIException('No content at %s%s' % (self.base, cmd))
        data = json.loads(r.content)
        if data['status'] != 'OK':
            raise MRJobAPIException(data['error'])
        return data


class MRJobDaemonAPI(APIBase):

    def run_job(self, path, args):
        if '/' in path:
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


if __name__ == '__main__':
    api = MRJobDaemonAPI('http://127.0.0.1:5000')

    print 'stdout:'
    print api.get_stdout('mr_word_freq_count.sjohnson.20111111.012938.397848')

    print 'stderr:'
    print api.get_stderr('mr_word_freq_count.sjohnson.20111111.012938.397848')

    print 'bad stdout:'
    print api.get_stdout('doesnotexist')

    #print api.run_job('mrjob.examples.mr_word_freq_count.MRWordFreqCount',
    #                  ['-r', 'local',
    #                   '/nail/home/sjohnson/pg/mrjob/README.rst'])
