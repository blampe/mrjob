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

    def jobs(self):
        return self.get('/jobs')['jobs']

    def run_job(self, path, args):
        return self.post('/jobs',
                         data={
                             'args': json.dumps(args),
                             'path': path,
                         })['runner']


if __name__ == '__main__':
    api = MRJobDaemonAPI('http://127.0.0.1:5000')

    print api.run_job('mrjob.examples.mr_word_freq_count.MRWordFreqCount',
                      ['-r', 'local',
                       '/nail/home/sjohnson/pg/mrjob/README.rst'])
