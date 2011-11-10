# Copyright 2009-2010 Yelp
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
from __future__ import with_statement

from optparse import OptionParser

from mrjob.emr import EMRJobRunner

def main():
    usage = 'usage: %prog [options] JOB_FLOW_ID'
    description = (
        'Display status information for an EMR job flow.'
    )

    option_parser = OptionParser(usage=usage, description=description)

    options, args = option_parser.parse_args()

    job_flow_id = args[0]

    job_status = get_job_status(job_flow_id)

    print job_status

def get_job_status(emr_job_flow_id, **runner_kwargs):

    with EMRJobRunner(emr_job_flow_id=emr_job_flow_id, **runner_kwargs) as runner:
        return runner.job_status

if __name__ == '__main__':
    main()
