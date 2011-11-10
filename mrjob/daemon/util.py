def local_runner_to_json(runner):
    return {
        'name': runner._job_name,
        'type': 'local',
    }

runner_to_json = local_runner_to_json
