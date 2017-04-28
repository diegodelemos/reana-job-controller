# -*- coding: utf-8 -*-
#
# This file is part of REANA.
# Copyright (C) 2017 CERN.
#
# REANA is free software; you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# REANA is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# REANA; if not, write to the Free Software Foundation, Inc., 59 Temple Place,
# Suite 330, Boston, MA 02111-1307, USA.
#
# In applying this license, CERN does not waive the privileges and immunities
# granted to it by virtue of its status as an Intergovernmental Organization or
# submit itself to any jurisdiction.

"""Rest API endpoint for job management."""

import copy
import logging
import threading
import uuid

from flask import Flask, abort, jsonify, request

from reana_job_controller.k8s import (create_api_client, instantiate_job,
                                      watch_jobs, watch_pods)
from spec import openapi_spec

app = Flask(__name__)
app.secret_key = "mega secret key"
JOB_DB = {}

openapi = openapi_spec(app)


def filter_jobs(job_db):
    """Filter unsolicited job_db fields.

    :param job_db: Dictionary which contains all jobs.
    :returns: A copy of `job_db` without `obj`, `deleted` and `pod`
        fields.
    """
    job_db_copy = copy.deepcopy(job_db)
    for job_name in job_db_copy:
        del(job_db_copy[job_name]['obj'])
        del(job_db_copy[job_name]['deleted'])
        if job_db_copy[job_name].get('pod'):
            del(job_db_copy[job_name]['pod'])

    return job_db_copy


@app.route('/jobs', methods=['GET'])
def get_jobs():
    """Get all jobs.

    ---
    get:
      description: Get all Jobs
      produces:
       - application/json
      responses:
        200:
          description: Job list.
          schema:
            type: array
            items:
              $ref: '#/definitions/Job'
          examples:
            application/json:
              {
                "jobs": {
                  "1612a779-f3fa-4344-8819-3d12fa9b9d90": {
                    "cmd": "sleep 1000",
                    "cvmfs_mounts": [
                      "atlas-condb",
                      "atlas"
                    ],
                    "docker-img": "busybox",
                    "experiment": "atlas",
                    "job-id": "1612a779-f3fa-4344-8819-3d12fa9b9d90",
                    "max_restart_count": 3,
                    "restart_count": 0,
                    "status": "succeeded"
                  },
                  "2e4bbc1d-db5e-4ee0-9701-6e2b1ba55c20": {
                    "cmd": "sleep 1000",
                    "cvmfs_mounts": [
                      "atlas-condb",
                      "atlas"
                    ],
                    "docker-img": "busybox",
                    "experiment": "atlas",
                    "job-id": "2e4bbc1d-db5e-4ee0-9701-6e2b1ba55c20",
                    "max_restart_count": 3,
                    "restart_count": 0,
                    "status": "started"
                  }
                }
              }
    """
    return jsonify({"jobs": filter_jobs(JOB_DB)}), 200


@app.route('/jobs', methods=['POST'])
def create_job():
    """Create a new job.

    ---
    post:
      summary: |-
        This resource is expecting JSON data with all the necessary
        information of a new job.
      description: Create a new Job.
      operationId: create_job
      consumes:
       - application/json
      produces:
       - application/json
      parameters:
       - name: job
         in: body
         description: Information needed to instantiate a Job
         required: true
         schema:
           $ref: '#/definitions/JobRequest'
      responses:
        201:
          description: The Job has been created.
          schema:
            type: object
            properties:
              job-id:
                type: string
          examples:
            application/json:
              {
                "job-id": "cdcf48b1-c2f3-4693-8230-b066e088c6ac"
              }
        400:
          description: Invalid request - probably malformed JSON
        500:
          description: Internal error - probably the job could not be allocated
    """

    if not request.json \
       or not ('experiment') in request.json\
       or not ('docker-img' in request.json):
        print(request.json)
        abort(400)

    cmd = request.json['cmd'] if 'cmd' in request.json else None
    env_vars = (request.json['env-vars']
                if 'env-vars' in request.json else {})

    if request.json.get('cvmfs_mounts'):
        cvmfs_repos = request.json.get('cvmfs_mounts')
    else:
        cvmfs_repos = []

    job_id = str(uuid.uuid4())

    job_obj = instantiate_job(job_id,
                              request.json['docker-img'],
                              cmd,
                              cvmfs_repos,
                              env_vars,
                              request.json['experiment'],
                              shared_file_system=True)

    if job_obj:
        job = copy.deepcopy(request.json)
        job['job-id'] = job_id
        job['status'] = 'started'
        job['restart_count'] = 0
        job['max_restart_count'] = 3
        job['obj'] = job_obj
        job['deleted'] = False
        JOB_DB[job_id] = job
        return jsonify({'job-id': job_id}), 201
    else:
        return jsonify({'job': 'Could not be allocated'}), 500


@app.route('/jobs/<job_id>', methods=['GET'])
def get_job(job_id):
    """Get a Job.

    ---
    get:
      description: Get a Job by its id
      produces:
       - application/json
      parameters:
       - name: job_id
         in: path
         description: ID of the Job
         required: true
         type: string
      responses:
        200:
          description: The Job.
          schema:
            $ref: '#/definitions/Job'
          examples:
            application/json:
              "job": {
                "cmd": "sleep 1000",
                "cvmfs_mounts": [
                  "atlas-condb",
                  "atlas"
                ],
                "docker-img": "busybox",
                "experiment": "atlas",
                "job-id": "cdcf48b1-c2f3-4693-8230-b066e088c6ac",
                "max_restart_count": 3,
                "restart_count": 0,
                "status": "started"
              }
        404:
          description: The Job does not exist.
    """
    if job_id in JOB_DB:
        job_copy = copy.deepcopy(JOB_DB[job_id])
        del(job_copy['obj'])
        del(job_copy['deleted'])
        if job_copy.get('pod'):
            del(job_copy['pod'])
        return jsonify({'job': job_copy}), 200
    else:
        abort(404)


openapi.add_path(view=get_jobs)
openapi.add_path(view=get_job)
openapi.add_path(view=create_job)

@app.route("/openapi")
def apispec():
    return jsonify(openapi.to_dict())

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(threadName)s - %(levelname)s: %(message)s'
    )
    app.config.from_object('config')
    """
    job_event_reader_thread = threading.Thread(target=watch_jobs,
                                               args=(JOB_DB,
                                                     app.config['PYKUBE_API']))

    job_event_reader_thread.start()
    pod_event_reader_thread = threading.Thread(target=watch_pods,
                                               args=(JOB_DB,
                                                     app.config['PYKUBE_API']))
    app.config['PYKUBE_CLIENT'] = create_api_client(app.config['PYKUBE_API'])
    pod_event_reader_thread.start()
    """
    app.run(debug=True, port=5000,
            host='0.0.0.0')
