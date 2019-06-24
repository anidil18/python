#!/usr/bin/env python2

"""
Sources:

https://docs.ansible.com/ansible-tower/3.2.5/html/towerapi/launch_jobtemplate.html
https://docs.ansible.com/ansible/2.4/dev_guide/developing_api.html
https://github.com/ansible/ansible/blob/devel/lib/ansible/cli/playbook.py


"""


from __future__ import (absolute_import, division,
                        print_function, unicode_literals)

import json
from collections import namedtuple
from ansible.parsing.dataloader import DataLoader
from ansible.vars.manager import VariableManager
from ansible.inventory.manager import InventoryManager
from ansible.playbook.play import Play
from ansible.executor.task_queue_manager import TaskQueueManager
from ansible.plugins.callback import CallbackBase
from ansible.utils.display import Display

from config import TOWER_API, FLASK_HOST_ANSIBLE, FLASK_ANSIBLE_PORT, FLASK_DEBUG
from config import ANSIBLE_DIR, ANSIBLE_INVENTORY

from flask import request, jsonify
from app import app

def parse_json(http_request):
  """Parse HTTP request
  """
  request_json = http_request.get_json(silent=True)
  return request_json


def run(playbook):
  """Run ansible playbook
  """
  pass


@app.route(TOWER_API + '/<int:job_template_id>/launch', methods=['GET','POST'])
@requires_auth
def launch_playbook(job_template_id):
  """Launch ansible playbook <id>
  """

  app.logger.info('job_template_id: %s', job_template_id)

  rest_params = parse_json(request)
  app.logger.info('rest params: %s', rest_params)


  return jsonify({'data': job_template_id })

if __name__ == "__main__":
  app.run(host=FLASK_HOST_ANSIBLE, port=FLASK_ANSIBLE_PORT, debug=FLASK_DEBUG)
