#!/usr/bin/env python3
"""Test script"""

from __future__ import unicode_literals
from __future__ import print_function

import sys

REST_CLONE_PARAMETER = {
    'current_user': 'localhost',
    'method_name': None,
    'backup_name': None,
    'rman_until_time': None,
    'schedule_at_timespec': None,
    'step': None
}

API_CLONE_PARAMS = {
    'task_id': 'DUMMY', 'target_db': None, 'source_db': None,
    'target_hostname': None, 'source_hostname': None, 'source_is_RAC_YN': None,
    'target_is_RAC_YN': None}

# parameters_initial = [
#     ('C', 'task_id', taskid, 'N', 'OVERALL_DEFAULT',
#      'Y'), ('C', 'current_user', REST_CLONE_PARAMETER['current_user'], 'N',
# ]


def main(argv):
  """Main()"""

  parameters_initial = []
  for key, value in API_CLONE_PARAMS.items():
    temp = ('C', key, value, 'N', 'OVERALL_DEFAULT', 'Y')
    parameters_initial.append(temp)

  print(parameters_initial)


if __name__ == "__main__":
  main(sys.argv[1:])
