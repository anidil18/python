#!/usr/bin/python

from __future__ import absolute_import, division, print_function

import os
import glob
import re

from ansible.module_utils.basic import AnsibleModule

ANSIBLE_METADATA = {
    'metadata_version': '1.0',
    'status': ['preview'],
}

DOCUMENTATION = '''
---
module: asm_wwid
short_description: Get WWID of ASM dm device
options:
    path:
        description: asm disk multipath path
        required: true
author:
    - "Jiri Srba"
'''

EXAMPLES = '''
- name: Get wwn of ASM device
  asm_wwn:
    path: "/dev/mapper/asm_250FX_0296_CPSZB_FRA"
  register: asm_disks_result
'''

RETURN = '''
    description: Attributes of the Disk
    returned: success
    type: complex
    contains:
      name:
      path:
      prefix:
      dev:
      type: DATA / FRA
      partition: true / false
      wwn:
'''

def get_inode(path):
  ''' Get inode of realpath dm device '''
  return os.stat(os.path.realpath(path)).st_ino


def parse_multipath_name(path):
  """ Naparsuje asm dm device name a splitne dle znaku '_'
      return: storage, disk id, db, asm_data_type

      example: path = 'asm_G800_20D2_DBA_FRA'
  """

  dm_name = os.path.basename(path).split("_")

  prefix = "_".join([dm_name[0], dm_name[1]])

  # D01, DATA, FRA
  if re.search("^D0[1-9]$", dm_name[4].upper()):
    asm_data_type = dm_name[4].upper()
  elif dm_name[4].upper().startswith('DATA'):
    asm_data_type = 'DATA'
  elif dm_name[4].upper().startswith('FRA'):
    asm_data_type = 'FRA'
  else:
    asm_data_type = 'Unknown'

  return (prefix, dm_name[2], asm_data_type)


def get_all_disk_by_name():
  """Return dict {disk i-node: disk dm name}
  """

  dm_name = {}

  # get dict of dm-name
  # dm-name-asm_vmax250FX_4532_JIRKA_DATA -> ../../dm-2
  for disk_name in glob.glob('/dev/disk/by-id/dm-name-asm*'):
    # i-node jako lookup key
    key = get_inode(disk_name)
    dm_name[key] = os.path.basename(disk_name).replace('dm-name-', '')

  return dm_name

def get_wwid_partition(path):
  """ Zjisteni wwid a bool(partition) dle dm device a /dev/disk/by-id
      return: wwid, partition
  """

  # return list of unique WWID
  # dict jsem pouzil z duvodu duplicit disku s/bez partitions
  disks = {}

  # get dict { i-node: dm name }
  dm_name = get_all_disk_by_name()

  # i-node asm pro dohledani wwid
  asm_device_inode = [get_inode(x) for x in path]

  # To get WWID of LUN you can use the /dev/disk/by-id/dm-uuid-mpath- file:
  for dm_device in glob.glob('/dev/disk/by-id/dm-uuid-*'):
    dm_device_inode = get_inode(dm_device)

    # dle i-node of DM device detekuji wwid
    if dm_device_inode in asm_device_inode:
      short_dm_device = os.path.basename(dm_device)
      # strip dm-uuid-part1-mpath-
      wwid = re.sub(r'^dm-uuid-(part1-)?mpath-', '', short_dm_device)

      # vyrazeni duplict, pokud je disk uveden jako partition s i bez
      if wwid not in disks:

        # partition preved na non-part
        # dm-uuid-mpath-36000c29fc8c95ac1db84af09d6e5503d
        # dm-uuid-part1-mpath-360000970000297801441533030304532
        if 'part' in short_dm_device:
          dm_device_inode = get_inode(dm_device.replace('-part1-', '-'))

        # lookup name dle i-node
        name = dm_name[dm_device_inode]

        # get prefix, dev, type dle parse "name"
        prefix, dev, asm_data_type = parse_multipath_name(name)

        disks[wwid] = {
            "name": name,
            "wwn": wwid,
            "dev": dev,
            "path": os.path.join('/dev/mapper', name),
            "prefix": prefix,
            "type": asm_data_type,
            "partition": False
        }

  # convert dict to list of dict values
  return [v for v in disks.values()]


def main():

  fields = {"path": {"required": True, "type": "str"}}

  module = AnsibleModule(argument_spec=fields)

  path = module.params.get('path')

  # debug
  """
path = [
    "/dev/mapper/asm_vmax250FX_4532_JIRKA_DATA",
    "/dev/mapper/asm_vmax250FX_4532_JIRKA_DATA1",
    "/dev/mapper/asm_vmware_3A91_CLOUDB_DATA"
]
  """

  # assert kontrola na 'asm' v dm name
  if not all("asm" in s for s in path):
    msg = 'This is not an ASM device disk'
    module.fail_json(msg=msg, changed=False)

  response = get_wwid_partition(path)

  module.exit_json(changed=False, disks=response)


if __name__ == '__main__':
  main()
