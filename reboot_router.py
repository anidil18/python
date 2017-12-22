#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Python script to Reboot LinkSys E4200 router

requirements.txt:
pip install requests

run:
change the hostname and admin password of Linksys router
"""

import requests
from base64 import b64encode
from json import loads
import logging
import sys

# !!! chage it !!!
# change hostname/IP of LinkSys router
host = '192.168.2.1'
# change admin password <password>
auth = ('admin',
        '<password>')

# logging.DEBUG
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


def main():
  # auth convert string > bytes > base64 bytes > decode str
  passphrase = b64encode(bytes(':'.join(auth), 'utf-8')).decode()

  url = "http://{:s}/JNAP/".format(str(host))
  task = {}

  headers = {'X-JNAP-Action': 'http://linksys.com/jnap/core/Reboot',
             'X-JNAP-Authorization': 'Basic {:s}'.format(str(passphrase))}

  r = requests.post(url,
                    #auth=auth,
                    json=task,
                    headers=headers,
                    verify=False)

  logging.debug(r.json())


if __name__ == "__main__":
  main()
