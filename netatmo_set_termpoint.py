#!/usr/bin/env python3

from __future__ import unicode_literals
from __future__ import print_function

import logging
import time
import json
from os.path import expanduser, exists
import requests

logging.basicConfig(level=logging.INFO)

BASE_URL = "https://api.netatmo.net/"
AUTH_REQ = BASE_URL + "oauth2/token"
GETTHERMO_REQ = BASE_URL + "api/getthermostatsdata"
SETTEHRMPOINT = BASE_URL + "api/setthermpoint"

cred = {"client_id" :  "",
        "client_secret" : "",
        "username" : "",
        "password" : ""
       }

CREDENTIALS = expanduser("~/netatmo.config")

def getParameter(key, default):
  return getenv(key, default[key])

if exists(CREDENTIALS):
  with open(CREDENTIALS, "r") as f:
    cred.update(json.loads(f.read()))
    logging.debug('credentials: %s', cred)


def postRequest(url, params):
  """Submit Post request"""

  resp = requests.post(url, data=params)
  return resp.json()


def client_auth(cred):
  """auth """

  postParams = {
      "grant_type": "password",
      "client_id": cred['client_id'],
      "client_secret": cred['client_secret'],
      "username": cred['username'],
      "password": cred['password'],
      "scope": "read_thermostat write_thermostat"
  }

  resp = postRequest(AUTH_REQ, postParams)
  logging.debug('resp: %s', resp)
  access_token = resp['access_token']
  refresh_token = resp['refresh_token']
  scope = resp['scope']
  expiration = int(resp['expire_in'] + time.time())

  return access_token


def get_thermostat_data(access_token, device_id):

  post_params = {"access_token": access_token,
                 "device_id": device_id}
  resp = postRequest(GETTHERMO_REQ, post_params)

  for device in resp['body']['devices']:
    logging.info('device: {}'.format(device['_id']))
    for module in device['modules']:
      logging.info('module: {}'.format(module['module_name']))

      temperature = module['measured']['temperature']
      setpoint_mode = module['setpoint']['setpoint_mode']
      if setpoint_mode == 'max':
        setpoint_temp = 'max'
      elif setpoint_mode == 'off':
        setpoint_temp = 'off'
      else:
        setpoint_temp = float(module['measured']['setpoint_temp'])

      logging.info('  - temperature: {}'.format(temperature))
      logging.info('  - setpoint_temp: {}'.format(setpoint_temp))

  if setpoint_mode in ['manual', 'max']:
    setpoint_endpoint = module['setpoint']['setpoint_endtime']


def set_term_to_max(access_token, device_id, module_id, setpoint_duration):

  postParams = {
      "access_token": access_token,
      "device_id": device_id,
      "module_id": module_id,
      "setpoint_mode": "max"
  }

  endtime = time.time() + float(setpoint_duration)
  postParams['setpoint_endtime'] = endtime

  resp = postRequest(SETTEHRMPOINT, postParams)


def set_term(access_token, device_id, module_id, setpoint_temp,
             setpoint_duration):

  postParams = {"access_token": access_token}
  postParams['device_id'] = device_id
  postParams['module_id'] = module_id
  postParams['setpoint_mode'] = 'manual'
  postParams['setpoint_temp'] = setpoint_temp

  endtime = time.time() + float(setpoint_duration)
  postParams['setpoint_endtime'] = endtime

  resp = postRequest(SETTEHRMPOINT, postParams)


def main():
  """Main() """

  device_id = '70:ee:50:07:3d:ec'
  module_id = '04:00:00:07:3e:10'
  setpoint_temp = 24
  setpoint_duration = 86400

  access_token = client_auth(cred)

  set_term_to_max(access_token, device_id, module_id, setpoint_duration)
  get_thermostat_data(access_token, device_id)


if __name__ == "__main__":
  main()
