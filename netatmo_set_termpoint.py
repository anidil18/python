#!/usr/bin/env python3

from __future__ import unicode_literals
from __future__ import print_function

import requests
import time

BASE_URL = "https://api.netatmo.net/"
GETTHERMO_REQ = BASE_URL + "api/getthermostatsdata"
SETTEHRMPOINT = BASE_URL + "api/setthermpoint"


def postRequest(url, params):

  # headers = {"Content-Type": "application/x-www-form-urlencoded;charset=utf-8"}

  resp = requests.post(url, data=params)

  # req = urllib2.Request(url=url, data=params, headers=headers)
  return resp.json()


def getthermostatsdata(access_token, device_id):
  postParams = {"access_token": access_token}
  postParams['device_id'] = device_id
  resp = postRequest(GETTHERMO_REQ, postParams)

  rawData = resp['body']['devices'][0]['modules'][0]

  temperature = rawData['measured']['temperature']
  print('temperature: {}'.format(temperature))
  setpoint_mode = rawData['setpoint']['setpoint_mode']
  if setpoint_mode == 'max':
    setpoint_temp = 'MAX'
  elif setpoint_mode == 'off':
    setpoint_temp = 'OFF'
  else:
    setpoint_temp = float(rawData['measured']['setpoint_temp'])
    print('setpoint_temp: {}'.format(setpoint_temp))

  if setpoint_mode == 'manual':
    setpoint_endpoint = rawData['setpoint']['setpoint_endtime']


def setthermpoint(access_token, device_id, module_id, setpoint_temp):

  setpoint_duration = 3600

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
  access_token = '54e9b301495a8887058e6e75|6aacccd909ce3a7099ea8a75b2cfff51'
  device_id = '70:ee:50:07:3d:ec'
  module_id = '04:00:00:07:3e:10'
  setpoint_temp = 24

  setthermpoint(access_token, device_id, module_id, setpoint_temp)
  getthermostatsdata(access_token, device_id)


if __name__ == "__main__":
  main()
