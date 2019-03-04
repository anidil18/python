#!/usr/bin/env python3

"""
#
# Terrarium teplomer
#

python libs:
   - https://github.com/szazo/DHT11_Python
   - https://github.com/sourceperl/rpi.lcd-i2c

"""

import time
import dht11

import RPi_I2C_LCD
from RPi import GPIO
from w1thermsensor import W1ThermSensor

# DEBUG flag
DEBUG = False

dht_temp_sensor = 18  #define GPIO 18 as DHT11 data pin
ds_sensor_id = '0000055d2b41'  # 0000055d2b41


def lcd_init():
  """init LCD
  """
  lcd = RPi_I2C_LCD.LCD()
  lcd.set_backlight(True)
  lcd.clear()
  return lcd


def main():

  # initialize GPIO
  GPIO.setwarnings(False)
  GPIO.setmode(GPIO.BCM)
  GPIO.cleanup()

  # DS18B20
  sensor = W1ThermSensor(W1ThermSensor.THERM_SENSOR_DS18B20, ds_sensor_id)

  # DHT11
  instance = dht11.DHT11(pin=dht_temp_sensor)

  # LCD
  lcd = lcd_init()

  temp = {
      'ds18b20': 0,
      'dht11':  0,
      'humi': 0}

  # inifinity loop
  while True:

    # DS18B20
    temp['ds18b20'] = round(sensor.get_temperature(), 1)

    # DHT11
    dht11_result = instance.read()

    if dht11_result.is_valid():
      temp['dht11'] = round(dht11_result.temperature, 1)
      temp['humi'] = round(dht11_result.humidity, 1)

    if DEBUG:
      print("ds: {} dht: {}".format(temp['ds18b20'], temp['dht11']))

    # LCD line 1
    lcd_line1 = "T1:{:.1f}C T2:{:.1f}C".format(
        temp['ds18b20'], temp['dht11'])
    lcd.set_cursor(row=0)
    lcd.message(lcd_line1)

    # LCD line 2
    lcd_line2 = "humidity:{:.1f}%".format(temp['humi'])
    lcd.set_cursor(row=1)
    lcd.message(lcd_line2)

    time.sleep(5) # 5 sec delay


if __name__ == '__main__':
  main()
