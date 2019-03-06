#!/usr/bin/env python3

"""
#
# Terrarium teplomer
#

python libs:
   - https://github.com/adafruit/Adafruit_Python_DHT
   - https://github.com/sourceperl/rpi.lcd-i2c

"""

import time
import Adafruit_DHT

import RPi_I2C_LCD
from RPi import GPIO
from w1thermsensor import W1ThermSensor

# DEBUG flag
DEBUG = False

dht11_pin = 18  #define GPIO 18 as DHT11 data pin
ds_sensor_id = '0000055d2b41'  # 0000055d2b41


def lcd_init():
  """init LCD """
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
  dht11_sensor_device = Adafruit_DHT.DHT11

  # LCD
  lcd = lcd_init()

  data = {
      'ds18b20': 0,
      'dht11':  0,
      'humi': 0}

  # inifinity loop
  while True:

    # DS18B20
    data['ds18b20'] = round(sensor.get_temperature(), 1)

    # DHT11
    temp_dht11, humi_dht11 = Adafruit_DHT.read_retry(dht11_sensor_device,
                                                     dht11_pin)

    if humi_dht11 is not None and temp_dht11 is not None:
      data['dht11'] = temp_dht11
      data['humi'] = temp_dht11

    if DEBUG:
      print("ds: {} dht: {}".format(data['ds18b20'], data['dht11']))

    # LCD line 1
    lcd_line1 = "T1:{:.1f}C T2:{:.1f}C".format(data['ds18b20'], data['dht11'])
    lcd.set_cursor(row=0)
    lcd.message(lcd_line1)

    # LCD line 2
    lcd_line2 = "humidity:{:.1f}%".format(data['humi'])
    lcd.set_cursor(row=1)
    lcd.message(lcd_line2)

    time.sleep(5) # 5 sec delay


if __name__ == '__main__':
  main()
