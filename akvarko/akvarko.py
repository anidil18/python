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
# import csv    # zapis temp values do csv
import Adafruit_DHT
# import max31865

import RPi_I2C_LCD
from RPi import GPIO
from w1thermsensor import W1ThermSensor

# DEBUG flag
DEBUG = False

DHT11_PIN = 17  #define GPIO 18 as DHT11 data pin
# DS_SENSOR_ID = ['0307977998fd', '03159779154e']

# MAX31865 (csPin,misoPin,mosiPin,clkPin)
# MAX_PIN = (8, 9, 10, 11)


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
  # ds_sensor = W1ThermSensor(W1ThermSensor.THERM_SENSOR_DS18B20, DS_SENSOR_ID)

  # DHT11
  dht11_sensor_device = Adafruit_DHT.DHT11

  # max31865
  # max_sensor = max31865.max31865(*MAX_PIN)

  # LCD
  lcd = lcd_init()

  data = {'dht11': 0, 'humi': 0}

  # inifinity loop
  while True:

    # DS18B20
    for sensor in W1ThermSensor.get_available_sensors():
      data[sensor.id] = sensor.get_temperature()

    if DEBUG:
      print("temp: {}".format(data.values()))

    # DHT11
    humidity, temperature = Adafruit_DHT.read_retry(
        dht11_sensor_device, DHT11_PIN)

    if humidity is not None and temperature is not None:
      data['dht11'] = temperature
      data['humi'] = humidity

    # LCD line 1
    lcd_line1 = "{:2.0f}C {:.1f}C {:.1f}C"
    lcd.set_cursor(row=0)
    lcd.message(
        lcd_line1.format(
            data['dht11'],
            data['03159779154e'],
            data['0307977998fd'])
            )

    # LCD line 2
    lcd_line2 = "humidity:{:2.0f}%"
    lcd.set_cursor(row=1)
    lcd.message(lcd_line2.format(data['humi']))

    time.sleep(30)  # sec delay


if __name__ == '__main__':
  main()
