#!/usr/bin/env python3

"""
#
# Terrarium teplomer
#

python libs:
   - https://github.com/adafruit/Adafruit_Python_DHT
   - https://github.com/sourceperl/rpi.lcd-i2c
   - https://github.com/steve71/MAX31865

"""

import time
import Adafruit_DHT
import max31865

import RPi_I2C_LCD
from RPi import GPIO
from w1thermsensor import W1ThermSensor

# DEBUG flag
DEBUG = False

DHT11_PIN = 18  #define GPIO 18 as DHT11 data pin
DS_SENSOR_ID = '0000055d2b41'  # 0000055d2b41

# MAX31865 (csPin,misoPin,mosiPin,clkPin)
MAX_PIN = (8, 9, 10, 11)


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
  ds_sensor = W1ThermSensor(W1ThermSensor.THERM_SENSOR_DS18B20, DS_SENSOR_ID)

  # DHT11
  dht11_sensor_device = Adafruit_DHT.DHT11

  # max31865
  max_sensor = max31865.max31865(*MAX_PIN)

  # LCD
  lcd = lcd_init()

  data = {
      'ds18b20': 0,
      'pt100': 0,
      'dht11':  0,
      'humi': 0}

  # inifinity loop
  while True:

    # DS18B20
    data['ds18b20'] = ds_sensor.get_temperature()

    # PT100 max31865
    data['pt100'] = max_sensor.readTemp()

    if DEBUG:
      print("ds: {} pt100: {}".format(data['ds18b20'], data['pt100']))

    # DHT11
    temp_dht11, humi_dht11 = Adafruit_DHT.read_retry(dht11_sensor_device,
                                                     DHT11_PIN)

    if humi_dht11 is not None and temp_dht11 is not None:
      data['dht11'] = temp_dht11
      data['humi'] = temp_dht11

    # LCD line 1
    lcd_line1 = "{:.1f}C {:.1f}C {:.0f}C"
    lcd.set_cursor(row=0)
    lcd.message(lcd_line1
                .format(data['ds18b20'], data['pt100'], data['dht11']))

    # LCD line 2
    lcd_line2 = "humidity:{:.1f}%"
    lcd.set_cursor(row=1)
    lcd.message(lcd_line2.format(data['humi']))

    time.sleep(5) # 5 sec delay


if __name__ == '__main__':
  main()
