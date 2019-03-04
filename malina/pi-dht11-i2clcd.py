#!/usr/bin/env python
# coding: utf-8

#
# Use Raspberry Pi to get temperature/humidity from DHT11 sensor
# Project Tutorial Url:http://osoyoo.com/2016/12/01/use-raspberry-pi-display-temperaturehumidity-to-i2c-lcd-screen/
# source: http://osoyoo.com/driver/pi-dht11-i2clcd.txt
#

import smbus
import time
import dht11
import RPi.GPIO as GPIO
from w1thermsensor import W1ThermSensor

dht_temp_sensor = 18  #define GPIO 18 as DHT11 data pin

w1_sensor_id = ""   # najdi cokoliv pripojeneho

# print T on stdout
DEBUG = False

# Define some device parameters
I2C_ADDR  = 0x27 # I2C device address, if any error, change this address to 0x27
LCD_WIDTH = 16   # Maximum characters per line

# Define some device constants
LCD_CHR = 1 # Mode - Sending data
LCD_CMD = 0 # Mode - Sending command

LCD_LINE_1 = 0x80 # LCD RAM address for the 1st line
LCD_LINE_2 = 0xC0 # LCD RAM address for the 2nd line
LCD_LINE_3 = 0x94 # LCD RAM address for the 3rd line
LCD_LINE_4 = 0xD4 # LCD RAM address for the 4th line

LCD_BACKLIGHT  = 0x08  # On
#LCD_BACKLIGHT = 0x00  # Off

ENABLE = 0b00000100 # Enable bit

# Timing constants
E_PULSE = 0.0005
E_DELAY = 0.0005

#Open I2C interface
#bus = smbus.SMBus(0)  # Rev 1 Pi uses 0
bus = smbus.SMBus(1) # Rev 2 Pi uses 1


def lcd_init():
  # Initialise display
  lcd_byte(0x33,LCD_CMD) # 110011 Initialise
  lcd_byte(0x32,LCD_CMD) # 110010 Initialise
  lcd_byte(0x06,LCD_CMD) # 000110 Cursor move direction
  lcd_byte(0x0C,LCD_CMD) # 001100 Display On,Cursor Off, Blink Off
  lcd_byte(0x28,LCD_CMD) # 101000 Data length, number of lines, font size
  lcd_byte(0x01,LCD_CMD) # 000001 Clear display
  time.sleep(E_DELAY)


def lcd_byte(bits, mode):
  # Send byte to data pins
  # bits = the data
  # mode = 1 for data
  #        0 for command

  bits_high = mode | (bits & 0xF0) | LCD_BACKLIGHT
  bits_low = mode | ((bits<<4) & 0xF0) | LCD_BACKLIGHT

  # High bits
  bus.write_byte(I2C_ADDR, bits_high)
  lcd_toggle_enable(bits_high)

  # Low bits
  bus.write_byte(I2C_ADDR, bits_low)
  lcd_toggle_enable(bits_low)


def lcd_toggle_enable(bits):
  # Toggle enable
  time.sleep(E_DELAY)
  bus.write_byte(I2C_ADDR, (bits | ENABLE))
  time.sleep(E_PULSE)
  bus.write_byte(I2C_ADDR,(bits & ~ENABLE))
  time.sleep(E_DELAY)


def lcd_string(message,line):
  # Send string to display

  message = message.ljust(LCD_WIDTH," ")

  lcd_byte(line, LCD_CMD)

  for i in range(LCD_WIDTH):
    lcd_byte(ord(message[i]),LCD_CHR)


def main():

  # Main program block
  GPIO.setwarnings(False)
  GPIO.setmode(GPIO.BCM)       # Use BCM GPIO numbers

  # display init
  lcd_init()

  # DHT11 init
  instance = dht11.DHT11(pin = dht_temp_sensor)

  # DS18B20
  sensor = W1ThermSensor(W1ThermSensor.THERM_SENSOR_DS18B20)

  ds_temp = "00.0"
  dht_temp = "00"

  while True:

    # get DHT11 sensor value
    result_dht11 = instance.read()

    # get DS18B20 temp value
    result_DS18B20 = sensor.get_temperature()
    ds_temp = str(round(result_DS18B20, 1))

    # Add dht11 values
    if result_dht11.is_valid():

      dht_temp = str(result_dht11.temperature)

      # humidity rovnou vypis na LCD
      lcd_line2 = "humi R: " + str(result_dht11.humidity) + "%"
      lcd_string(lcd_line2, LCD_LINE_2)

    # add T1 and T2 temp into LCD string line 1
    # print on LCD
    lcd_line1 = "T1:{} T2:{}C".format(ds_temp, dht_temp)

    if DEBUG:
      print(lcd_line1)

    lcd_string(lcd_line1, LCD_LINE_1)

    time.sleep(10)       # 10 second delay

    # lcd_string("Dortik a Emmet", LCD_LINE_2)
    # time.sleep(10)  # 5 second delay


if __name__ == '__main__':

  try:
    main()
  except KeyboardInterrupt:
    pass
  finally:
    lcd_byte(0x01, LCD_CMD)
