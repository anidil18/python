#!/usr/bin/python

import dbus

level = 0
bus = dbus.SessionBus()
proxy = bus.get_object('org.gnome.SettingsDaemon',
                       '/org/gnome/SettingsDaemon/Power')
#print proxy
iface = dbus.\
  Interface(proxy, dbus_interface='org.gnome.SettingsDaemon.Power.Screen')

# Set brightness:
try:
    iface.SetPercentage(level)
except:
    print "Wrong brightness input: \nplease enter int from 1 to 100"