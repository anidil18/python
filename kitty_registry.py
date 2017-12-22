#
# Kitty registry
# 
# - replace Kitty setting in registry
# - replace and remove -m prefix in hostname in putty registry
# - copy Registry keys from Kitty to Putty
#

import re
from winreg import *

# print debug values
debug = 1

# set up registry value in key-value pair format
registry = {
    'HostName':'mgmt',      # mgmt/data
    #'UserNameFromEnvironment':1,
    #'PublicKeyFile':r'C:\Users\sol60210\.ssh\id_rsa.ppk',
    'Beep':1
    }
# hostname bez mgmt lan	
hostname_exception = ["ovo","racek1","racek2",
				"ktpdb2","retdb","retdb-test",
				"vmasrac1.vs.csin.cz","vmasrac2.vs.csin.cz",
				"vmbudrac1.vs.csin.cz","vmbudrac2.vs.csin.cz"]
				
KITTY_REGISTRY = "Software\\9bis.com\\kiTTY\\Sessions"
PUTTY_REGISTRY = "Software\\SimonTatham\\PuTTY\\Sessions"

def getValue(subkey, key_name):
    val = QueryValueEx(subkey, key_name)
    return val[0]
    
def changeRegistry(subkey, key_name, new_value):
    val = QueryValueEx(subkey, key_name)
    if val[0] != new_value:
        if debug: print ("setting "+str(new_value))
        SetValueEx(subkey,key_name,0, val[1], new_value)

def checkPuttyKey(subkey):
    # testni, zda klic existuje
    try:
        key = OpenKey(HKEY_CURRENT_USER,subkey, 0, KEY_WRITE)
        CloseKey(key)
    except WindowsError:
        # pokud ne, tak ho vytvor
        print (putty_subkey + " not exists")
        CreateKey(HKEY_CURRENT_USER,subkey)
        pass

def validIP(address):
    parts = address.split(".")
    if len(parts) != 4:
        return False
    for x in parts:
        if not x.isdigit():
            return False
    return True        

# registry Putty sessions
key = OpenKey(HKEY_CURRENT_USER,KITTY_REGISTRY)

try:
    i = 0
    while True:
        subkey = EnumKey(key, i)

        # copy registry entries from Kitty to putty without setting for launchy
        putty_subkey=PUTTY_REGISTRY+"\\"+subkey
        checkPuttyKey(putty_subkey)

        # loop thru the keys in server settings
        subkey = OpenKey(key,subkey, 0, KEY_ALL_ACCESS)

        for key_name, new_value in registry.items():
            # change hostname to data or mgmt
            if key_name == 'HostName':
                hostname = getValue(subkey, key_name)
                # exceptions, like ovo or IP address in HostName
                if not ((hostname in hostname_exception) or (validIP(hostname) is True)):
                    if new_value == 'data':
                        hostname = re.sub(r'(\w+)(-m)?(.*)',r'\1\3',hostname)
                    elif new_value == 'mgmt':
                        hostname = re.sub(r'(\w+)(-m)?(.*)',r'\1-m\3',hostname)
                    
                #set hostname into new_value        
                new_value = hostname

            # change value in registry
            changeRegistry(subkey, key_name, new_value)

        # increment loop thru hostnames
        i += 1
except WindowsError:
    pass

CloseKey(key)
