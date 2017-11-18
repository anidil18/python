# replace and remove -m prefix in hostname in putty registry

import re
from winreg  import OpenKey, SetValue, EnumKey, KEY_ALL_ACCESS,REG_SZ,HKEY_CURRENT_USER, QueryValue,QueryValueEx, SetValueEx,SetValue, CloseKey

# print debug values
debug = 1

# registry Putty sessions
key = OpenKey(HKEY_CURRENT_USER,"Software\\Simontatham\\Putty\\Sessions")

try:
    i = 0
    while True:
        subkey = EnumKey(key, i)
        subkey=OpenKey(key,subkey, 0, KEY_ALL_ACCESS)
        val = QueryValueEx(subkey, 'HostName')
        if debug: print (val[0])
        new_value = re.sub(r'(\w+)(-m)?(.*)',r'\1\3',val[0])
        if debug: print (new_value)
        i += 1
        SetValueEx(subkey,'HostName',0, REG_SZ, new_value)
except WindowsError:
    break

CloseKey(key)
