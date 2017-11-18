# replace and remove -m prefix in hostname in tnsnames.ora
import re
import os

filename = os.environ["ORACLE_HOME"] + '/network/admin/tnsnames.ora'

print (filename)

# get lines from the file
lines = []
with open(filename, 'r') as f:
    for line in f:
        line = re.sub(r'HOST\s?=\s?(\w+)-m', r'HOST = \1', line)
        lines.append(line)

# write lines to the file
with open(filename, "w") as f:
    for item in lines:
        f.write(item)
