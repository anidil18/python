import matplotlib
#matplotlib.use('SVG')
matplotlib.use('AGG')
import matplotlib.pyplot as plt
import matplotlib
import csv
import sys
import os


data_dir = 'data/bdbzal/'

# typy testu, pro ktere zobrazim graf
tests = ['iops', 'mbps', 'lat' ]

# vytvor z nazvu file hlavicku
def getlabel(test):
  test = test.replace(data_dir, '')
  title = test.split('_')
  return (title[0]+'_'+title[1]+'_'+title[2]).upper()

# vytvor graf pro dany typ testu
def createGraph(typ):
    fig = plt.figure()
    ax = fig.add_axes([0.1, 0.1, 0.8, 0.8])

    # pro typ testu LAT dopln uplny nazev 'Latency'
    popisekTestu = typ
    if popisekTestu == 'lat': popisekTestu = 'latency'

    plt.title("Orion "+popisekTestu.upper())
    ax.set_xlabel('IO load[-]')
    ax.set_ylabel(popisekTestu.upper())

    # pro vsechny CSV soubory daneho typu - iops/mbps/lat proved pridani cary do grafu
    for root, dirs, files in os.walk(data_dir):
        for file in [ fi for fi in files if fi.endswith(typ.lower()+".csv") ]:
            csvfile = (os.path.join(root, file))

            # zpracuj CSV vstup
            csv_reader = csv.reader(open(csvfile))
            
            # skip hlavicky CSV vystupu
            firstLine = csv_reader.next()

            # data ulozena v col nebo row ?
            if float(firstLine[1]) == 0:
              x = []
              y = []
              for row in csv_reader:
                  x.append(row[0])
                  y.append(row[1])
            else:
               x = firstLine
               y = csv_reader.next()
               # promazni hlavicku dat, ktera se nebude zobrazovat
               del x[0]
               del y[0]

            # vykresli caru do grafu
            ax.plot(x,y, label=getlabel(csvfile))
    # popisku grafy            
    plt.legend(loc='upper left')

    # vyresli nebo uloz graf
    #plt.show()
    fig.savefig(typ.lower())

# main()
# pro vsechny typu testu vykresli graf
for test in tests:
  createGraph(test)
