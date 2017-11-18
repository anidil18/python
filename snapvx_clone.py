#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Usage:
  snapvx_clone.py <command> --source_db=<SRC> [--target_db=<DBNAME>] [options]
  snapvx_clone.py -h | --help | --version

Commands:
  show        Show symcli envinronment
  list        List all of available snaphosts
  create      Create snapshot
  link        Link snapshot to target storage group
  unlink      Unlink target storage groups

Options:
  -h --help                       Show this screen.
  --version                       Show the version.
  --source_db=<SRC>               Source <SRC> database
  --target_db=<DBNAME>            Clone/Destination <DBNAME> database
  --symid=<SymmID>                Set disk array SymmID
  --source_sg=<source-sg>         Set Source Storage Group
  --target_sg=<target-sg>         Set Target Storage Group
  --snapshot_name=<snapshot_name> Set snapshot name for link to storage group
  --ttl=<ttl>                     Set TTL in days [default: 100]
  --copy
  --metro
  --json                          Output format set to JSON

Example:
  snapvx_clone.py list --source_db=JIRKA --target_db=BOSON

"""

from docopt import docopt
import logging
import os
import subprocess
import re
import xml.etree.ElementTree as ET
import json.tool
from datetime import datetime
from operator import itemgetter

__version__ = '1.6'
__author__ = 'Jiri Srba'
__status__ = 'Development'


"""
Changes:
- konverze snapshot_timestamp na ISO format
- timeout na link do R1 SRDF/Metro
- relink - nahradit za unlink a relink, pokud prvni relink neprojde
- establish - vyhodit vystup ze symsnapvx establish do debug logu
- autodetekce SRDF/Metro
"""

"""
Requirements:
============

- GK disk na oem/boem serveru
- sudo na symcli
- python3 modules:

Important links:

- http://scummins.com/parsing-symclis-xml-output-with-python/
- https://pypi.python.org/pypi/PyStorage/
- https://fossies.org/linux/cinder/cinder/volume/drivers/emc/emc_vmax_common.py
"""

# ------ Initialize global variables ------
DEBUG = False        # pouze pro vypsani symcli prikazu namisto jejich zavolani
TRACE_DIR = '/var/log/dba'
VMAX3_MODELS = ['VMAX200K']
SYMCLI_PATH = 'sudo /usr/symcli/bin/'
DATA_ASM_DISKGROUP = r'_(D\d+|DATA)$'
EXCLUDED_REGEXP_SG = r'^[pb]porazal_.*|_GK'
SNAPSHOT_NAME_PREFIX = "SN_"


class SnapVXError(LookupError):
  """raise this when there is an SnapVX script error"""


def set_logging():
  """ Initialize debug logging """
  timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
  trace_file = os.path.join(TRACE_DIR, timestamp + "_" + 'snapvx_clone.log')
  logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',
                      datefmt="%Y-%m-%d %X",
                      filename=trace_file,
                      level=logging.DEBUG)
  console = logging.StreamHandler()
  console.setLevel(logging.INFO)
  formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s',
                                datefmt="%Y-%m-%d %X")
  console.setFormatter(formatter)
  logging.getLogger().addHandler(console)


def run_symcli_cmd(symcli_cmd, format='text', check=True, debug=False):
  """ Run symcli command

  :param symcli_cmd: symcli command list parameters with parameters to run
  :param format: defaultne textovy, jinak xml vystup
  :param check: kontrola na returncode > 0, vyhod exception

  :return: pro format=xml pouze vystup
           pro format=text [output, return code]
  """

  # prihod prefix na SYMCLI path vcetne volani sudo
  if symcli_cmd.startswith('sym'):
    symcli_cmd = os.path.join(SYMCLI_PATH, symcli_cmd.strip())

  # pro XML nastav vystup symcli na xml_e
  if format == 'xml':
    symcli_cmd += ' -output xml_e'

    # parse symcli command
  args = symcli_cmd.split()

  logging.debug("symcli command: {cmd}".format(cmd=' '.join(args)))

  # run symcli command
  if debug is False:
    try:
      # subprocess.run - funguje az od python 3.5
      # subprocess.check_output - starsi verze Pythonu
      sp = subprocess.check_output(args=args, stderr=subprocess.STDOUT,
                                   universal_newlines=True)
      returncode = 0
    except subprocess.CalledProcessError as e:
      # zachyt vyjimku a predej navratovy kod dale ke zpracovani
      output = e.output
      returncode = e.returncode
      logging.debug('returncode: {rc}'.format(rc=returncode))
      logging.debug("output: {output}".format(output=output))
      # pokud je nastavena kontrola na navratovy kod, rajsni eksepsnu
      if (check is True and returncode > 0):
        logging.error('{output}'.format(output=output))
        raise
    except Exception as e:
      # jakoukoliv jinou expcetion raisnu
      raise
  else:
    # pro debug=True vrat None a return code 0
    return [None, 0]

  # logging.debug("symcli output: %s", sp)

  # v pripade chyby vrat vystup ze STDERR
  sp = sp if returncode == 0 else output

  # pro XML vrat pouze vystup, jinak [vystup, returncode]
  return [ET.fromstring(sp), returncode] if format == 'xml' else [sp, returncode]


def get_symid(symid):
  """ Vrat vsechna dostupna SymID VMAX3 pole ze symcfg list

  - zadane SymmID zkontroluj, zda je platne
  - za neuvedene SymmID dopln vsechna existujici pole

  :return symid: list all of SymmID
  """

  symcli_cmd = 'symcfg list'
  [syminfo_tree, returncode] = run_symcli_cmd(symcli_cmd, format='xml')

  """
  naparsuj xml a vytvor
  list of symid, vybrane vmax pole, posledni 3 cislice
  """
  symid_list = [int(item.find('symid').text[-3:])
                for item in syminfo_tree.findall('Symmetrix/Symm_Info')
                if item.find('model').text in VMAX3_MODELS]

  logging.debug("SymmId arrays: {symid}".format(symid=symid_list))

  # pokud neni symid zadan, vrat veskera dostupna pole
  if symid is None:
    return symid_list
  # pokud je symid zadan, proved validaci a vrat jako list
  elif int(symid) in symid_list:
    return [symid]
  # None = Error
  else:
    raise ValueError('zadane symid {symid} neexistuje'.format(symid=symid))


def symsg_show(symid, sg):
  """ symsg show

  :param symid: symid VMAX3 pole
  :param sg:    nazev storage groupy

  :return: tuple ([dev_name], metro)
  """

  symcli_cmd = 'symsg -sid {symid} show {sg}'.format(symid=symid, sg=sg)
  [output_xml, returncode] = run_symcli_cmd(symcli_cmd,
                                            format='xml', check=True)

  dev_name = sorted([item.find('dev_name').text for item
                     in output_xml.findall('SG/DEVS_List/Device')])
  dev_type = [item.find('configuration').text for item
              in output_xml.findall('SG/DEVS_List/Device')]

  if any([s.startswith('RDF') for s in dev_type]):
    metro = True
  else:
    metro = False

  logging.debug("dev name: {dev}".format(dev=','.join(dev_name)))
  logging.debug("dev type: {dev}".format(dev=dev_type))
  logging.debug("metro: {metro}".format(metro=metro))

  return (dev_name, metro) if dev_name else None


def symsg_list(symid, dbname):
  """ Funkce hleda nazev storage groupy dle zadane nazvu dbname

  :return: namedtuple(symid, sg_name, num_devs, metro):
  """

  # DBNAME
  logging.debug("dbname: {dbname}".format(dbname=dbname))
  sg_to_match = re.escape(dbname) + DATA_ASM_DISKGROUP
  logging.debug("storage group to match: {sg}".format(sg=sg_to_match))

  """
  sg = namedtuple(symid, sg_name, num_devs, metro)
  pozor, pokud je vice storage group na vice polich, vrati pouze prvni sg
  """
  sg = tuple()

  # pokud není symid typu list, tak ho zkonvertuj na list
  if not isinstance(symid, list):
    symid = [symid]
  for sid in symid:
    symcli_cmd = "symsg -sid {sid} list".format(sid=sid)
    [sginfo_tree, returncode] = run_symcli_cmd(symcli_cmd, format='xml')

    # parse XML output ze symsg list
    for item in sginfo_tree.findall('SG/SG_Info'):
      sg_name = item.find('name').text

      if (re.search(sg_to_match, sg_name, flags=re.IGNORECASE) and
              # vynech excludovane sg pro offload backup servery
              not(re.search(EXCLUDED_REGEXP_SG, sg_name,
                            flags=re.IGNORECASE))):
        if sg_name not in sg:
          # detekce typu disku RDF[12]
          dev_name, metro = symsg_show(sid, sg_name)
          sg = (sid, sg_name, dev_name, item.find('num_devs').text, metro)
        else:
          # pokud jiz sg existuje na jinem poli, vyhod Warning
          logging.warning("Multiple SymID for storage group {sg} found"
                          .format(sg=sg_name))

  logging.debug("symsg list: {sg}".format(sg=sg))
  if not sg:
    raise ValueError('storage groupa pro db {db} nenalezena'.format(db=dbname))

  return sg if sg else None


def validate_sg(symcli_env):
  """ Kontrola source_sg a target_sg na
    - shodne SymmID source and target sg
    - shodny pocet disku pro source and target sg
  """

  if symcli_env['source_devs'] != symcli_env['target_devs']:
    raise SnapVXError("""source {source} and target {target}
                     number of disks is different
                     """
                     .format(source=symcli_env['source_sg'],
                             target=symcli_env['target_sg']))
  # pokud vsechno sedi, return True
  return True


def get_symcli_env(source_db, target_db, symid, snapshot_name):
  """ Get symcli env from source and target db """

  # preved symid na list a zkontroluj, zda je symid platne
  symid = get_symid(symid)
  logging.debug("symid: {symid}".format(symid=symid))

  # zacni nejprve s target db a dle toho nastav symid
  # pokud neni target db definovana, nema cenu pro ni dohledavat
  if target_db is None:
    logging.debug("target_db not defined")
    target_sg = None
    target_devs = None
    target_dev_name = None
    target_is_metro = None
  else:
    # symid se prehodi na nalezenou storage groupu, pro parovani se source sg
    symid, target_sg, target_dev_name, target_devs, target_is_metro = symsg_list(symid, target_db)
    logging.debug("target: {},{},{},{}".format(symid, target_sg,
                                               target_dev_name, target_devs,
                                               target_is_metro))

  # pokracuj s definici source storage groupy
  symid, source_sg, source_dev_name, source_devs, source_is_metro = symsg_list(symid, source_db)

  # napl dict symcli_env ziskanymi hodnotami
  # SymmID: odvozuje se z cilove nebo zdrojove (pokud neni target db uvedena)
  symcli_env = {'symid': symid,
                'source_db': source_db,
                'source_sg': source_sg,
                'source_dev_name': source_dev_name,
                'source_devs': source_devs,
                'source_is_metro': source_is_metro,
                'target_db': target_db,
                'target_sg': target_sg,
                'target_dev_name': target_dev_name,
                'target_devs': target_devs,
                'target_is_metro': target_is_metro,
                'snapshot_name': snapshot_name}

  # proved kontrolu zjistenych symcli nastaveni, pokud je target_db uvedena
  if target_db is not None:
    logging.debug('validation')
    validate_sg(symcli_env)

  # pro TARGET sg zjisti rdf_group a proved validaci
  if target_is_metro:
     rdf_group = validate_metro_rdf(symcli_env)
     if rdf_group is False:
       raise SnapVXError("nepodarilo se ziskat RDF groupu")
     else:
       symcli_env['rdf_group'] = rdf_group

  logging.debug("symcli_env: {env}".format(env=symcli_env))

  return symcli_env


def get_snapshot(symcli_env):
  """ List a SnapVX snaphosts from source sg
  symsnapvx -sid ${SymmID} -sg $SOURCE_SG list -output xml_e
  """

  snapshot = list()

  symcli_cmd = 'symsnapvx -sid {sid} -sg {sg} list' \
               .format(sid=symcli_env['symid'], sg=symcli_env['source_sg'])

  [snapshot_xmltree, _returncode] = run_symcli_cmd(symcli_cmd, format='xml', check=False)

  for item in snapshot_xmltree.findall('SG/Snapvx/Snapshot'):
    snapshot_name      = item.find('snapshot_name').text
    snapshot_timestamp = item.find('last_timestamp').text
    snapshot_link      = item.find('link').text

    s = dict()
    if snapshot_name.startswith(SNAPSHOT_NAME_PREFIX):
      # vyrazeni duplict snapshot name pres jednotlive disky
      if snapshot_name not in [s['snapshot_name'] for s in snapshot]:
        # prirad pouze prvni disk
        logging.debug("name: {snapshot_name}"
                      .format(snapshot_name=snapshot_name))
        # konverze snapshot_timestamp na datetime
        snapshot_timestamp = datetime.strptime(
            snapshot_timestamp, '%a %b %d %H:%M:%S %Y')
        s['snapshot_name']      = snapshot_name
        s['snapshot_timestamp'] = snapshot_timestamp.isoformat()
        s['snapshot_link']      = snapshot_link
        snapshot.append(s)

  # sort
  snapshot = sorted(snapshot, key=itemgetter('snapshot_name'), reverse=True)

  if snapshot:
    logging.debug("snapshot list: {output}".format(output=snapshot))
  else:
    logging.warning('No snapshots was found.')

  return snapshot


def symrdf_list(symid):
  """ Vypis vsech dostupnych SRDF/Metro disku
  symrdf -sid 756 -rdf_metro list

  :return dict {rdf_dev: rdf_group}:
  """

  symcli_cmd = 'symrdf -sid {sid} -rdf_metro list'.format(sid=symid)

  [output_xml, returncode] = run_symcli_cmd(symcli_cmd, format='xml',
                                            check=True)

  rdf_dev = dict()
  for item in output_xml.findall('Symmetrix/Device/RDF/Local'):
    dev_name = item.find('dev_name').text
    rdf_group = item.find('ra_group_num').text
    rdf_dev[dev_name] = rdf_group

  return rdf_dev if rdf_dev else None


def validate_metro_rdf(symcli_env):
  """ check list of disk in storage group againt rdf group

  :return rdf_group: pokud je vse ok, pak vrat cislo RDF groupy
  """

  # target devs z target storage groupy
  sg_dev_name = symcli_env['target_dev_name']
  logging.debug('sg_dev_name: {dev}'.format(dev=sg_dev_name))

  # target RDF grupa vcetne disku ve formatu dict
  rdf_dev = symrdf_list(symcli_env['symid'])

  # zjisteni RDF group
  rdf_group = list({v for k, v in rdf_dev.items() if k in sg_dev_name})

  logging.debug('rdf group: {group}'.format(group=rdf_group))

  # pokud je vice RDF group, pak vyhod exception
  if len(rdf_group) != 1:
    logging.error('nelze klonovat, neplatna RDF groupa {rdf}'
                  .format(rdf=rdf_group))
    return False

  # vsechny disky z dané metro RDF groups
  sg_rdf_dev = sorted([k for k, v in rdf_dev.items() if v in rdf_group])

  logging.debug('target sg_dev_name: {dev}'.format(dev=sg_dev_name))
  logging.debug('target sg_rdf_dev: {dev}'.format(dev=sg_rdf_dev))

  # disky ze storage group jsou obsazene v rdf group
  if (set(sg_dev_name).intersection(set(sg_rdf_dev))):
    # covert string to int
    return int(''.join(rdf_group))
  else:
    logging.error('nelze klonovat, nesedi RDF groupa se SG')
    return False


def establish_snapshot(symcli_env):
  """ Creates a SnapVX snapshot
  symsnapvx -sid 67 -nop -sg snapsource establish \
    -name hourlysnap -ttl -delta 2
  """

  # pokud neni snapshot uveden, vytvor dle konvence novy
  if symcli_env['snapshot_name'] is None:
    timestamp = datetime.now().strftime("%Y%m%d")
    symcli_env['snapshot_name'] = SNAPSHOT_NAME_PREFIX + \
        symcli_env['source_db'] + '_' + timestamp

  logging.info('creating snapshot {sn} ...'
               .format(sn=symcli_env['snapshot_name']))
  symcli_cmd = '''symsnapvx -sid {sid} -sg {sg}
    -name {snapshot_name} -noprompt
    establish {opts}
    '''.format(sid=symcli_env['symid'],
               sg=symcli_env['source_sg'],
               snapshot_name=symcli_env['snapshot_name'],
               opts=' '.join(symcli_env['snapshot_opts']))

  [output, returncode] = run_symcli_cmd(symcli_cmd, format="text", check=True)

  logging.info("{output}".format(output=output))
  logging.info('snapshot name: {sn} created'
               .format(sn=symcli_env['snapshot_name']))


def unlink_snapshot(symid, target_sg):
  """ unlink target storage groupy dle nazvu linknuteho snapshotu a lndevs
  Poznamka: namisto

  :param symid: symid VMAX3 pole
  :param target_sg:    nazev linknute storage groupy
  """

  # nejprve ověřím, zda je vubec potreba target sg linknuty
  symcli_cmd = '''symsnapvx -sid {sid} list -lnsg {target_sg} -linked -by_tgt
    '''.format(sid=symid, target_sg=target_sg)
  [output_xml, returncode] = run_symcli_cmd(symcli_cmd, format='xml', check=False)

  logging.debug('output: {output}'.format(output=output_xml))
  logging.debug('returncode: {returncode}'.format(returncode=returncode))

  # proved unlink, pokud je potreba
  if returncode == 0:
    lndevs = [item.find('link').text for item
              in output_xml.findall('SG/Snapvx/Snapshot')]
    lndevs = ','.join(lndevs)

    sourcedevs = [item.find('source').text for item
                  in output_xml.findall('SG/Snapvx/Snapshot')]
    sourcedevs = ','.join(sourcedevs)

    # generace, jedna unikátní hodnota
    gen = [item.find('generation').text for item
           in output_xml.findall('SG/Snapvx/Snapshot')]
    gen = ''.join(set(gen))

    # snapshot name, jedna unikátní hodnota Snapshotu
    sn = [item.find('snapshot_name').text for item
          in output_xml.findall('SG/Snapvx/Snapshot')]
    sn = ''.join(set(sn))

    logging.debug("lndevs: {dev}".format(dev=lndevs))
    logging.debug("sourcedevs: {dev}".format(dev=sourcedevs))
    logging.debug("snapshot: {sn}".format(sn=sn))

    logging.info('unlinking {sg} linked with snapshot {sn}'
                 .format(sg=target_sg, sn=sn))

    symcli_cmd = '''symsnapvx -sid {symid} -noprompt -devs {sourcedevs}
      -lndevs {lndevs} -snapshot_name {sn} -generation {gen} unlink
      '''.format(symid=symid, sourcedevs=sourcedevs, lndevs=lndevs,
                 gen=gen, sn=sn)
    [output_xml, returncode] = run_symcli_cmd(symcli_cmd, format='text')
    logging.info('unlink {target_sg} finished'.format(target_sg=target_sg))
  else:
    logging.info('storage group {sg} is NOT linked'.format(sg=target_sg))



def link_snapshot(symcli_env):
  """ Link a SnapVX snapshot to a target database
  """

  snapshot_name = symcli_env['snapshot_name']
  metro         = symcli_env['target_is_metro']
  link_opts     = symcli_env['link_opts']

  # dostupne snapshoty, dict:'snapshot_name'
  available_snapshot = [s['snapshot_name'] for s in get_snapshot(symcli_env)]
  logging.debug("available_snapshot {snap}".format(snap=available_snapshot))

  # pokud neni snapshot zadan, nacti posledni/nejnovejsi z dostupnych
  if snapshot_name is None:
    snapshot_name = available_snapshot[0]

  """ overeni, ze je dany snapshot k dispozici
  overeni neprovadim, proste se pokusim zadany snapshot linknout
  if snapshot_name not in available_snapshot:
   raise SnapVXError('pozadovany snapshot {snapshot} neni k dispozici'
                     .format(snapshot=snapshot_name))
  """

  # Metro: suspend RDF group
  if metro:
    logging.info('Suspending RDF link ...')
    symcli_cmd = '''symrdf -sid {sid} -noprompt
      -rdfg {rdf} -sg {target_sg} suspend -force
      '''.format(sid=symcli_env['symid'], rdf=symcli_env['rdf_group'],
                 target_sg=symcli_env['target_sg'])
    [output, returncode] = run_symcli_cmd(symcli_cmd, format='text',
                                          check=True, debug=DEBUG)
    logging.info("{output}".format(output=output))

  # unlink snapshotu na target sg, pokud je potřeba
  unlink_snapshot(symcli_env['symid'], symcli_env['target_sg'])

  # link target storage group
  logging.info('Linking snapshot {sn} to sg {sg} ...'
               .format(sn=snapshot_name, sg=symcli_env['target_sg']))
  symcli_cmd = '''symsnapvx -sid {sid} -sg {source_sg} -lnsg {target_sg}
      -snapshot_name {snapshot_name} -nop {action} {opts}
      '''.format(sid=symcli_env['symid'],
                 source_sg=symcli_env['source_sg'],
                 target_sg=symcli_env['target_sg'],
                 snapshot_name=snapshot_name,
                 action='link',
                 opts=' '.join(link_opts))

  [output, returncode] = run_symcli_cmd(symcli_cmd, format='text',
                                        check=True, debug=DEBUG)
  logging.info("{output}".format(output=output))

  """
  kontrola, ze je link ve stavu DEFINED
  - -nocopy - 6x po 10-ti
  - -copy - aspon 2 hodinky
  """
  logging.debug('link opts: {opts}'.format(opts=link_opts))
  if '-copy' in link_opts:
    # cekej bez omezeni ... a zkoušej to po 10 min
    wait_opts = '-i 600'
    # verify linked a copied stav, jinak linked a defined stav
    verify_opts = '-copied -defined'
  else:
    wait_opts = '-i 10 -c 6'
    verify_opts = '-linked'

  if '-copy' in link_opts:
    # pokud se snapshot kopiruje, pak vypis prikazy pro aktualni stav
    logging.info('waiting for disks to be in COPIED/DEFINED state ' +
                 'for {wait_opts} ...'.format(wait_opts=wait_opts))

    # QUERY status:
    symcli_cmd = '''sudo symsnapvx -sid {sid} -lnsg {target_sg}
      -snapshot_name {snapshot_name} list -by_tgt -linked -detail -gb
      '''.format(sid=symcli_env['symid'],
                 target_sg=symcli_env['target_sg'],
                 snapshot_name=snapshot_name)
    logging.info('prubeh kopirovani snapshotu lze sledovat prikazem:')
    logging.info('{query}'.format(query=' '.join(symcli_cmd.split())))

  # symsnapvx verify
  symcli_cmd = '''symsnapvx -sid {sid} -lnsg {target_sg} {wait_opts}
     -snapshot_name {snapshot_name} verify {verify_opts} -by_tgt
    '''.format(sid=symcli_env['symid'], target_sg=symcli_env['target_sg'],
               verify_opts=verify_opts,
               wait_opts=wait_opts,
               snapshot_name=snapshot_name)

  [_output, returncode] = run_symcli_cmd(symcli_cmd, format='text',
                                         check=False, debug=DEBUG)

  if returncode > 0:
    raise SnapVXError('''disky se nepodarilo dostat do stavu LINKED/COPIED
      ve stanovem casovem limitu''')

  # finální vypis stavu disků
  symcli_cmd = '''symsnapvx -sid {sid} list -lnsg {sg} -linked -by_tgt
    -detail -gb'''.format(sid=symcli_env['symid'], sg=symcli_env['target_sg'])
  [output, returncode] = run_symcli_cmd(symcli_cmd, format='text',
                                        check=True, debug=DEBUG)
  logging.debug("{output}".format(output=output))

  if metro:
    logging.debug('symsnapvx unlink sg:')
    symcli_cmd = '''symsnapvx -sid {sid} -sg {source_sg} -lnsg {target_sg}
        -snapshot_name {snapshot_name} -noprompt unlink
        '''.format(sid=symcli_env['symid'],
                   source_sg=symcli_env['source_sg'],
                   target_sg=symcli_env['target_sg'],
                   snapshot_name=snapshot_name)
    [output, _returncode] = run_symcli_cmd(symcli_cmd, format='text',
                                           check=True, debug=DEBUG)
    logging.debug("{output}".format(output=output))

    # establish RDF
    symcli_cmd = '''symrdf -sid {sid} -rdfg {rdf} -sg {target_sg} establish
      -use_bias -nop
      '''.format(sid=symcli_env['symid'],
                 rdf=symcli_env['rdf_group'],
                 target_sg=symcli_env['target_sg'])

    [output, returncode] = run_symcli_cmd(symcli_cmd, format='text',
                                          check=True, debug=DEBUG)
    logging.debug("{output}".format(output=output))

    # vypsani query na status RDF groupy, bez dalsiho zpracovani
    symcli_cmd = '''sudo symrdf -sid {sid} -rdfg {rdf} -sg {target_sg}
        query -i 5
        '''.format(sid=symcli_env['symid'],
                   rdf=symcli_env['rdf_group'],
                   target_sg=symcli_env['target_sg'])
    logging.info('waiting for establish RDF link ...')
    logging.info('prubeh sync R1 > R2 lze sledovat prikazem:')
    logging.info('{query}'.format(query=' '.join(symcli_cmd.split())))

    # verify Active Bias
    symcli_cmd = '''symrdf -sid {sid} -rdfg {rdf} -sg {target_sg}
      verify {wait_opts} -activebias -nop
      '''.format(sid=symcli_env['symid'],
                 wait_opts=wait_opts,
                 rdf=symcli_env['rdf_group'],
                 target_sg=symcli_env['target_sg'])

    [output, returncode] = run_symcli_cmd(symcli_cmd, format='text',
                                          check=True, debug=DEBUG)
    logging.info("{output}".format(output=output))
    logging.info('ActiveBias in sync')

    # proved terminate zdrojoveho snapshotu
    # po unlinku full copy uz je stejne k nicemu
    # nahradit za unlink_snapshot()
    symcli_cmd = '''symsnapvx -sid {sid} -sg {source_sg}
        -snapshot_name {snapshot_name} -noprompt terminate
        '''.format(sid=symcli_env['symid'],
                   source_sg=symcli_env['source_sg'],
                   snapshot_name=snapshot_name)
    [output, returncode] = run_symcli_cmd(symcli_cmd, format='text',
                                          check=True, debug=DEBUG)
    logging.debug("{output}".format(output=output))

  logging.info('link finished')


def show_env(symcli_env, output_format):
  """ Get symcli ENV and print the values in requested format """

  # logging.info('show envinronment for SnapVX cloning:')

  if output_format == 'text':
    for key, value in symcli_env.items():
      print('{key}="{value}"'.format(key=key, value=value))
  elif output_format == 'json':
    print(json.dumps(symcli_env, sort_keys=True, indent=4))


def list_snapshot(symcli_env, output_format='text'):
  """ Vypise vsechny dostupne snapshoty pro danou storage groupu """

  snapshot = get_snapshot(symcli_env)

  if output_format == 'text':
    if snapshot:
      logging.info("list of all available snapshots:")
      for dic in snapshot:
        print("snapshot_name={name}  # Timestamp: {timestamp} Linked: {linked}"
              .format(name=dic['snapshot_name'],
                      timestamp=dic['snapshot_timestamp'],
                      linked=dic['snapshot_link']))
    else:
      logging.info('No snapshots was found.')
  elif output_format == 'json':
    if snapshot:
      print(json.dumps(snapshot, sort_keys=False, indent=4))
    else:
      err_msg = {'errors': [{'code': 404, 'message': 'No snapshots was found.'}]}
      print(json.dumps(err_msg, indent=4))


def main(arguments):
  """ Main function
  """

  # set logging
  set_logging()
  logging.debug("snapvx_clone.py args: %s", arguments)

  # nacti symcli env
  symcli_env = get_symcli_env(arguments['--source_db'],
                              arguments['--target_db'],
                              arguments['--symid'],
                              arguments['--snapshot_name'])

  # prepis nazvy storage group z args, pokud jsou nastaveny
  if arguments['--source_sg'] is not None:
    symcli_env['source_sg'] = arguments['--source_sg']
  if arguments['--target_sg'] is not None:
    symcli_env['target_sg'] = arguments['--target_sg']

  if symcli_env['source_sg'] is None:
    raise ValueError('nepodarilo se nastavit prostredi pro snapvx')

  # prepis hodnoty metro dle args
  if arguments['--metro']:
    symcli_env['source_is_metro'] = arguments['--metro']
    symcli_env['target_is_metro'] = arguments['--metro']

  # nastav typ vystupu na TEXT nebo JSON, zatim ma smysl pouze pro vypis snapshotu
  output_format = 'json' if arguments['--json'] else 'text'
  logging.debug("output_format: {format}".format(format=output_format))

  # zavolej akci dle commandu
  action = arguments['<command>']
  logging.debug("action: {action}".format(action=action))

  if action == 'show':
    show_env(symcli_env, output_format)

  elif action == 'list':
    list_snapshot(symcli_env, output_format)

  elif action == 'create':
    # default create snapshot options
    symcli_env['snapshot_opts'] = ['-ttl', '-delta', arguments['--ttl']]

    if symcli_env['source_is_metro']:
      symcli_env['snapshot_opts'] += ['-both_sides']

    establish_snapshot(symcli_env)
    # na konci vypis vsechny vytvorene snapshoty
    list_snapshot(symcli_env, output_format)

  elif action == 'link':
    # default link options
    symcli_env['link_opts'] = ['-copy'] if arguments['--copy'] else []

    if symcli_env['target_is_metro']:
      symcli_env['link_opts'] = ['-copy', '-remote']

    link_snapshot(symcli_env)

  elif action == 'unlink':
    unlink_snapshot(symcli_env['symid'], symcli_env['target_sg'])


if __name__ == "__main__":
  arguments = docopt(__doc__, version=__version__)
  main(arguments)
