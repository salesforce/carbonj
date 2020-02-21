#!/usr/bin/env python
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#


import sys
import os
import mmap
import struct
import signal
import optparse
import fnmatch
import StringIO
import httplib
import urllib
import requests

# modify these values as necessary
cfg = { "host": "127.0.0.1", "port": "56787", "rootdir" : "/home/dbabenko/work/carbon/migration/data", "createMetricIfMissing" : "true"}

p_r = { "60s24h": (60, 86400), "5m7d" : (300, 604800), "30m2y" : (1800, 63072000)}

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

option_parser = optparse.OptionParser(usage='''%prog <target_db_name> <source_dir_path>''')

option_parser.add_option(
    '--dryrun', default=False, action='store_true', dest='dryrun',
    help="Perform test run without actually trying to import data.")

(options, args) = option_parser.parse_args()

if len(args) < 2:
  option_parser.print_usage()
  sys.exit(1)

dbname = args[0]
path = args[1]

if not os.path.exists(path):
  sys.stderr.write("[ERROR] Path '%s' does not exist!\n\n" % path)
  option_parser.print_usage()
  sys.exit(1)

if not dbname in p_r:
  sys.stderr.write("[ERROR] Database name '%s' is not defined in this script!\n\n" % dbname)  
  sys.exit(1)

def mmap_file(filename):
  fd = os.open(filename, os.O_RDONLY)
  map = mmap.mmap(fd, os.fstat(fd).st_size, prot=mmap.PROT_READ)
  os.close(fd)
  return map

def read_header(map):
  try:
    (aggregationType,maxRetention,xFilesFactor,archiveCount) = struct.unpack(whisper.metadataFormat,map[:whisper.metadataSize])
  except:
    raise CorruptWhisperFile("Unable to unpack header")

  archives = []
  archiveOffset = whisper.metadataSize

  for i in xrange(archiveCount):
    try:
      (offset, secondsPerPoint, points) = struct.unpack(whisper.archiveInfoFormat, map[archiveOffset:archiveOffset+whisper.archiveInfoSize])
    except:
      raise CorruptWhisperFile("Unable to read archive %d metadata" % i)

    archiveInfo = {
      'offset' : offset,
      'secondsPerPoint' : secondsPerPoint,
      'points' : points,
      'retention' : secondsPerPoint * points,
      'size' : points * whisper.pointSize,
    }
    archives.append(archiveInfo)
    archiveOffset += whisper.archiveInfoSize

  header = {
    'aggregationMethod' : whisper.aggregationTypeToMethod.get(aggregationType, 'average'),
    'maxRetention' : maxRetention,
    'xFilesFactor' : xFilesFactor,
    'archives' : archives,
  }
  return header

def archive_data(archive_filename, precision, retention):
    content = None
    map = mmap_file(archive_filename)
    header = read_header(map)
    for i,archive in enumerate(header['archives']):
        if archive['secondsPerPoint'] == precision and archive['retention'] <= retention:
            content = load_archive_data(archive, map)
    map.close()
    return content

def load_archive_data(archive, map):
  output = StringIO.StringIO()
  offset = archive['offset']
  for point in xrange(archive['points']):
    (timestamp, value) = struct.unpack(whisper.pointFormat, map[offset:offset+whisper.pointSize])
    if timestamp != 0:
        val = str(value)
        val_without_trailing_zeros = val.rstrip('0').rstrip('.') if '.' in val else val
        print >> output, '%d, %s' % (timestamp, val_without_trailing_zeros)
    offset += whisper.pointSize
  content = output.getvalue()
  output.close()
  return content

def extract_meta(filename):
    # TODO: where to close?
    map = mmap_file(path)
    meta = []
    header = read_header(map)
    for i,archive in enumerate(header['archives']):
        m = (archive['secondsPerPoint'], archive['retention'])
        meta.append(m)
    return meta

def to_metric_name(root_dir, metric_file):
    dir_path = os.path.abspath(root_dir)
    m_path = os.path.abspath(metric_file)
    rel_path = os.path.relpath(m_path, dir_path)
    metric_name = rel_path.replace('/', '.')[:-4]
    return metric_name

def select():
    dir_path = "./data/pod2"
    matches = []
    meta = {}
    for root, dirs, files in os.walk(dir_path):
        for f in fnmatch.filter(files, "*.wsp"):
            filename = os.path.abspath(os.path.join(root, f))
            metric_name = to_metric_name(root_dir, filename)
            print metric_name
            matches.append(filename)

            for x in extract_meta(filename):
                if not x in meta:
                    meta[x] = 1
                else:
                    meta[x] += 1
            #meta.append(extract_meta(filename))
    print  meta

def process_files(dbname, search_path):
    file_count = 0
    root_dir = cfg['rootdir']
    for root, dirs, files in os.walk(search_path):
        for f in fnmatch.filter(files, "*.wsp"):
            filename = os.path.abspath(os.path.join(root, f))
            #print "Filename %s" % filename
            metric_name = to_metric_name(root_dir, filename)
            process_file(dbname, metric_name, filename)
            file_count = file_count + 1
    return file_count

def import_url(dbname, metric_name):
    return 'http://{0}:{1}/_dw/rest/carbonj/dbloader/{2}/{3}/'.format(cfg['host'], cfg['port'], dbname, metric_name)


def process_file(dbname, metric_name, f):
    (precision, retention) = p_r[dbname]
    data = archive_data(f, precision, retention)
    if data == None or len(data) == 0:
        print "[%s] No data points found with precision %s and renetion %s." % f, precision, retention
    else:
        request_url = import_url(dbname, metric_name)
        payload = { 'points' : data, 'createMetricIfMissing' : cfg['createMetricIfMissing'] }
        if options.dryrun:
            print "skipped sending request to server. url: [%s]" % request_url
        else:
            r = requests.post(request_url , data=payload)
            print r.text        

if options.dryrun:
  print "!!! THIS IS A DRY RUN !!! "

print """----------------------------------
carbonj: %s:%s
target database: %s
search path: %s
root dir: %s
create metric if missing: %s
----------------------------------""" % (cfg['host'], cfg['port'], dbname, path, cfg['rootdir'], cfg['createMetricIfMissing'])

files = process_files(dbname, path)
print "Found %s whisper files" % files


