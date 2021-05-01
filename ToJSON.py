import os
import io
import json
import sys
import calendar, datetime
import codecs

# command-line arguments
backupFolder = os.path.normpath(sys.argv[1])

#repoRoot = os.getcwd()
repoRoot = os.path.dirname(os.path.realpath(__file__))

# import google sdks
sys.path.append(os.path.join(repoRoot, 'SDKs/google_appengine'))
sys.path.append(os.path.join(repoRoot, 'SDKs/google-cloud-sdk/lib/third_party'))
from google.appengine.api.files import records
from google.appengine.datastore import entity_pb
from google.appengine.api import datastore

from google.appengine.api import datastore_types

def GetCollectionInJSONTreeForProtoEntity(jsonTree, entity_proto):
  result = jsonTree
  elements = entity_proto.key().path().element_list()
  for i, element in enumerate(elements):
    if element.has_type():
      nextKey = element.type()
      if nextKey not in result:
        result[nextKey] = {}
      result = result[nextKey]
    
    if element.has_name() and i + 1 < len(elements):
      nextKey = element.name()
      if nextKey not in result:
        result[nextKey] = {}
      result = result[nextKey]
  return result
'''
def GetCollectionOfProtoEntity(entity_proto):
  # reverse path-elements, so we always get last collection
  for element in entity_proto.key().path().element_list():
    if element.has_type(): return element.type()
'''
def GetKeyOfProtoEntity(entity_proto):
  # reverse path-elements, so we always get last key
  for element in reversed(entity_proto.key().path().element_list()):
    if element.has_name(): return element.name()
    #if element.has_id(): return element.id()
def GetValueOfProtoEntity(entity_proto):
  return datastore.Entity.FromPb(entity_proto)

def GetMapOfProtoEntity(embedded):
  v = entity_pb.EntityProto()
  v.ParsePartialFromString(embedded)
  return v

def ParseEntityData(data, depth=0):
  if isinstance(data, datastore.Entity):
    m = {}
    for k, v in data.items():
      m[k] = ParseEntityData(v, depth+1)
    return m
  elif isinstance(data, datastore_types.EmbeddedEntity):
    e = entity_pb.EntityProto()
    e.ParsePartialFromString(data)
    m = {}
    for p in e.property_list() + e.raw_property_list():
      if p.multiple():
        if p.name() not in m:
          m[p.name()] = []
        l = m[p.name()]
        l.append(ParseEntityData(datastore_types.FromPropertyPb(p), depth+1))
      else:
        m[p.name()] = ParseEntityData(datastore_types.FromPropertyPb(p), depth+1)
    return m
  elif isinstance(data, list):
    return map(lambda d: ParseEntityData(d, depth+1), data)
  else:
    return data

def Start():
  jsonTree = {}

  for filename in os.listdir(backupFolder):
    if not filename.startswith("output-"): continue
    print("Reading from:" + filename)
    
    inPath = os.path.join(backupFolder, filename)
    raw = open(inPath, 'rb')
    reader = records.RecordsReader(raw)
    for recordIndex, record in enumerate(reader):
      entity_proto = entity_pb.EntityProto(contents=record)

      collectionInJSONTree = GetCollectionInJSONTreeForProtoEntity(jsonTree, entity_proto)
      key = GetKeyOfProtoEntity(entity_proto)

      result = ParseEntityData(datastore.Entity.FromPb(entity_proto))
      collectionInJSONTree[key] = result

  outPath = os.path.join(backupFolder, 'Data.json')
  with open(outPath, 'w') as out:
      out.write(json.dumps(jsonTree, default=JsonSerializeFunc, ensure_ascii=False, indent=2).encode('utf-8'))
  print("JSON file written to: " + outPath)


def JsonSerializeFunc(obj):
  if isinstance(obj, datetime.datetime):
    if obj.utcoffset() is not None:
      obj = obj - obj.utcoffset()
    millis = int(
      calendar.timegm(obj.timetuple()) * 1000 +
      obj.microsecond / 1000
    )
    return millis
  #raise TypeError('Not sure how to serialize %s' % (obj,))
  return obj

Start()