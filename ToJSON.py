import os
import json
import sys


repoRoot = os.path.dirname(os.path.realpath(__file__))

# import google sdks
sys.path.append(os.path.join(repoRoot, 'SDKs/google_appengine'))
sys.path.append(os.path.join(repoRoot, 'SDKs/google-cloud-sdk/lib/third_party'))
from google.appengine.api.files import records
from google.appengine.datastore import entity_pb
from google.appengine.api import datastore

from google.appengine.api import datastore_types


def resolve_entity_path(entity_proto):
  path = []

  elements = entity_proto.key().path().element_list()
  for element in elements:
    if element.has_type():
      path.append(element.type())
    if element.has_name():
      path.append(element.name())
  
  return path


def parse_entity_data(data, depth=0):
  if isinstance(data, datastore.Entity):
    m = {}
    for k, v in data.items():
      m[k] = parse_entity_data(v, depth+1)
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
        l.append(parse_entity_data(datastore_types.FromPropertyPb(p), depth+1))
      else:
        m[p.name()] = parse_entity_data(datastore_types.FromPropertyPb(p), depth+1)
    return m
  elif isinstance(data, list):
    return map(lambda d: parse_entity_data(d, depth+1), data)
  else:
    return data


def process(output_folder):
  result = {}
  count = 0

  for filename in os.listdir(output_folder):
    if not filename.startswith("output-"): continue
    print("Reading from: " + filename)
    
    with open(os.path.join(output_folder, filename), "rb") as f:
      reader = records.RecordsReader(f)
      for record in reader:
        entity_proto = entity_pb.EntityProto(contents=record)

        path = resolve_entity_path(entity_proto)
        data = parse_entity_data(datastore.Entity.FromPb(entity_proto))

        # === Process ===
        
        if len(path) == 4 and path[0] == "projects" and path[2] == "site-meta":
          routes = [route["id"] for route in data["routes"].values() if route["status"] == "PUBLISHED"]
          result[path[1]] = routes

          count = count + len(routes)

  print("count = {}".format(count))

  result_path = os.path.join(output_folder, 'result.json')
  with open(result_path, 'w') as out:
      out.write(json.dumps(result, ensure_ascii=False, indent=2).encode('utf-8'))
  print("Result written to: " + result_path)


def main():
  # command-line arguments
  output_folder = os.path.normpath(sys.argv[1])

  process(output_folder)


if __name__ == "__main__":
  main()
