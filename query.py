from uri import *


def retrieve_all_subattributes(client, uri):
  result = client.uri_query(uri)

  query_result = dict()
  if result[0][0] != True:
    raise RuntimeError(result[0][1])
  else:
    listing = uri_accessor.node_content_lister.extract_listing(result[1])
    for field in listing:
      if not field.endswith("/"):
        field_query = uri + "/" + field
        field_query_result = client.uri_query(field_query)
        
        if field_query_result[0][0] != True:
          raise RuntimeError("Error while accessing field "+str(field)+": "+str(field_query_result[0][1]))
        query_result[field] =  field_query_result[1]
    return query_result

def retrieve_all_subnodes(client, uri):
  result = client.uri_query(uri)

  if result[0][0] != True:
    raise RuntimeError(result[0][1])
  else:
    subnodes = []
    listing = uri_accessor.node_content_lister.extract_listing(result[1])
    for field in listing:
      if field.endswith("/"):
        subnodes.append(field.replace("/", ""))
    return subnodes