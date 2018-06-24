#!/usr/bin/python

import queue_system
import sys
import datetime
import query
from uri import *

max_col_chars = 70
max_full_status_col_chars=25

def format_table(table):
  col_width = [max(len(str(x)) for x in col) for col in zip(*table)]
  result = ""
  for line in table:
    result += "| " + " | ".join("{:{}}".format(str(x), col_width[i])
                                for i, x in enumerate(line)) + " |"
    result += "\n"
  return result

def usage():
  print("Usage: qstatus.py          - print status of all jobs")
  print("   or: qstatus.py <job id> - print detailed status of one job")
  print("   or: qstatus.py <URI>    - query the queue system with URIs")
  
    
def uri_query(client, uri):
  query_result = client.uri_query(uri)

  if query_result[0][0] != True:
    print("Error:", query_result[0][1])
  else:
    response = query_result[1]
    if uri_accessor.node_content_lister.is_listing(response):
      content = uri_accessor.node_content_lister.extract_listing(response)
      for x in content:
        print(x)
    else:
      print(response)

def detailed_job_query(client, job_id):
  # TODO Look in all queues
  query_uri = "qsystem://queues/default/jobs/by-id/" + str(job_id)
  print("Querying",query_uri,"...")
  result = query.retrieve_all_subattributes(client, query_uri)

  table = []
  for field in result:
    table.append([field, str(result[field])[:max_col_chars]])
  print(format_table(table))

def priority_sort_order(job_data):
  if job_data["running"] == True:
    return float("inf")
  return job_data["effective_priority"]

def get_displayed_field_name(f):
  if f == "running":
    return "R?"
  return queue_system.job_data.displayed_names[f] 
  
def full_status_report(client):
  # TODO Look in all queues
  query_uri = "qsystem://queues/default/jobs/by-id"
  queried_fields=[queue_system.job_data.ID,
                  queue_system.job_data.NAME,
                  queue_system.job_data.RUNNING,
                  queue_system.job_data.START_DATE,
                  queue_system.job_data.DIR]

  jobs = query.retrieve_all_subnodes(client, query_uri)
  

  if len(jobs) == 0:
    print("No jobs in queue")
  else:
    job_status = []
    for j in jobs:
      jdata = query.retrieve_all_subattributes(client, query_uri + "/" + j)
      job_status.append(jdata)
    
    job_status = sorted(job_status, key = lambda j : -priority_sort_order(j))

    header = [get_displayed_field_name(f) for f in queried_fields]

    table = [header]

    for j in job_status:
      row = []
      for f in queried_fields:
        row.append(str(j[f])[:max_full_status_col_chars])
      table.append(row)
    print(format_table(table))

def is_integer(s):
  try:
    x = int(s)
    return True
  except:
    return False

# *************************************************************************
if __name__ == '__main__':
  if len(sys.argv) > 2:
    usage()
    sys.exit(-1)

  client = queue_system.queue_client()

  try:
    if len(sys.argv) == 1:
      full_status_report(client)
    else:
      if sys.argv[1] == "--help":
        usage()
        sys.exit(0)
      elif is_integer(sys.argv[1]):
        detailed_job_query(client, int(sys.argv[1]))
        sys.exit(0)
      else:
        uri_query(client, sys.argv[1])
        sys.exit(0)
  except Exception as e:
    print("Error:",e)