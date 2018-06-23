#!/usr/bin/python

import queue_system
import sys
import datetime

max_command_chars = 20
max_dir_chars = 40

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
  #print("   or: qstatus.py running  - print detailed status of running job")
  #print("   or: qstatus.py r        - print detailed status of running job")

def find_job(job_id, queue_status):
  for elem in queue_status:
    jdata = queue_system.job_data(elem)
    if jdata.get_field(queue_system.job_data.ID) == job_id:
      return jdata
  raise ValueError("Job "+str(job_id)+" not found.")
  
def print_detailed_information(queue_status, job_id):
  try:
    print("=======================================")
    print("Obtaining detailed information on job", queried_job_id)

    job_object = find_job(job_id, queue_status)
    job = job_object.get()

    table = []
    for field in sorted(job.keys()):
      # Do not display the shell environment
      if(field != queue_system.job_data.ENV):
        name = job_object.get_displayed_field_name(field)
        value = job_object.get_displayed_field_value(field)
        table.append([name, value])

    print(format_table(table))

  except Exception as e:
    print("Error:", e)
    
def uri_query(client, uri):
  query_result = client.uri_query(uri)

  if query_result[0][0] != True:
    print("Error:", query_result[0][1])
  else:
    print(query_result[1])
  

# *************************************************************************
if __name__ == '__main__':
  if len(sys.argv) > 2:
    usage()
    sys.exit(-1)


  client = queue_system.queue_client()
  
  uri_query(client, sys.argv[1])
  sys.exit(0)
    
  print_detailed_status_information = False
  queried_job_id = 0
  if len(sys.argv) == 2:
    if sys.argv[1] == "--help":
      usage()
      sys.exit(0)
    else:
      print_detailed_status_information = True
      try:
        queried_job_id = int(sys.argv[1])
      except:
        usage()
        sys.exit(-1)
    
  status = client.get_queue_status()
  
  if status[0][0] != True:
    print("Could not obtain queue status:",status[0][1])
    
  data = status[1]

  if len(data) == 0:
    print("No jobs are currently enqueued.")
  else:
    print("Overall queue status:")
    table = [["Job", "State", "Running since", "Command", "Working directory"]]
    for raw_job_data in data:
      j = queue_system.job(queue_system.job_data(raw_job_data))
      line = []
      
      state = "Pending"
      running_since = "---"
      if(j.is_running()):
        state = "Running"
        running_since = j.get_start_date().strftime("%y-%m-%d %H:%M")
      
      line.append(str(j.get_id()))
      line.append(state)
      line.append(running_since)
      
      command_str = ""
      for cmd_element in j.get_cmd():
        command_str+=cmd_element
        command_str+=" "
      line.append(command_str[:max_command_chars])
      line.append(j.get_working_directory()[:max_dir_chars])
      
      table.append(line)
    print(format_table(table))
    
    if print_detailed_status_information:
      print_detailed_information(data, queried_job_id)
      
