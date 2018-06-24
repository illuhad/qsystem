#!/usr/bin/python

import queue_system
import os
import sys
import uri
import os.path


def parse_submission_script(script_file):
  file = open(script_file, "r")

  initial_job_data = queue_system.job_data()
  dummy_job = queue_system.job(initial_job_data)

  for line in file:
    stripped_line = line.strip()
    if stripped_line.startswith("#job://"):
      stripped_line = stripped_line.replace("#job://", "", 1)
      parts = stripped_line.split(":")

      if len(parts) != 2:
        raise ValueError("Encountered invalid line in submission script: "+line)
      
      accessed_uri = parts[0].strip()
      value = parts[1].strip()

      if not accessed_uri in queue_system.job_data.submission_values:
        raise RuntimeError("Error: Queue server doesn't allow setting the field "+accessed_uri)

      initial_job_data.set_field(accessed_uri, value)
      print ("Submission script: Setting",accessed_uri,"=",value)
  return initial_job_data
      
def find_submission_script(command):

  for arg in command:
    if arg.endswith(".sh"):
      if os.path.isfile(arg):
        return arg
  return None

if __name__ == '__main__':
  client = queue_system.queue_client()
  
  command = sys.argv[1:]
  env = dict(os.environ)
  
  if len(command) == 0:
    print("Usage: qsubmit.py <command>")
    sys.exit(-1)
  
      
  jdata = queue_system.job_data()

  submission_script = find_submission_script(command)
  if submission_script != None:
    jdata = parse_submission_script(submission_script)

  jdata.set_field(queue_system.job_data.CMD, command)
  jdata.set_field(queue_system.job_data.DIR, os.getcwd())
  jdata.set_field(queue_system.job_data.ENV, env)

  response = client.enqueue_job(jdata)
  if(response[0][0] != True):
    print("Could not enqueue job:",response[0][1])
  else:
    print("Enqueued as job "+str(response[1]))
      
