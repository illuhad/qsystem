#!/usr/bin/python

import queue_system
import os
import sys

def usage():
  print("Usage: qcancel.py <Job ID>")

if __name__ == '__main__':
  client = queue_system.queue_client()
  
  if len(sys.argv) != 2:
    usage()
    sys.exit(-1)
    
  if sys.argv[1] == "--help":
    usage()
    sys.exit(0)
  
  job_id = int(sys.argv[1])
  
  response = client.cancel_job(job_id)
  if(response[0][0] != True):
    print("Could not cancel job:",response[0][1])
  else:
    print("The job has been cancelled.")
      
