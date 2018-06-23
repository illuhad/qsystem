#!/usr/bin/python

import queue_system
import os
import sys


if __name__ == '__main__':
  client = queue_system.queue_client()
  
  command = sys.argv[1:]
  env = dict(os.environ)
  
  if len(command) == 0:
    print("Usage: qsubmit.py <command>")
    sys.exit(-1)
  
  response = client.enqueue_job(command, os.getcwd(), env)
  if(response[0][0] != True):
    print("Could not enqueue job:",response[0][1])
  else:
    print("Enqueued as job "+str(response[1]))
      
