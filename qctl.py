#!/usr/bin/python

import queue_system
import os
import sys

def usage():
  print("Usage: qctl.py shutdown  -  shutdown the queue system after the currently running job is finished")
  #print("        pause     -  do not automatically proceed to the next job in the queue")
  #print("        resume    -  resume processing the jobs in the queue")


if __name__ == '__main__':
  client = queue_system.queue_client()
  
  if len(sys.argv) != 2:
    usage()
    sys.exit(-1)
    
  if sys.argv[1] == "shutdown":
    response = client.shutdown_queue()
    if not response[0][0]:
      print("Could not shutdown queue system:",response[0][1])
    else:
      print("Shutting down.")
  else:
    usage()
    sys.exit(-1)
'''
  elif sys.argv[1] == "pause":
    pass
  elif sys.argv[1] == "resume":
    pass
'''
      
