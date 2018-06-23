#!/usr/bin/python


import datetime
import os
import os.path
import time
import threading
import subprocess
import signal
import sys
from multiprocessing.connection import Listener
from multiprocessing.connection import Client

from uri import *


socket_name = "/tmp/queue_system_socket"
default_log_file = "/tmp/queue_system.log"

allow_cancel_running_jobs = True
default_max_queue_length = 500


class job_data:

  def format_command(command):
    result = ""
    for c in command:
      result += c
      result += " "
    return result

  ID = "id"
  CMD= "cmd"
  RUNNING   = "running"
  START_DATE = "start_date"
  SUBMISSION_DATE = "submission_date"
  DIR="dir"
  PROCESS="pid"
  ENV = "env"
  NAME = "name"
  STDOUT_FILE = "stdout_file"
  STDERR_FILE = "stderr_file"

  displayed_names = {
    ID: "job id",
    CMD: "command",
    RUNNING: "is running",
    START_DATE: "time of execution",
    SUBMISSION_DATE: "time of submission",
    DIR: "working directory",
    PROCESS: "PID",
    ENV: "shell environment",
    NAME: "job name",
    STDOUT_FILE: "stdout file",
    STDERR_FILE: "stderr file"
  }


  displayed_format = {
    CMD: format_command
  }

  default_values = {
    ID : -1, 
    CMD: "",
    RUNNING: False, 
    START_DATE: None,
    SUBMISSION_DATE: None,
    DIR: "", 
    PROCESS: None, 
    ENV: {},
    NAME: "<unnamed-%J>",
    STDOUT_FILE: "stdout_job.%J",
    STDERR_FILE: "stderr_job.%J"
  }
  
  # Fields that are allowed to be set by the client
  # at job submission
  submission_values = set([
    CMD,
    DIR,
    ENV,
    NAME,
    STDOUT_FILE,
    STDERR_FILE
  ])

  # Fields that are allowed to be changed by the client
  # after the submission
  client_modifyable_values = set([
    STDOUT_FILE,
    STDERR_FILE
  ])


  def get_displayed_field_value(self, field):
    if field in job_data.displayed_format:
      format_func = job_data.displayed_format[field]
      return format_func(self._data[field])

    return self._data[field]

  def get_displayed_field_name(self, field):
    if field in job_data.displayed_names:
      return job_data.displayed_names[field]
    
    return field

  def __init__(self, data = None):

    self._data = dict()
    if(data != None):
      
      for field in self.default_values:
        if not field in data:
          raise ValueError("Invalid job data: required field "+str(field)+" not found.")

      self._data = data
    else:
      for field in self.default_values:
        self._data[field] = self.default_values[field]

  # Sanitizes the job data received from a new submission from the client
  # to only include values that are allowed to be set by the client
  def sanitize_new_submission(self):
    for field in self._data:
      if not field in self.submission_values:
        self._data[field] = self.default_values[field]

  # Checks if the client should be allowed to modify a field
  def can_field_be_set_by_client(self, field_name):
    if field_name in self.client_modifyable_values:
      return True
    return False

  def client_set_field(self, name, value):
    if not name in self.client_modifyable_values:
      raise ValueError("Permission denied by queue server.")

    self.set_field(name, value)

  def set_field(self, name, value):
    if not name in self._data:
      raise ValueError("Job property was not properly initialized: "+name)
    if not name in self.default_values:
      raise ValueError("Invalid job property: "+name)

    self._data[name] = value

  def get_field(self, name):
    if not name in self._data:
      raise ValueError("Job property was not properly initialized: "+name)
    if not name in self.default_values:
      raise ValueError("Invalid job property: "+name)

    return self._data[name]

  def get(self):
    return self._data



class job(uri_node):
  def __init__(self, jdata):
    self._data = jdata

  # URI node code
  
  # No children available below the job level
  def uri_children(self, permissions):
    return dict()

  # Returns a list that contains the available URI attributes of this node
  def uri_node_attributes(self, permissions):
    return self._data.get().keys()

  def read_uri_attribute(self, name, permissions):
    if not name in self._data.get():
      raise uri_exception_no_such_attribute()

    if name == job_data.CMD:
      return job_data.format_command(self.get_cmd())
    if name == job_data.STDOUT_FILE:
      return self._format_filename(self.get_stdout_file())
    if name == job_data.STDERR_FILE:
      return self._format_filename(self.get_stderr_file())
    if name == job_data.NAME:
      return self._format_string(self.get_name())

    return self._data.get_field(name)

  def write_uri_attribute(self, name, value, permissions):
    
    if not name in self._data.get():
      raise uri_exception_no_such_attribute()

    if permissions == uri_permissions.CLIENT:
      if self._data.can_field_be_set_by_client(name):
        self._data.set_field(name, value)
      else:
        raise uri_exception_permission_denied()
    else:
      self._data.set_field(name, value)
  # End URI node code 
    
  def get_id(self):
    return self._data.get_field(job_data.ID)

  def get_name(self):
    return self._format_string(self._data.get_field(job_data.NAME))

  def get_cmd(self):
    return self._data.get_field(job_data.CMD)
  
  def is_running(self):
    return self._data.get_field(job_data.RUNNING)
  
  def get_start_date(self):
    return self._data.get_field(job_data.START_DATE)
  
  def get_working_directory(self):
    return self._data.get_field(job_data.DIR)
  

  def _format_string(self, s):
    formatted = s.replace("%J", str(self.get_id()))
    formatted = formatted.replace("%j", str(self.get_id()))
    formatted = formatted.replace("%N", self._data.get_field(job_data.NAME))
    formatted = formatted.replace("%n", self._data.get_field(job_data.NAME))
    return formatted

  def _format_filename(self, filename):
    result = self._format_string(filename)

    if "/" in result:
      raise ValueError("Filename invalid, contains '/': " + filename)
      
    return result

  def get_stdout_file(self):
    return self._format_filename(self._data.get_field(job_data.STDOUT_FILE))
  
  def get_stderr_file(self):
    return self._format_filename(self._data.get_field(job_data.STDERR_FILE))
  
  
  def run(self):
    if self.is_running():
      raise RuntimeError("Tried to mark already running job as running")
    
    self._data.set_field(job_data.RUNNING, True)
    self._data.set_field(job_data.START_DATE, datetime.datetime.now())
    
    os.chdir(self.get_working_directory())
    outfile=open(self.get_stdout_file(), "w")
    errfile = outfile
    if self.get_stdout_file() != self.get_stderr_file():
      errfile = open(self.get_stderr_file(), "w")
    
    try:
      process = subprocess.Popen(self._data.get_field(job_data.CMD), 
                                 cwd=self._data.get_field(job_data.DIR), 
                                 shell=False, 
                                 env=self._data.get_field(job_data.ENV), 
                                 stdout=outfile, stderr=errfile, preexec_fn=os.setsid)
      self._data.set_field(job_data.PROCESS, process.pid)
      process.wait()
      
      duration = datetime.datetime.now() - self._data.get_field(job_data.START_DATE)
      duration = datetime.timedelta(days=duration.days, seconds=duration.seconds)
      
      outfile.write("*******************************\n")
      outfile.write("Job terminated with exit code "+str(process.returncode)+" after running for "+str(duration)+"\n")
    except Exception as e:
      errfile.write("An exception occured during the execution of the job: "+str(e)+"\n")
    finally:
      self._data.set_field(job_data.PROCESS, None)
    
      outfile.close()
      errfile.close()
    
      self._data.set_field(job_data.RUNNING, False)

  def raw_data(self):
    return self._data

  
  def kill(self):
    process_id = self._data.get_field(job_data.PROCESS)
    if process_id != None:
      #self._process.kill()
      os.killpg(os.getpgid(process_id), signal.SIGTERM)
    

class queue_worker(uri_node):
  
  def __init__(self):
    self._jobs = []
    #self._current_job = None
    self._num_jobs = 0
    self._lock = threading.Lock()
    self._run = True
    
  def uri_children(self, permissions):
    
    jobs_by_id = dict()
    jobs_by_name = dict()
    for j in self._jobs:
      jobs_by_id[str(j.get_id())] = j
      jobs_by_name[j.get_name()] = j
    by_id = uri_directory(jobs_by_id)
    by_name = uri_directory(jobs_by_name)
    return {"jobs" : uri_directory({"by-name" : by_name, "by-id" : by_id})}

  # Returns a list that contains the available URI attributes of this node
  def uri_node_attributes(self, permissions):
    return ["num_jobs", "running"]

  def read_uri_attribute(self, name, permissions):
    try:
      self._lock.acquire()
      if name == "num_jobs":
        return self._num_jobs

      if name == "running":
        return self._run

      raise uri_exception_no_such_attribute()
    finally:
      self._lock.release()
    

  def write_uri_attribute(self, name, value, permissions):
    try:
      self._lock.acquire()

      if name == "num_jobs":
        raise uri_exception_read_only()

      if name == "running":
        if permissions == uri_permissions.SERVER:
          if type(value) is bool:
            self._run = value
          else:
            raise TypeError("Invalid data type for 'running' attribute. Must be bool, but is "+str(type(value)))
        
      raise uri_exception_no_such_attribute()
    finally:
      self._lock.release()

  def new_job(self, jdata):
    self._lock.acquire()
    
    try:
      # Make sure that the client has not set any fields that he shouldn't ;)
      jdata.sanitize_new_submission()

      job_id = self._num_jobs
      jdata.set_field(job_data.ID, job_id)
      jdata.set_field(job_data.SUBMISSION_DATE, datetime.datetime.now())
      j = job(jdata)
    
      self._jobs.append(j)
      self._num_jobs += 1
    except:
      raise
    finally:
      self._lock.release()

    return job_id

  def find_job_by_id(self, id):
    for job in self._jobs:
      if job.get_id() == id:
        return job
    return None

  def find_jobs_by_name(self, name):
    result = []
    for job in self._jobs:
      if job.get_name() == name:
        result.append(job)
    return result
  
  def cancel_job(self, job_id):
    self._lock.acquire()
    
    for i,j in enumerate(self._jobs):
      if j.get_id() == job_id:
        if j.is_running():
          if allow_cancel_running_jobs:
            j.kill()
            self._lock.release()
            return (True, "Job was running and has been killed.")
          else:
            self._lock.release()
            return (False, "Cancelling running jobs is not allowed. Please terminate the job with \"kill\".")
        else:
          self._jobs.pop(i)
          self._lock.release()
          return (True, "Job has been cancelled.")
      
    
    self._lock.release()
    
    return (False, "Specified job was not found.")
  
  def shutdown(self):
    self._lock.acquire()
    self._run = False
    self._lock.release()
    
  def get_num_jobs(self):
    self._lock.acquire()
    result = len(self._jobs)
    self._lock.release()
    return result
    
  def get_queued_jobs(self):
    self._lock.acquire()
    result = [j.raw_data().get() for j in self._jobs]
    self._lock.release()
    
    return result
  
  def is_running(self):
    return self._run
    
  def main_loop(self):
    print("Entering main batch processing loop...")
    while self._run:
      if len(self._jobs) > 0:
        try:
          self._jobs[0].run()
        except Exception as e:
          print("An exception occured during the execution of job",self._jobs[0].get_id(),":",e)
        finally:
          # Do not put that inside the try, because
          # the job must always be removed from the queue after it was attempted to execute it
          self._lock.acquire()
          j = self._jobs.pop(0)
          self._lock.release()
      
      # This not only prevents high cpu usage, it also prevents DoS attacks
      # against the queue system (recursive submission scripts!)
      time.sleep(0.4)
    print("Shutting down...")
        
      
class queue_server:
  
  def __init__(self, queue_worker, max_queue_length, uri_root):
    self._listener = Listener(socket_name, 'AF_UNIX')
    self._queue_worker = queue_worker
  
    self._max_queue_length = max_queue_length
    self._uri_root = uri_root
    
  def _handle_enqueue(self, msg, conn):

    # Refuse new jobs if the specified max. queue length is reached.
    # This can prevent "submission-bomb" DoS attacks.
    if(self._queue_worker.get_num_jobs() >= self._max_queue_length):
      conn.send([(False, "Request to enqueue has been denied. Queue has reached maximum allowed length.")])
      return
          
    # Refuse new jobs if the queue has shutdown
    if not self._queue_worker.is_running():
      conn.send([(False, "Queue has shutdown.")])
      return
    
    try:
      jdata = job_data(msg)

      job_id = self._queue_worker.new_job(jdata)
      conn.send([(True, "Job is enqueued."), job_id])
    except Exception as e:
      conn.send([(False, "Could not enqueue job: "+str(e))])
    
      
    

  def listen(self):
    print("********* Listening for instructions ***********")
    while True:
      try:
        conn = self._listener.accept()
        msg = conn.recv()
        # Only log the the first three fields since the fourth is the environment,
        # which would just clutter the log.
        print("Received message:",msg[:3])
        
        if(msg[0] == 'enqueue'):
          self._handle_enqueue(msg[1],conn)
            
        elif(msg[0] == 'cancel'):
          job_id = msg[1]
          result = self._queue_worker.cancel_job(job_id)
          if not result:
            conn.send([result])
          else:
            conn.send([result])
          
        elif(msg[0] == 'status'):
          queue_status = self._queue_worker.get_queued_jobs()
          conn.send([(True, "Queue status has been queried."), queue_status])
          
        elif(msg[0] == 'shutdown'):
          conn.send([(True, "Queue will shutdown as soon as all running jobs have completed.")])
          self._queue_worker.shutdown()

        elif(msg[0] == 'uri_query'):
          uri = msg[1]
          try:
            permissions = uri_permissions.CLIENT
            access = uri_accessor(uri, self._uri_root, permissions)
            result = access.read(permissions)
            conn.send([(True, "URI has been read."), result])
          except uri_exception_read_only:
            conn.send([(False, "URI is read-only.")])
          except uri_exception_permission_denied:
            conn.send([(False, "Access to URI has been denied.")])
          except uri_exception_not_found:
            conn.send([(False, "Object at specified URI does not exist.")])
          except uri_exception_no_such_attribute:
            conn.send([(False, "Attribute specified by URI does not exist.")])
          except Exception as e:
            conn.send([(False, str(e))])
            raise
          
        else:
          print("Warning: Message is unknown, cannot be handled.")
          conn.send([(False, "Invalid message")])
        
        conn.close()
      except Exception as e:
        print("Warning: Exception occured during handling of message:", e)
        #raise
      
    self._listener.close()
    

class queue_client:
  def __init__(self):
    pass
  
  def enqueue_job(self,command, submission_dir, environment):
    connection = Client(socket_name)
    
    jdata = job_data()
    jdata.set_field(job_data.CMD, command)
    jdata.set_field(job_data.DIR, submission_dir)
    jdata.set_field(job_data.ENV, environment)

    msg = ['enqueue', jdata.get()]
    connection.send(msg)
    
    return connection.recv()
    
  def cancel_job(self,job_id):
    connection = Client(socket_name)
    
    msg = ['cancel', job_id]
    connection.send(msg)
    
    return connection.recv()
    
  def get_queue_status(self):
    connection = Client(socket_name)
    
    msg = ['status']
    connection.send(msg)
    return connection.recv()

  def uri_query(self, uri):
    connection = Client(socket_name)

    msg = ["uri_query", uri]
    connection.send(msg)
    return connection.recv()
  
  def shutdown_queue(self):
    connection = Client(socket_name)
    
    msg = ['shutdown']
    connection.send(msg)
    return connection.recv()
      
class queue_system(uri_node):
  def __init__(self, log_file=default_log_file):
    self._log_filename = log_file
    self._set_log_file(log_file)

    self._queues = dict()
    self._queues["default"] = queue_worker()
    self._server = queue_server(self._queues["default"], default_max_queue_length, self)
    self._listener_thread = None

  def _set_log_file(self, filename):
    self._log_filename = filename
    self._log = open(self._log_filename, "w", 1)
    sys.stdout = self._log
    sys.stderr = self._log
     
  def uri_children(self, permissions):
    queues = uri_directory(self._queues)
    return {"queues" : queues}

  # Returns a list that contains the available URI attributes of this node
  def uri_node_attributes(self, permissions):
    return ["log_file"]

  def read_uri_attribute(self, name, permissions):
    if name == "log_file":
      return self._log_filename
    raise uri_exception_no_such_attribute()
    

  def write_uri_attribute(self, name, value, permissions):
    if name == "log_file":
      raise uri_exception_read_only()
    raise uri_exception_no_such_attribute()

  def run(self):
    self._listener_thread = threading.Thread(target=self._server.listen, daemon=True)
    self._listener_thread.start()

    # TODO This of course does not work for more than one queue
    for queue_name in self._queues:
      self._queues[queue_name].main_loop()

  def get_queues(self):
    return self._queues

if __name__ == '__main__':
  q = queue_system()
  q.run()
      
    
