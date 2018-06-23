

# Implements parsing of URI-like attributes of the queue system, i.e. in order
# to access the pid of job #98:
# queues://default/jobs/98/pid


class uri_permissions:
  CLIENT = 0,
  SERVER=1
  
class uri_exception(Exception):
  pass

class uri_exception_read_only(uri_exception):
  pass

class uri_exception_permission_denied(uri_exception):
  pass

class uri_exception_no_such_attribute(uri_exception):
  pass

class uri_exception_not_found(uri_exception):
  pass

class uri_node:
  def __init__(self):
    pass

  # Returns a dict that maps child name -> URI node object
  def uri_children(self, permissions):
    raise NotImplementedError("URI list children not implemented")

  # Returns a list that contains the available URI attributes of this node
  def uri_node_attributes(self, permissions):
    raise NotImplementedError("URI attribute listing not implemented")

  def read_uri_attribute(self, name, permissions):
    raise NotImplementedError("URI read not implemented")

  def write_uri_attribute(self, name, value, permissions):
    raise NotImplementedError("URI write not implemented")

class uri_directory(uri_node):
  def __init__(self, children):
    self._children = children

  def uri_children(self, permissions):
    return self._children

  # Returns a list that contains the available URI attributes of this node
  def uri_node_attributes(self, permissions):
    return []

  def read_uri_attribute(self, name, permissions):
    raise uri_exception_no_such_attribute

  def write_uri_attribute(self, name, value, permissions):
    raise uri_exception_no_such_attribute

class uri_accessor:
  queue_uri  = "queues://"
  job_uri    = "jobs://"
  system_uri = "qsystem://"

  class attribute_setter:
    def __init__(self, uri_node, attribute_name):
      self._node = uri_node
      self._name = attribute_name
    
    def set_attribute(self, value, permissions):
      self._node.write_uri_attribute(self._name, value, permissions)


  class node_content_lister:
    def __init__(self, uri_node):
      self._node = uri_node

    def content(self, permissions):
      result = ("uri-query-listing", [])
      for child in self._node.uri_children(permissions):
        result[1].append(child + "/")
      for attribute in self._node.uri_node_attributes(permissions):
        result[1].append(attribute)
      return result

    def is_listing(query_result):
      try:
        if query_result[0] == "uri-query-listing":
          return True
      except:
        pass
      return False
      
    def extract_listing(query_result):
      return query_result[1]
      

  def __init__(self,
               uri,
               qsystem,
               permissions):
    self._getter = self._ungettable
    self._setter = self._unsettable
    self._attribute_setter = None

    self._queue_system = qsystem
    self._parse(uri, permissions)


  def read(self, permissions):
    return self._getter(permissions)

  def write(self, value, permissions):
    self._setter(value, permissions)

  def _unsettable(self, value, permissions):
    raise RuntimeError("The specified URI cannot be written.")

  def _ungettable(self, permissions):
    raise RuntimeError("The specified URI cannot be read.")

  def _use_attribute_setter(self, value, permissions):
    self._attribute_setter.set_attribute(value, permissions)

  def _parse_system_uri(self, uri_parts, permissions):
    # Start at the root
    current_node = self._queue_system
    
    for i, part in enumerate(uri_parts):
      part = part.strip()

      children = current_node.uri_children(permissions)
      attributes = current_node.uri_node_attributes(permissions)

      investigate_children = True
      investigate_attributes = True

      if i != len(uri_parts) - 1:
        investigate_attributes = False
      else:
        # If the uri was terminated with a /, we are referring to
        # the current node and are done.
        if part == "":
          break

      part_present_in_children = part in children
      part_present_in_attributes = part in attributes

      part_available = False
      if investigate_children and part_present_in_children:
        part_available = True
      if investigate_attributes and part_present_in_attributes:
        part_available = True

      if not part_available:
        raise uri_exception_not_found()
      
      # Prefer attributes when in the last part of the URI
      if investigate_attributes and part_present_in_attributes:
        self._getter = lambda perm : current_node.read_uri_attribute(part, perm)
        self._attribute_setter = self.attribute_setter(current_node, part)
        self._setter = self._use_attribute_setter
        return
      
      if investigate_children and part_present_in_children:
        current_node = children[part]

    # When an attribute is reached, the function is aborted. Hence, since
    # we haven't aborted until now, we are actually accessing a node
    content_lister = self.node_content_lister(current_node)
    self._getter = lambda perm : content_lister.content(perm)
    self._setter = self._unsettable
        
      
        

  def _parse_job_uri(self, uri_parts, permissions):
    if len(uri_parts) == 0:
      self._setter = self._unsettable
      self._getter = self._ungettable

  def _parse_queue_uri(self, uri_parts, permissions):
    pass
  
  def _parse_jobs_uri(self, uri_parts, permissions):
    pass

  def _parse(self, uri, permissions):
    self._getter = self._ungettable
    self._setter = self._unsettable

    if uri.startswith(self.queue_uri):
      uri = uri.replace(self.queue_uri, "", 1)
      self._parse_queue_uri(uri.split("/"), permissions)
      
    elif uri.startswith(self.job_uri):
      uri = uri.replace(self.job_uri, "", 1)
      self._parse_jobs_uri(uri.split("/"), permissions)

    elif uri.startswith(self.system_uri):
      uri = uri.replace(self.system_uri, "", 1)
      self._parse_system_uri(uri.split("/"), permissions)

    else:
       raise ValueError("Invalid URI. Doesn't start with accepted URI identifiers.")

