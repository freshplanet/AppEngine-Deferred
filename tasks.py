# -*- coding: utf-8 -*-
'''
Copyright 2014 FreshPlanet (http://freshplanet.com | opensource@freshplanet.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
import cPickle
import importlib
import logging
import random
import types

from google.appengine.api import taskqueue, modules
from google.appengine.ext import ndb
from google.appengine.ext.deferred import deferred
import webapp2


# List of task queue names we should use as default queues to execute the tasks
DEFAULT_QUEUES = ['default']

# Name of the module to import to load your application code.
# That's where you should have implemented your /_warmup handler.
# Use this to make sure all your code is available by the time we de-serialize the task.
# Alternatively you can use the appengine_config.py module to make sure all your modules are always loaded.
# Ex: "myapp.warmup"
WARMUP_MODULE = None

# If you want to execute your tasks on another module than the default one, specify the module name here.
# This feature is meant to be used this way:
#   - You use two modules with the same code, with different settings (scheduler, instance class...)
#   to optimize how you serve user-facing requests versus how you serve things that run in the background like your tasks.
#   - Both modules use versions with the same names, versions with the same name sharing the same code.
# Then we can redirect the tasks you enqueue to the right module and version,
# avoiding compatibility issues when you deploy new code to a new version.
BACKGROUND_MODULE = None


@ndb.tasklet
def addTask(queues, func, *args, **kwargs):
    """ Enqueue a task to execute the specified function call later from the task queue.
    
    Handle exceptions and dispatching to the right queue.
    
    @param queues: List of queues names. We will randomly select one to push the task into.
                    Can be 'default' to use default queues.
    
    @param func: The function to execute later

    @param _countdown: seconds to wait before calling the function
    @param _eta: timestamp defining when to call the function
    @param _name: Name to give the Task; if not specified, a name will be
        auto-generated when added to a queue and assigned to this object.
        Must match the _TASK_NAME_PATTERN regular expression: ^[a-zA-Z0-9_-]{1,500}$
    @param _target: specific version and/or module the task should execute on
    @param _raiseIfExists: if set to True, we raise the eventual TaskAlreadyExistsError
    
    @return: A Future that will yield True if the task could be enqueued.
    @rtype: ndb.Future
    """
    if not isinstance(queues, list):
        queues = DEFAULT_QUEUES
    
    _raiseIfExists = kwargs.pop('_raiseIfExists', False)
    taskName = kwargs.pop('_name', None)
    countdown = kwargs.pop('_countdown', None)
    eta = kwargs.pop('_eta', None)
    target = kwargs.pop('_target', None)
    
    if not target and BACKGROUND_MODULE:
        # Tasks from the default module are executed into the background module.
        # Tasks from other modules (stage, background) stays inside their module.
        if modules.get_current_module_name() == 'default':
            # Target mirror of current version to avoid compatibility issues
            # If that version does not exist, it will fall back to the default background version.
            target = modules.get_current_version_name() + '.' + BACKGROUND_MODULE
    
    success = False
    try:
        yield _defer(queues, func, args, kwargs, countdown, eta, taskName, target)
        success = True
            
    except (taskqueue.TaskAlreadyExistsError, taskqueue.TombstonedTaskError):
        # TaskAlreadyExistsError: a task with same name is in the queue
        # TombstonedTaskError: a task with same name has been in the queue recently
        if taskName:
            # If we specified a name it's to avoid duplicated so this error is expected
            logging.info("TaskAlreadyExistsError: task with name %s already enqueued.", taskName)
            if _raiseIfExists:
                raise
        else:
            logging.exception("Could not enqueue the task")
    except:
        logging.exception("Could not enqueue the task")
    
    raise ndb.Return(success)


def isFromTaskQueue(request=None):
    """ Check if we are currently running from a task queue """
    request = request or webapp2.get_request()
    # As stated in the doc (https://developers.google.com/appengine/docs/python/taskqueue/overview-push#Task_Request_Headers)
    # These headers are set internally by Google App Engine.
    # If your request handler finds any of these headers, it can trust that the request is a Task Queue request.
    # If any of the above headers are present in an external user request to your App, they are stripped.
    # The exception being requests from logged in administrators of the application, who are allowed to set the headers for testing purposes.
    return bool(request.headers.get('X-Appengine-TaskName'))


def getRetryCount():
    """ Returns the current number of times the current task is being retried """
    return int(webapp2.get_request().headers.get('X-Appengine-TaskRetryCount', 0))


def logAsRetried(message, *args, **kwargs):
    """
    Depending on # of times the task is being retried, we will increase the logging level.
    """
    if isFromTaskQueue():
        retryCount = getRetryCount()
        if retryCount == 0:
            level = logging.INFO
        elif retryCount == 1:
            level = logging.WARN
        elif retryCount == 2:
            level = logging.ERROR
        else:
            level = logging.CRITICAL
    else:
        level = logging.ERROR
    logging.log(level, message, *args, **kwargs)


def _defer(queues, func, funcArgs, funcKwargs, countdown=None, eta=None, taskName=None, target=None):
    """
    Our own implementation of deferred.defer.
    
    This allows:
    - using webapp2 as deferred handler and applying our middlewares
    - using task asynchronous API
    - using cPickle instead of pickle
    - logging headers at DEBUG level instead of INFO
    """
    payload = _serialize(func, funcArgs, funcKwargs)
    
    queueName = random.choice(queues)
    
    # We track which function is called so that it appears clearly in the App admin dash-board.
    # Note: if it's a class method, we only track the method name and not the class name.
    url = "/_cb/deferred/%s/%s" % (getattr(func, '__module__', ''), getattr(func, '__name__', ''))
    
    headers = {"Content-Type": "application/octet-stream"}
    
    task = taskqueue.Task(payload=payload, target=target, url=url, headers=headers,
                          countdown=countdown, eta=eta, name=taskName)
    
    return task.add_async(queueName)


class DeferredHandler(webapp2.RequestHandler):
    
    # Queue & task name are already set in the request log.
    # We don't care about country and name-space.
    _SKIP_HEADERS = {'x-appengine-country', 'x-appengine-queuename', 'x-appengine-taskname', 'x-appengine-current-namespace'}
    
    def post(self, *args, **kwargs):
        """ Executes a deferred task """
        # Add some task debug information.
        headers = []
        for key, value in self.request.headers.items():
            k = key.lower()
            if k.startswith("x-appengine-") and k not in self._SKIP_HEADERS:
                headers.append("%s:%s" % (key, value))
        logging.debug(", ".join(headers))
        
        # Make sure all modules are loaded
        if WARMUP_MODULE:
            importlib.import_module(WARMUP_MODULE)
        
        # Make sure we are called from the Task Queue (security)
        if isFromTaskQueue(self.request):
            try:
                func, args, kwds = cPickle.loads(self.request.body)
            except Exception:
                logging.exception("Permanent failure attempting to execute task")
                return
            
            try:
                func(*args, **kwds)
            except TypeError:
                logging.debug("Deferred function arguments: %s %s", args, kwds)
                raise
            except deferred.SingularTaskFailure as e:
                msg = "Failure executing task, task retry forced"
                if e.message:
                    msg += ": %s" % e.message
                logging.debug(msg)
                self.response.set_status(408)
            except deferred.PermanentTaskFailure:
                logging.exception("Permanent failure attempting to execute task")
            
        else:
            logging.critical('Detected an attempted XSRF attack: we are not executing from a task queue.')
            self.response.set_status(403)

#===========================================================================
# From google.appengine.ext.deferred.defer lib
#===========================================================================


def _invokeMember(obj, memberName, *args, **kwargs):
    """Retrieves a member of an object, then calls it with the provided arguments.
    
    Args:
      obj: The object to operate on.
      membername: The name of the member to retrieve from ojb.
      args: Positional arguments to pass to the method.
      kwargs: Keyword arguments to pass to the method.
    Returns:
      The return value of the method invocation.
    """
    return getattr(obj, memberName)(*args, **kwargs)


def _curry_callable(obj, args, kwargs):
    """Takes a callable and arguments and returns a task queue tuple.
    
    The returned tuple consists of (callable, args, kwargs), and can be pickled
    and unpickled safely.
    
    Args:
      obj: The callable to curry. See the module docstring for restrictions.
      args: Positional arguments to call the callable with.
      kwargs: Keyword arguments to call the callable with.
    Returns:
      A tuple consisting of (callable, args, kwargs) that can be evaluated by
      run() with equivalent effect of executing the function directly.
    Raises:
      ValueError: If the passed in object is not of a valid callable type.
    """
    if isinstance(obj, types.MethodType):
        return (_invokeMember, (obj.im_self, obj.im_func.__name__) + args, kwargs)
    elif isinstance(obj, types.BuiltinMethodType):
        if not obj.__self__:
            return (obj, args, kwargs)
        else:
            return (_invokeMember, (obj.__self__, obj.__name__) + args, kwargs)
    elif isinstance(obj, types.ObjectType) and hasattr(obj, "__call__"):
        return (obj, args, kwargs)
    elif isinstance(obj, (types.FunctionType, types.BuiltinFunctionType,
                          types.ClassType, types.UnboundMethodType)):
        return (obj, args, kwargs)
    else:
        raise ValueError("obj must be callable")


def _serialize(obj, args, kwargs):
    """Serializes a callable into a format recognized by the deferred executor.
    
    Args:
      obj: The callable to serialize. See module docstring for restrictions.
      args: Positional arguments to call the callable with.
      kwargs: Keyword arguments to call the callable with.
    Returns:
      A serialized representation of the callable.
    """
    curried = _curry_callable(obj, args, kwargs)
    return cPickle.dumps(curried, protocol=cPickle.HIGHEST_PROTOCOL)
