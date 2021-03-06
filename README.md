## AppEngine Deferred module improved ##

The AppEngine Python SDK has a great module when it comes to easily run some code "later": [the deferred library](https://developers.google.com/appengine/articles/deferred)
(if you are unfamiliar with this library, please have a look at its documentation first).

However the library has a few drawbacks that we solve here:

- No asynchronous method ([issue 9530](https://code.google.com/p/googleappengine/issues/detail?id=9530))
- The internal request handler inherits from webapp instead of webapp2 which can be an issue when using some webapp2 features (like webapp2.get_request)
- It uses pickle instead of leveraging cPickle available with the Python 2.7 runtime.
- The Route it uses is internal and will skip your middlewares unless you define them in a special appengine_config.py module.
- Task could fail to properly execute when hitting a fresh instance which did not had all code loaded
- Logging is a bit verbose: it uses the INFO level to show task headers that are redundant with what we can see by default in the AppEngine logs

We also added a couple of features to it:

- Explicit URLs for tasks: instead of just seeing */_ah/queue/deferred* in your logs you will see something like */_cb/deferred/app.module.name/funcName*
- Possibility to spread tasks over several queues in case of high throughput
- Improved logging in case of error
- Possibility to tie it to a *background* module to optimize serving requests

And some helpers:
- isFromTaskQueue(): Check if we are currently running from a task queue
- getRetryCount(): Returns the current number of times the current task is being retried
- logAsRetried(): Depending on # of times the task is being retried, we will increase the logging level.

Our module still makes use of the deferred exceptions types ```SingularTaskFailure``` and ```PermanentTaskFailure``` to be compatible with existing code.

### Installation ###
1. Copy the [tasks](https://github.com/freshplanet/AppEngine-Deferred/blob/master/tasks.py) module inside your project.
2. Add a route to your webapp2 application mapping to where you included the tasks module:
```webapp2.Route('/_cb/deferred/<module>/<name>', 'tasks.DeferredHandler')```
3. Optionally, better integrate with your application by updating the tasks module parameters:

- ```tasks.WARMUP_MODULE```
- ```tasks.DEFAULT_QUEUES```
- ```tasks.BACKGROUND_MODULE```

(See the tasks module documentation for more details)

--

**GAME DEVELOPMENT in NYC at FreshPlanet/SongPop**

Join us at our new offices across from the Empire State Building on 34th and 5th Avenue.
FreshPlanet is a NYC based mobile game development firm and we are looking for senior Python engineers to join our back-end team.

Work on back-end services that support mobile games entertaining millions of players around the world.

Please contact Tom Cassidy (tcassidy@freshplanet.com) or apply at http://freshplanet.com/jobs/
