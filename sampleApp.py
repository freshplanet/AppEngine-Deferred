import logging

from google.appengine.ext import ndb
import webapp2

import tasks


class DemoHandler(webapp2.RequestHandler):
    """
    Basic handler that trigger some code to be executed later.
    """
    def get(self):
        
        # If you have the ndb.toplevel middleware you can just fire-and-forget:
        tasks.addTask('default', doStuff, "foo")
        
        # Otherwise it is recommended to call get_result before exiting the request handler:
        tasks.addTask('default', doStuff, "bar").get_result()
        
        self.response.write("Task enqueued")

        
def doStuff(what):
    logging.info("Doing stuff: %s", what)


app = webapp2.WSGIApplication([
    webapp2.Route('/',    DemoHandler),
    webapp2.Route('/_cb/deferred/<module>/<name>', tasks.DeferredHandler)
])
app = ndb.toplevel(app)
