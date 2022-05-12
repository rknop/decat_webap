#!/usr/bin/python3

import sys
import os
import web

class HandlerBase(object):
    def __init__(self):
        self.response = ""

    def htmltop(self):
        self.response = "<!DOCTYPE html>\n"
        self.response += "<html>\n<head>\n<meta charset=\"UTF-8\">\n"
        self.response += "<title>Testing</title>\n"
        self.response += "</head>\n<body>\n"

    def htmlbottom(self):
        self.response += "\n</body>\n</html>\n"
        
    def GET( self, *args, **kwargs ):
        return self.do_the_things( *args, **kwargs )

    def POST( self ):
        return self.do_the_things( *args, **kwargs )


class HelloWorld(HandlerBase):
    def do_the_things(self):
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        self.response += "<p>Hello, world.</p>\n"
        self.response += "<p>UID: " + str(os.getuid()) + "</p>\n";
        pg = web.ctx.environ["mod_wsgi.process_group"] if "mod_wsgi.process_group" in web.ctx.environ else None
        self.response += "<p>process group: \"" + str(web.ctx.environ["mod_wsgi.process_group"]) + "\""
        # self.response += "<pre>\n"
        # with open("/home/raknop/secret/test.txt") as ifp:
        #     for line in ifp:
        #         self.response += line
        # self.response += "</pre>\n"

        self.response += "<p>sys.argv = {}</p>\n".format( sys.argv )
        
        self.response += "<ul>\n"
        for key in web.ctx.environ:
            self.response += "<li>{} = {}</li>\n".format( key, web.ctx.environ[key] )
        self.response += "</ul>\n"

        
        self.htmlbottom()
        return self.response

class Blah(HandlerBase):
    def do_the_things (self, arg1, arg2=None, arg3=None, **kwargs ):
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        self.response += f"<p>arg1: {arg1}</p>\n"
        self.response += f"<p>arg2: {arg1}</p>\n"
        self.response += f"<p>arg3: {arg1}</p>\n"
        self.response += f'<p>kwargs: {kwargs}</p>\n'
        self.response += f"<p>web.data(): {web.data()}</p>\n"
        webinput = web.input();
        self.response += f"<p>webinput: {webinput}</p>\n"
        self.response += f"<p>webinput.keys(): {webinput.keys()}</p>\n"
        try:
            self.response += f"<p>webinput.rbtype: {webinput.rbtype}</p>\n"
        except KeyError as e:
            self.response += f"<p>KeyError looking for webinput.rbtype: {e}</p>\n"
        self.response += f"<p>web.ctx.environ['PATH_INFO']: {web.ctx.environ['PATH_INFO']}</p>\n"
        self.htmlbottom()
        return self.response
    
urls = (
    '/blah/(.+)', 'Blah',
    '/.*', 'HelloWorld',
    )

application = web.application(urls, globals()).wsgifunc()
