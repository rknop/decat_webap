#!/usr/bin/python
# -*- coding: utf-8 -*-

# https://www.legacysurvey.org/viewer?ra=30.1530962&dec=-5.0236864&zoom=16&layer=dr8

import sys
import math
import os
import pathlib
import re
import time
import random
import json
import psycopg2
import psycopg2.extras
import psycopg2.extensions
import base64
from io import BytesIO
import uuid
import datetime
import dateutil.parser
import pytz
import gzip
import web
import numpy
import pandas
from web import form
import PIL
import PIL.Image

scriptdir = str( pathlib.Path( __file__ ).parent )
if scriptdir not in sys.path:
    sys.path.insert(0, scriptdir )

from webapconfig import webapfullurl, webapdir, webapdirurl, DBdata, DBname
from util import dtohms, dtodms, radectolb, mjd, dateofmjd, parsedms, sanitizeHTML
from decatdb import DB, ObjectTable, ExposureTable

def adapt_numpy_int64( val ):
        return psycopg2.extensions.AsIs( val )
psycopg2.extensions.register_adapter( numpy.int64, adapt_numpy_int64 )
    

# ======================================================================

class HandlerBase(object):
    def __init__(self):
        self.response = ""
        self.idordinal = 0
        
        with open( f'{DBdata}/dbuser' ) as ifp:
            user = ifp.readline().strip()
        with open( f'{DBdata}/dbpasswd' ) as ifp:
            password = ifp.readline().strip()
        with open( f'{DBdata}/dbhost' ) as ifp:
            host = ifp.readline().strip()
        with open( f'{DBdata}/dbport' ) as ifp:
            port = ifp.readline().strip()
        with open( f'{DBdata}/{DBname}' ) as ifp:
            database = ifp.readline().strip()
        # sys.stderr.write( f'sys.path={sys.path}' )
        # sys.stderr.write("decatview.py connecting to database with dbname='{}' user='{}' host='{}' password='{}'\n"
        #                  .format( database, user, host, password ))
        # sys.stderr.write( f'DBdata={DBdata}, DBname={DBname}\n' )
        # sys.stderr.write( f'webapdir={webapdir}, webapdirurl={webapdirurl}, webapfullurl={webapfullurl}\n' )
        # sys.stderr.write(f'web.ctx.home={web.ctx.home}\n')
        self.db = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'"
                                   .format( database, user, host, password ))
        # sys.stderr.write("db.status is {}\n".format(self.db.status))

    def finalize(self):
        self.db.close()
        
    def htmltop(self):
        self.response = "<!DOCTYPE html>\n"
        self.response += "<html lang=\"en\">\n<head>\n<meta charset=\"UTF-8\">\n"
        self.response += "<link rel=\"stylesheet\" href=\"{}decat.css\">\n".format( webapdirurl )
        self.response += "<script src=\"{}decatview.js\" type=\"module\"></script>\n".format( webapdirurl )
        self.response += "<title>DECaT LBL Pipeline Candidate Viewer</title>\n"
        self.response += "</head>\n<body>\n"

    def htmlbottom(self):
        self.response += "\n</body>\n</html>\n"

    def hidden_state( self, omit=[] ):
        for val in self.state:
            if not val in omit:
                self.response += form.Input( name=val, id="{}{}".format( val, self.idordinal ),
                                             type="hidden", value=self.state[val] ).render() + "\n"
        self.idordinal += 1
                
    def back_to_home( self, omit=[] ):
        self.response += '<form method=\"POST\" action=\"{}\">\n'.format( webapfullurl )
        self.hidden_state( omit=omit )
        self.response += "<p><button class=\"link\" type=\"submit\">Back to Home</button></p>\n"
        self.response += "</form>\n"
                
    def set_state( self ):
        # Ugh.  All state for all different pages.  Ugly.
        # Rob, do this better.  Maybe.
        self.state = { "date0": "",
                       "date1": "",
                       # "onlygallat": False,
                       # "gallatincexc": "exc",
                       "minb": 0,
                       "maxb": 90,
                       "stackorindiv": "all",
                       "allpropornot": "inc",
                       "whichprops": [ '2021B-0149', '2021A-0275', '2020B-0053' ],
                       "showvetting": False,
                       "offset": 0,
                       "numperpage": 100,
                       "exposure": "",
                       "orderby": "real/bogus",
                       "ccds": "",
                       "showrb": False,
                       "whattodo": "Show Objects",        # ROB THINK ABOUT THIS
                       "mindet": 5,
                       "rbcut": 0.6,
                       "minrb": 5,
                       "numdays": 3,
                       "minmag": 25,
                       "maxmag": 15,
                       "limitbyradec": False,
                       "raformat": "hms",
                       "ra": "",
                       "decformat": "dms",
                       "dec": "",
                       "searchunit": "arcsec",
                       "searchradius": "",
                       "ccds": "",
                       "orderby": "real/bogus",
                       "showrb": True,
                       "candidate": "",
                       "user": None,
                       "password": None,
                       "vetfieldtype": None,
                       "vetprerated": None,
                       "vetted order": "random",
        }
        # ROB!  Remember to give defaults for the array quantities
        self.webinput = web.input( whichprops=self.state['whichprops'] )
        for stateval in self.state:
            if stateval in self.webinput:
                self.state[stateval] = self.webinput[stateval]
        # ROB!  This is terrible!  You really need a totally different
        #  infrastructure
        for key, value in self.webinput.items():
            if key[0:7] == "vetcond":
                self.state[key] = value
        # errstr = f"Before hacking: type(whichprops)={type(self.state['whichprops'])}\n"
        # for i, val in enumerate(self.state["whichprops"]):
        #     errstr += f"{i:2d} : {val}\n"
        # sys.stderr.write( errstr )
        # ARGH.  web.py has, I think, irritating handling of list parameters.
        # I'd call it a bug.
        if ( len(self.state["whichprops"]) == 1 ) and ( self.state["whichprops"][0][0:2] == "['" or
                                                        self.state["whichprops"][0][0:2] == '["' ):
            self.state["whichprops"][0] = self.state["whichprops"][0][2:-2]
        # errstr = f"After hacking: type(whichprops)={type(self.state['whichprops'])}\n"
        # for i, val in enumerate(self.state["whichprops"]):
        #     errstr += f"{i:2d} : {val}\n"
        # sys.stderr.write( errstr )
        # sys.stderr.write( f'whichprops = {self.state["whichprops"]}' )
                
    # ========================================

    def prevnext( self, state, numobjs, formtarget="showexp" ):
        nextarr = []
        prevarr = []
        nextord = self.idordinal
        prevord = nextord + 1
        self.idordinal += 2
        for key in state:
            if key=="offset":
                nextvalue = int(state["offset"]) + int(state["numperpage"])
                prevvalue = int(state["offset"]) - int(state["numperpage"])
                prevvalue = prevvalue if prevvalue > 0 else 0
            else:
                nextvalue = prevvalue = state[key]
            nextarr.append( form.Hidden( key, value=nextvalue, id="{}{}".format(key, nextord) ) )
            prevarr.append( form.Hidden( key, value=prevvalue, id="{}{}".format(key, prevord) ) )
        nextarr.append( form.Button( name="Next {}".format( state["numperpage"] ),
                                     id="Next_{}.{}".format( state["numperpage"], self.idordinal ),
                                     html="Next {}".format( state["numperpage"] ),
                                     type="submit",
                                     formaction="{}{}".format( webapfullurl, formtarget ) ) )
        prevarr.append( form.Button( name="Previous {}".format( state["numperpage"] ),
                                     id="Previous_{}.{}".format( state["numperpage"], self.idordinal ),
                                     html="Previous {}".format( state["numperpage"] ),
                                     type="submit",
                                     formaction="{}{}".format( webapfullurl, formtarget ) ) )
        
        nextform = form.Form( *nextarr )
        prevform = form.Form( *prevarr )

        self.back_to_home()

        # self.response += ( "<p>In prevnext, offset={}, numperpage={}, numobjs={}</p>\n"
        #                    .format( state["offset"], state["numperpage"], numobjs ) )
        
        if int(state["offset"]) > 0:
            self.response += "<form method=\"Post\">\n"
            self.response += prevform.render()
            self.response += "</form>\n"
        if int(state["offset"]) + int(state["numperpage"]) < numobjs:
            self.response += "<form method=\"Post\">\n"
            self.response += nextform.render()
            self.response += "</form>\n"

    # ========================================
        
    def show_objecttable( self, objtable, showvetting=False, goodbad=None, user=None, passwd=None ):

        setallunsetbadbutton = ""
        if goodbad is not None:
            setallunsetbadbutton = "<p>"
            button = form.Button( name="makethingsbad", html="Make All Unset Bad", )
            # I'm using something undocumented in web.py here, which is very dangerous
            # (Documentation is very sparse.)
            button.attrs['data-user'] = user
            button.attrs['data-password'] = passwd
            setallunsetbadbutton += button.render()
            setallunsetbadbutton += "</p>"

        self.response += setallunsetbadbutton
            
        self.response += "<form method=\"POST\" action=\"{}showcand\">\n".format( webapfullurl )
        self.hidden_state( omit=["candidate"] )
        
        self.response += "<table class=\"maintable\">\n"
        self.response += "<tr><th>Info</th><th>New</th><th>Ref</th><th>Sub</th></tr>\n"
        
        for dex, row in objtable.iterrows():
            candid = row["cid"]
            objid = dex # row["oid"]
            rb = row["rb"] if ( "rb" in row ) else "n/a"
            ra = row["ra"]
            dec = row["dec"]
            ccdnum = row["ccdnum"]
            filename = row["exposurename"]
            if ( "scijpg" in row ) and isinstance( row["scijpg"], str ):
                scib64 = row["scijpg"].replace("\n","")
            else:
                scib64 = None
            if ( "refjpg" in row ) and isinstance( row["refjpg"], str ):
                refb64 = row["refjpg"].replace("\n","")
            else:
                refb64 = None
            if ( "diffjpg" in row ) and isinstance( row["diffjpg"], str ):
                diffb64 = row["diffjpg"].replace("\n","")
            else:
                diffb64 = None
                
            self.response += "<tr>\n"
            self.response += "<td>Candidate: "
            self.response += form.Button( name="candidate", id="candidate_button_{}".format(candid),
                                          value=candid, class_="link", html=candid ).render()
            self.response += "<br>\n"
            if goodbad is None:
                self.response += "<b>rb: {:.4f}</b><br>\n".format( rb )
            self.response += u"α: {} &nbsp;&nbsp; δ: {}<br>\n".format( dtohms( ra ), dtodms( dec ) )
            self.response += "File: {}<br>ccd: {}<br>\n".format(filename.replace(".fz",""), ccdnum)
            self.response += "Obj ID: {}\n".format( objid )

            if scib64 is None:
                self.response += "<td>(Sci cutout missing)</td>\n"
            else:
                self.response += ( "<td><img src=\"data:image/jpeg;base64,{}\" "
                                   "width=204 height=204 alt=\"New\"></td>\n".format( scib64 ) )

            if refb64 is None:

                self.response += "<td>(Ref cutout missing)</td>\n"
            else:
                self.response += ( "<td><img src=\"data:image/jpeg;base64,{}\" "
                                   "width=200 height=200 alt=\"Ref\"></td>\n".format( refb64 ) )

            if diffb64 is None:
                self.response += "<td>(Diff cutout missing)</td>\n"
            else:
                self.response += ( "<td><img src=\"data:image/jpeg;base64,{}\" "
                                   "width=200 height=200 alt=\"Sub\"></td>\n".format( diffb64 ) )

            if goodbad is not None:
                isgood = False
                isbad = False
                if objid in goodbad:
                    if goodbad[objid] == "good":
                        isgood = True
                    if goodbad[objid] == "bad":
                        isbad = True
                self.response += "<td>\n"
                self.response += ( f'<input type="radio" value="good" name="{objid}statusset" '
                                   f'id="{objid}statusgood" data-user="{user}" data-password="{passwd}" '
                                   f'data-objid="{objid}" data-goodbad="good">\n' )
                self.response += "<label for=\"{obj}statussgood\">Good</label><br>\n".format( obj=objid )
                self.response += ( f'<input type="radio" value="bad" name="{objid}statusset" '
                                   f'id="{objid}statusbad" data-user="{user}" data-password="{passwd}" '
                                   f'data-objid="{objid}" data-goodbad="bad">\n' )
                self.response += "<label for=\"{obj}statussbad\">Bad</label><br>\n".format( obj=objid )
                self.response += "Current: "
                if isgood:
                    self.response += "<span id=\"{}status\" class=\"good\">Good</span>\n".format(objid)
                elif isbad:
                    self.response += "<span id=\"{}status\" class=\"bad\">Bad</span>\n".format(objid)
                else:
                    self.response += "<span id=\"{}status\"><i>(not set)</i></span>\n".format(objid)

            elif showvetting:
                self.response += "<td class=\"vettinglist\">"
                first = True
                for username, score in zip( row["users"], row["goodbads"] ):
                    if first:
                        first = False
                    else:
                        self.response += "<br>"
                    self.response += "{username}: <span class=\"{score}\">{score}</span>".format( username=username,
                                                                                              score=score )
                self.response += "</td>"
                    
            self.response += "</tr>\n"
        self.response += "</table>\n</form>\n"

        self.response += setallunsetbadbutton

        
    def check_scan_user( self, data ):
        if "user" not in data:
            rval = { "error": "No user specified" }
            return json.dumps(rval)
        if "password" not in data:
            rval = { "error": "No password given" }
            return json.dumps(rval)
        cursor = self.db.cursor( cursor_factory = psycopg2.extras.RealDictCursor )
        query = "SELECT username,password FROM scanusers WHERE username=%s"
        cursor.execute( query, ( data["user"], ) )
        rows = cursor.fetchall()
        cursor.close()
        if len(rows) == 0:
            rval = { "error": "Unknown user {}".format( data["user"] ) }
            return json.dumps(rval)
        if rows[0]["password"] != data["password"]:
            rval = { "error": "Incorrect password for {}".format( data["user"] ) }
            return json.dumps(rval)
        return None
        
    def GET( self ):
        response = self.do_the_things()
        self.finalize()
        return response

    def POST( self ):
        response = self.do_the_things()
        self.finalize()
        return response

# ======================================================================

class FrontPage(HandlerBase):
    def do_the_things(self):
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        self.set_state()

        self.response += "<h1>DECAT extragalactic subtraction viewer</h1>\n"

        self.response += "<hr>\n"
        self.response += "<h2>Exposure Search</h2>\n"
        self.response += "<p>Enter dates as yyyy-mm-dd or yyyy-mm-dd hh:mm:ss or yyyy-mm-dd hh:mm:ss-05:00\n"
        self.response += "(the last one indicating a time zone that is 5 hours before UTC)."
        self.response += "<form method=\"POST\" action=\"{}listexp\"><p>\n".format( webapfullurl )
        self.response += "<p>List exposures from date\n";
        self.response += form.Input( name="date0", value=self.state["date0"], type="text" ).render()
        self.response += "\n&nbsp;&nbsp;to&nbsp;&nbsp;\n"
        self.response += form.Input( name="date1", value=self.state["date1"], type="text" ).render() + "</p>\n"
        self.response += "<p>Galactic latitude between ± "
        self.response += form.Input( name="minb", type="text", size="4", value=self.state["minb"] ).render()
        self.response += "°&nbsp;and&nbsp;"
        self.response += form.Input( name="maxb", type="text", size="4", value=self.state["maxb"] ).render()
        self.response += "°</p>\n<p>"
        self.response += "<br>" + form.Dropdown( name="stackorindiv", id="stackorindiv",
                                                 args=[ ("all", "Show both stacks and individual images"),
                                                        ("indiv", "Show only individual images, not stacks"),
                                                        ("stack", "Show only stacks, not individual images") ],
                                                 value=self.state["stackorindiv"] ).render()
        self.response += "<br><br>" + form.Dropdown( name="allpropornot", id="allpropornot",
                                                args=[ ("inc","Include all proposal IDs"),
                                                       ("some","Only proposal IDs:") ],
                                                value=self.state["allpropornot"] ).render()
        self.response += "<div id=\"whichprops_div\" style=\"display: none\">\n"
        for propid,displayprop in [ ('2021A-0113', '2021A-0113: Graham DDF Spring 2021'),
                                    ('2021B-0149', '2021B-0149: Graham DDF Fall 2021'),
                                    ('2021A-0275', '2021A-0275: Rest YSE'),
                                    ('2020B-0053', '2020B-0053: Brount DEBASS') ]:
            self.response += "\n<br>" + form.Checkbox( id=f"whichprops_{propid}", name="whichprops", value=propid,
                                                       checked=(propid in self.state["whichprops"]) ).render()
            self.response += f"\n<label for=\"whichprops_{propid}\">{displayprop}</label>"
        self.response += "\n</div>\n"
        self.response += "<p>" + form.Input( name="submit", type="submit", value="List Exposures" ).render()
        self.response += "</p>\n</form>\n"

        self.response += "<hr>\n"
        self.response += "<h2>Candidate Lookup</h2>\n"
        self.response += "<form method=\"POST\" action=\"{}showcand\">\n".format( webapfullurl )
        self.response += form.Input( name="candidate", id="cand49152", type="text" ).render()
        self.response += "\n<br>\n"
        self.response += form.Input( name="submit", type="submit", value="Show" ).render()
        self.response += "\n</form>\n"
        
        self.response += "<hr>\n"
        self.response += "<h2>Candidate Search</h2>\n"
        self.response += "<b>Note: this doesn't seem to work now, times out.</b>"
        self.response += "<form method=\"POST\" action=\"{}findcands\"></p>\n".format( webapfullurl )
        self.response += "<table class=\"candsearchparams\">\n"
        self.response += "<tr><td>Min # Detections:</td><td>"
        self.response += form.Input( name="mindet", type="number",
                                     min="1", max="1000", step="1", value=self.state["mindet"] ).render()
        self.response += "</td></tr>\n<tr><td>Min # rb≥"
        self.response += form.Input( name="rbcut", type="number", min="0", max="1",
                                     step="any", size="4", value=self.state["rbcut"] ).render()
        self.response += "</td><td>"
        self.response += form.Input( name="minrb", type="number", min="0", max="1000",
                                     step="1", value=self.state["minrb"] ).render()
        self.response += "</td></tr>\n<tr><td>Min diff. days detected:</td><td>"
        self.response += form.Input( name="numdays", type="number", min="0", max="1000",
                                     step="1", value=self.state["numdays"] ).render()
        self.response += "</td></tr>\n<tr><td>Min (brightest) mag ≤</td><td>"
        self.response += form.Input( name="minmag", type="number", min="15", max="30",
                                     step="0.1", value=self.state["minmag"] ).render()
        self.response += "</td></tr>\n<tr><td>Max (dimmest) mag ≥</td><td>"
        self.response += form.Input( name="maxmag", type="number", min="15", max="30",
                                     step="0.1", value=self.state["maxmag"] ).render()
        self.response += "</td></tr>\n<tr><td>Limit by RA/Dec?</td><td class=\"left\">"
        self.response += form.Input( name="limitbyradec", type="checkbox", value=self.state["limitbyradec"] ).render()
        self.response += "</td></tr>\n<tr><td>RA ("
        self.response += form.Dropdown( name="raformat", value=self.state["raformat"],
                                        args=[ ("hms","h:m:s"), ("decimalhrs", "decimal hrs"),
                                               ("decimaldeg", "decimal deg") ] ).render()
        self.response += "):</td><td>"
        self.response += form.Input( name="ra", type="text", value=self.state["ra"] ).render()
        self.response += "</td></tr>\n<tr><td>Dec ("
        self.response += form.Dropdown( name="decformat", value=self.state["decformat"],
                                        args=[ ("dms","d:m:s"), ("decimaldeg", "decimal °") ] ).render()
        self.response += "):</td><td>"
        self.response += form.Input( name="dec", type="text", value=self.state["dec"] ).render()
        self.response += "</td></tr>\n<tr><td>Search radius in "
        self.response += form.Dropdown( name="searcunit", value=self.state["searchunit"],
                                        args=[ ("arcsec", "″"), ("arcmin", "′"), ("degree", "°") ] ).render()
        self.response += "</td><td>: "
        self.response += form.Input( name="searchradius", type="text", value=self.state["searchradius"] ).render()
        self.response += "</td></tr>\n<tr><td>"
        self.response += form.Input( name="submit", type="submit", value="Search" ).render()
        self.response += "</td></tr>\n</table>\n</form>\n"


        self.response += "<hr>"
        self.response += "<h2>Candidate Vetting</h2>\n"
        self.response += "<form method=\"POST\" action=\"{}ratecands\"></p>\n".format( webapfullurl )
        self.response += "<table class=\"candsearchparams\">\n"
        self.response += "<tr><td>Username:</td><td>"
        self.response += form.Input( name="user", type="text" ).render()
        self.response += "</td></tr>\n"
        self.response += "<tr><td>Password:</td><td>"
        self.response += form.Input( name="password", type="text" ).render()
        self.response += "</td></tr>\n"
        self.response += "<tr><td>Type of field:</td><td class=\"left\">"
        self.response += form.Dropdown( name="vetfieldtype", args=[ ("gal", "Galactic"),
                                                                    ("exgal", "Extragalactic") ],
                                        value="exgal" ).render()
        self.response += "</td></tr>\n"
        self.response += "<tr><td>Objects vetted by others?</td><td class=\"left\">"
        self.response += form.Dropdown( name="vetprerated", args=[ ("yes", "Yes"), ("no", "No") ],
                                        value="no" ).render()
        self.response += "</td></tr>\n<tr><td>"
        self.response += form.Input( name="submitvet", type="submit", value="Show Objects" ).render()
        self.response += "</td><td>(This takes a little while, be patient.)</td></tr></table>\n"
        self.response += "</form>\n"

        self.response += "<h3>Show vetted candidates</h3>\n"
        self.response += form.Button( name="Add Vet Condition", id="Add Vet Condition",
                                      html="Add Condition" ).render()
        self.response += "<form method=\"POST\" action=\"{}showrated\">\n".format( webapfullurl )
        self.response += "<div id=\"vetconditions\">\n</div>\n"
        self.response += "<p>Order by: "
        self.response += form.Dropdown( name="vetted order", args=[ ("random", "Random Selections"),
                                                                    ("standard", "MJD, CCDNUM, object_id" ) ],
                                        value="standard" ).render()
        self.response += "</p>\n"
        self.response += "<p>(Gratuitously sorts by exposure, ccd, object id.  Could take a long time.)</p>"
        self.response += form.Input( name="submitshowvet", type="submit", value="Show Vetted Objects" ).render()
        self.response += "</p>\n</form>\n"
        
        self.htmlbottom()
        # sys.stderr.write( "About to return self.response\n" )
        return self.response
        
# ======================================================================

class FindCandidates(HandlerBase):
    def do_the_things(self):
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        self.response += "<h2>Broken</h2>\n"
        self.response += "<p>Finding candidates is currently broken, until Rob fixes its rb handling.</p>\n"
        return self.response
        
        self.set_state()
        
        minnum = int( self.state["mindet"] )
        rbcut = float( self.state["rbcut"] )
        minrbnum = int( self.state["minrb"] )
        mindays = int( self.state["numdays"] )
        minmag = float( self.state["minmag"] )
        maxmag = float( self.state["maxmag"] )
        useradec = self.state["limitbyradec"]
        raformat = self.state["raformat"]
        rastr = self.state["ra"]
        decformat = self.state["decformat"]
        decstr = self.state["dec"]
        radformat = self.state["searchunit"]
        try:
            radius = float( self.state["searchradius"] )
        except ValueError:
            radius = 0.

        self.back_to_home()

        if useradec:
            try:
                if raformat == "hms":
                    ra = parsedms( rastr )
                    if ra is None:
                        raise ValueError( "Error parsing RA." )
                    ra = 15. * ra
                elif raformat == "decimalhrs":
                    ra = 15. * float( rastr )
                elif raformat == "decimaldeg":
                    ra = float(rastr)
                else:
                    raise ValueError( "Unknown RA format \"" + sanitizeHTML( raformat, True ) + "\"" )
            except Exception as e:
                self.response += ( "<p>Error parsing RA: " + sanitizeHTML( rastr, True ) + " as " +
                                   sanitizeHTML( raformat, True) + "</p>" )
                self.htmlbottom()
                return self.response
            try:
                if decformat == "dms":
                    dec = parsedms( decstr )
                    if dec is None:
                        raise ValueError( "Error parsing Dec." )
                elif decformat == "decimaldeg":
                    dec = float( decstr )
                else:
                    raise ValueError( "Unknown Dec format \"" + sanitizeHTML( decformat, True ) + "\"" )
            except Exception as e:
                self.response += ( "<p>Error parsing Dec: " + sanitizeHTML( decstr, True ) + " as " +
                                   sanitizeHTML( decformat, True) + "</p>" )
                self.htmlbottom()
                return self.response
            if radformat == "arcmin":
                radius /= 60.
                radstr = "′"
            elif radformat == "arcsec":
                radius /= 3600.
                radstr = "″"
            else:
                radstr = "°"
            radecquerystr = 'WHERE q3c_radial_query(c.ra,c.dec,{},{},{}) '.format( ra, dec, radius )
            responsestr = ' within {:.04f}° of ({:.4f}, {:.4f})'.format( radius, ra, dec )
        else:
            radecquerystr = ''
            responsestr = ''
                
        self.response += ( "<h4>Candidates with ≥{} detections (≥{} with rb≥{:.2f})<br>\n"
                           .format( minnum, minrbnum, rbcut ) )
        self.response += ( "seen at ≥{} days apart with brightest mag. ≤{:.2f} and dimmest mag. ≥{:.2f}{}</h4>\n"
                           .format( mindays, minmag, maxmag, responsestr ) )
                           
        cursor = self.db.cursor( cursor_factory = psycopg2.extras.RealDictCursor )

        # One early days check suggested this returned 1/20 of the whole candidates table
        query = ( "SELECT c.id,count(o.id) AS countobj,max(o.rb) AS maxrb "
                  "FROM candidates c "
                  "INNER JOIN objects o ON o.candidate_id=c.id " )
        query += "{}".format( radecquerystr )
        query += ( "GROUP BY (c.id) "
                   "HAVING count(o.id)>=%s AND max(o.rb)>=%s " 
                   "ORDER BY c.id " )
        # sys.stderr.write( "Sending query {}\n".format( cursor.mogrify( query, (minnum,rbcut) ) ) )
        cursor.execute( query, ( minnum, rbcut ) )
        rows = cursor.fetchall()
        if len(rows) == 0:
            self.response += "<p>None found.</p>"
            self.htmlbottom()
            cursor.close()
            return self.response
        self.response += "<p>Initial query returned {} candidates</p>\n".format( len(rows) )
        # sys.stderr.write( "Initial query returned {} candidates\n".format( len(rows) ) )
        
        candlist = []
        cands = {}
        for row in rows:
            cid = row["id"]
            candlist.append( cid )
            cands[cid] = {}
            cands[cid]["numobj"] = row["countobj"]
            cands[cid]["jdlist"] = []
            cands[cid]["filterlist"] = []
            cands[cid]["maglist"] = []
            cands[cid]["rblist"] = []
            cands[cid]["numhighrb"] = 0
            cands[cid]["minjd"] = 1e32
            cands[cid]["maxjd"] = 0
            cands[cid]["minmag"] = 50
            cands[cid]["maxmag"] = 0

        query = ( "SELECT c.id AS candid,o.id AS objid,o.rb,o.mag,e.mjd,e.filter,c.ra,c.dec "
                  "FROM objects o "
                  "INNER JOIN subtractions s ON o.subtraction_id=s.id "
                  "INNER JOIN exposures e ON s.exposure_id=e.id "
                  "INNER JOIN candidates c ON o.candidate_id=c.id "
                  "WHERE c.id IN %s" )
        # sys.stderr.write( "Sending query {}\n".format( cursor.mogrify( query, ( tuple(candlist), ) ) ) )
        cursor.execute( query, ( tuple(candlist), )  )
        rows=cursor.fetchall()
        cursor.close()

        for row in rows:
            cid = row["candid"]
            mjd = row["mjd"]
            mag = row["mag"]
            filt = row["filter"]
            rb = row["rb"]
            ra = row["ra"]
            dec = row["dec"]
            cands[cid]["jdlist"].append( mjd )
            cands[cid]["filterlist"].append( filt )
            cands[cid]["maglist"].append( mag )
            cands[cid]["rblist"].append( rb )
            cands[cid]["ra"] = ra
            cands[cid]["dec"] = dec
            if mjd < cands[cid]["minjd"]:
                cands[cid]["minjd"] = mjd
            if mjd > cands[cid]["maxjd"]:
                cands[cid]["maxjd"] = mjd
            if mag < cands[cid]["minmag"]:
                cands[cid]["minmag"] = mag
            if mag > cands[cid]["maxmag"]:
                cands[cid]["maxmag"] = mag
            if rb >= rbcut:
                cands[cid]["numhighrb"] += 1

        # ****
        # self.response += "<ul>\n"
        # for c in candlist:
        #     self.response += ( "<li>{} ; Δjd={:.1f}; minmag={:.2f}; maxmag={:.2f}; numhighrb={}</li>\n"
        #                        .format( c, cands[c]["maxjd"] - cands[c]["minjd"], cands[c]["minmag"],
        #                                 cands[c]["maxmag"], cands[c]["numhighrb"], ) ) #cands[c]["rblist"] ) )
        # self.response += "</ul>\n"
        # ****
                
        candlist = [ c for c in candlist if ( ( cands[c]["numhighrb"] >= minrbnum ) and
                                              ( cands[c]["minmag"] <= minmag ) and
                                              ( cands[c]["maxmag"] >= maxmag ) and
                                              ( cands[c]["maxjd"] - cands[c]["minjd"] >= mindays ) ) ]

        self.response += "<p>After cuts, {} remain.</p>\n".format( len(candlist) )
        # sys.stderr.write( "After cuts, {} remain.\n".format( len(candlist) ) )
        
        self.response += "<form method=\"POST\" action=\"{}showcand\">\n".format( webapfullurl )
        self.hidden_state()
        self.response += "<table class=\"candlist\">"
        self.response += "<tr><th>Candidate</th>"
        self.response += "<th># Det</th>"
        self.response += "<th>rb≥{:.2f}</th>".format(rbcut)
        self.response += "<th>dimmest</th>"
        self.response += "<th>brightest</th>"
        self.response += "<th>t0</th>"
        self.response += "<th>t1</th>"
        self.response += "<th>ra</th>"
        self.response += "<th>dec</th>"
        self.response += "<th>ltcv</th></tr>\n"

        for cand in candlist:
            if False:
                jdlist = numpy.array( cands[cand]["jdlist"] )
                filterlist = numpy.array( cands[cand]["filterlist"] )
                maglist = numpy.array( cands[cand]["maglist"] )
                rblist = numpy.array( cands[cand]["rblist"] )
                dex = numpy.argsort( jdlist )
                jdlist = jdlist[dex]
                filterlist = filterlist[dex]
                maglist = maglist[dex]
                rblist = rblist[dex]
                datelist = numpy.empty( jdlist.shape, dtype=numpy.str )
                for i in range(0, len(datelist)):
                    Y, M, D = dateofmjd( jdlist[i] )
                    datelist[i] = "{:04n}-{:02n}-{:02n}".format( Y, M, D )
                uniqfilt = []
                for f in filterlist:
                    if not ( f in uniqfilt ):
                        uniqfilt.append( uniqfilt )

            y0, m0, d0 = dateofmjd( cands[cand]["minjd"] )
            y1, m1, d1 = dateofmjd( cands[cand]["maxjd"] )
                        
            self.response += ( "<tr><td><button class=\"link\" name=\"candidate\" "
                               "value=\"{cand}\">{cand}</button></td>".format( cand=cand ) )
            self.response += "<td>{}</td>".format( cands[cand]["numobj"] )
            self.response += "<td>{}</td>".format( cands[cand]["numhighrb"] )
            self.response += "<td>{:.2f}</td>".format( cands[cand]["maxmag"] )
            self.response += "<td>{:.2f}</td>".format( cands[cand]["minmag"] )
            self.response += "<td>{:04n}-{:02n}-{:02n}</td>".format( y0, m0, d0 )
            self.response += "<td>{:04n}-{:02n}-{:02n}</td>".format( y1, m1, d1 )
            self.response += "<td>{}</td>".format( dtohms( cands[cand]["ra"] ) )
            self.response += "<td>{}</td>".format( dtodms( cands[cand]["dec"] ) )
            if False:
                self.response += "<td><table class=\"ltcv\">\n  <tr>"
                for f in uniqfilt:
                    self.response += "<th>{}</th>".format(f)
                self.response += "<tr>\n"
                for f in uniqfilt:
                    self.response += "    <td><table class=\"ltcvf\">\n"
                    for i in range(0, len(datelist)):
                        if filterlist[i] == f:
                            self.response += "      <tr><td>{}</td><td>{:.02f}</td></tr>\n".format( datelist[i],
                                                                                                    maglist[i] )
                    self.response += "    </table></td>\n"
                self.response += "  </tr>\n</td>"
            self.response += "</tr>\n"

        self.htmlbottom()
        return self.response
    
# ======================================================================
    
class ListExposures(HandlerBase):
    def do_the_things(self):
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        self.set_state()
        rbtype = 1
        
        if len(self.state["date0"].strip()) == 0:
            self.state["date0"] = "1970-01-01"
        if len(self.state["date1"].strip()) == 0:
            self.state["date1"] = "2999-12-31"
        date0 = self.state["date0"].strip()
        date1 = self.state["date1"].strip()
        whichprops = self.state["whichprops"]
        minb = math.fabs( float( self.state["minb"] ) )
        maxb = math.fabs( float( self.state["maxb"] ) )
            
        try:    
            t0 = dateutil.parser.parse( date0 )
            if t0.tzinfo is None:
                t0 = pytz.utc.localize( t0 )
            t0 = t0.astimezone( pytz.utc )
            t1 = dateutil.parser.parse( date1 )
            if t1.tzinfo is None:
                t1 = pytz.utc.localize( t1 )
            t1 = t1.astimezone( pytz.utc )
        except ValueError as ex:
            self.response += "<p>...error parsing your dates... go back.</p>\n"
            self.htmlbottom()
            return self.response

        self.back_to_home()
        
        self.response += "<h4>Exposures from {} to {}\n".format( t0.isoformat(), t1.isoformat() )
        if minb > 0 or maxb < 90:
            self.response += f" with {minb:.2f}° ≥ |b| ≥ {maxb:.2f}° "
        self.response += "</h4>\n"

        if self.state['allpropornot'] == "some":
            self.response += ( f"<p style=\"font-weight: bold\">Including proposal"
                               f"{'' if len(self.state['whichprops'])==1 else 's'} "
                               f" {','.join(self.state['whichprops'])}</p>\n" )
            

        if self.state["allpropornot"] == "some":
            propids = self.state['whichprops']
        else:
            propids = None
        if self.state["stackorindiv"] == "indiv":
            includestack=False
        else:
            includestack = True
        if self.state["stackorindiv"] == "stack":
            onlystack=True
        else:
            onlystack=False
            
        exptab = ExposureTable.load( t0, t1, propids=propids, includestack=includestack, onlystack=onlystack,
                                     rbtype=rbtype, mingallat=minb, maxgallat=maxb )
        if len(exptab) == 0:
            self.response += "<p>No exposures!</p>\n"
            self.htmlbottom()
            return self.response
        exptab.load_nchips_checkpoint( checkpoint=27, column="nfinished" )
        exptab.load_nerrors()
        
        self.response += "<p>Number of objects per page: <input type=\"number\" name=\"numperpage\" value=100><br>\n"
        self.response += "Only include ccd numbers (comma-sep): "
        self.response += form.Input( name="ccds", type="text", value=self.state["ccds"] ).render()
        self.response += "\n<br>\n  Order by:\n"
        self.response += form.Radio( name="orderby", value=self.state["orderby"],
                                     args=[ ("real/bogus", "Real/Bogus"), ("objnum", "ObjectNum.") ] ).render()
        self.response += "\n<br>\n"
        self.response += form.Checkbox( id="showvetting", name="showvetting",
                                        value=self.state["showvetting"] ).render()
        self.response += " show manual vetting?</p>\n<p>"
        # self.hidden_state( omit=["ccds", "orderby", "showrb", "showvetting" ] )

        self.response += f"{len(exptab)} exposures<br>\n"
        self.response += "<table class=\"exposurelist\">\n"
        self.response += ( "<tr><th>Exposure</th><th>Filter</th><th>propid</th><th>t_exp</th>"
                           "<th>ra</th><th>dec</th><th>l</th><th>b</th>"
                           "<th>#Subs</th><th>#Done</th><th>N. Objects</th>"
                           "<th>rb>=0.6</th></tr>\n" )

        for eid, exposure in exptab.df.iterrows():
            ra = exposure["ra"]
            dec = exposure["dec"]
            l,b = radectolb( ra, dec )
            fnameclass = "stack" if "stack" in exposure['filename'] else "notstack"
            self.response += f'<tr class=\"{fnameclass}\"><td>{exposure["filename"]}</td>\n'
            self.response += f"  <td>{exposure['filter']}</td>\n"
            self.response += f"  <td>{exposure['proposalid']}</td>\n"
            self.response += f"  <td>{exposure['exptime']}</td>\n"
            self.response += f'  <td>{dtohms(ra)}</td>\n'
            self.response += f'  <td>{dtodms(dec)}</td>\n'
            self.response += f'  <td>{l:.02f}</td>\n'
            self.response += f'  <td>{b:.02f}</td>\n'
            self.response += f"  <td>{exposure['nsubs']}</td>\n"
            self.response += f"  <td>{exposure['nfinished']}</td>\n"
            self.response += f"  <td>{exposure['nobjs']}</td>\n"
            self.response += f"  <td>{exposure['nbigrbs']}</td>\n"
            self.response += ( f"  <td><button type=\"submit\" name=\"showobjects\" value=\"Show Objects\" "
                               f"data-exposure=\"{exposure['filename']}\">Show Objects</button></td>\n" )
            self.response += ( f"  <td><button type=\"submit\" name=\"showlog\" value=\"Show Log\" "
                               f"data-exposure=\"{exposure['filename']}\">Show Log</button></td>\n" )
            if exposure["nerrors"] > 0 :
                self.response += f"  <td class=\"bad\">{exposure['nerrors']} errors</td>\n"
            self.response += "</tr>\n"
            
        self.response += "</table>\n"

        self.htmlbottom()
        return self.response

# ======================================================================

class ShowExposure(HandlerBase):
    
    def do_the_things(self):
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        self.set_state()

        # sys.stderr.write( "ShowExposure webinput = {}, state = {}\n".format( json.dumps( self.webinput ),
        #                                                                      json.dumps( self.state ) ) )
                
        if self.state["whattodo"] == "Show Objects":
            self.show_objects()
        elif self.state["whattodo"] == "Show Log":
            self.show_log()
        else:
            self.response += "<p>Error, unknown whattodo \"{}\"</p>\n".format( self.state["whattodo"] )
            
        self.htmlbottom()
        return self.response

    # ========================================
    
    def show_objects( self ):
        offset = int( self.state["offset"] )
        if offset < 0: offset = 0
        numperpage = int( self.state["numperpage"] )
        filename = self.state["exposure"]
        orderby = self.state["orderby"]
        showvetting = self.state["showvetting"]
        rbtype = 1

        try:
            ccds = self.state["ccds"].strip()
            if len(ccds) > 0:
                ccdarr = [ int(x) for x in self.state["ccds"].split(",") ]
                if len(ccdarr) == 0: ccdarr = None
            else:
                ccdarr = None
        except Exception as e:
            self.response += f"<p><b>ERROR</b> parsing ccdlist {sanitizeHTML(ccds)}</p>\n"
            return
            
        objtab = ObjectTable.load_for_exposure( exposure_name=filename, ccdnums=ccdarr )
        objtab.loadrb( rbtype=rbtype )
        objtab.loadjpg()
        if showvetting:
            objtab.loadvetting()
        
        if orderby == "real/bogus":
            objtab.sortrb()
        elif orderby == "objnum":
            objtab.sortoid()

        numobjs = len(objtab)
        objtab.subset( offset, offset+numperpage )
        
        self.prevnext( self.state, numobjs )

        self.response += "<h3>Exposure: {}</h3>\n".format( filename )
        self.response += "<h4>Candidates starting at offset {} out of {}</h4>\n".format( offset, numobjs )

        self.response += "<p><b>ROB</b>: Print information about r/b</p>"
        
        self.show_objecttable( objtab.df, showvetting=showvetting )
        
        self.prevnext( self.state, numobjs )

    # ========================================
        
    def show_log( self ):
        query = ( "SELECT p.created_at,p.ccdnum,p.running_node,p.mpi_rank,p.notes,c.description,p.event_id "
                  "FROM processcheckpoints p "
                  "INNER JOIN exposures e ON p.exposure_id=e.id "
                  "LEFT JOIN checkpointeventdefs c ON p.event_id=c.id "
                  "WHERE e.filename=%s ORDER BY p.created_at" )
        cursor = self.db.cursor( cursor_factory = psycopg2.extras.RealDictCursor )
        cursor.execute( query, ( self.state["exposure"], ) )
        rows = cursor.fetchall()
        cursor.close()

        self.response += "<h2>{}</h2>\n".format (self.state["exposure"] )

        if len(rows) == 0:
            self.response += "<p>No log information.</p>\n"
            return

        self.response += "<p>Ran on {}</p>\n".format( rows[0]["running_node"] )

        hassub = [False] * 64
        hasobj = [False] * 64
        haserr = [False] * 64
        hasinf = [False] * 64
        for row in rows:
            if row["ccdnum"] == -1:
                if row["event_id"] == 999:
                    for i in range(0, 64):
                        haserr[i] = True
                if row["event_id"] == 998:
                    for i in range(0, 64):
                        hasinf[i] = True
            else:
                if row["event_id"] == 20:
                    hassub[row["ccdnum"]] = True
                if row["event_id"] == 27:
                    hasobj[row["ccdnum"]] = True
                if row["event_id"] == 999:
                    haserr[row["ccdnum"]] = True
                if row["event_id"] == 998:
                    hasinf[row["ccdnum"]] = True

        self.response += "<p>CCDs without subtraction: "
        for i in range(1, 63):
            if not hassub[i]:
                self.response += '&nbsp;<a href="#{num}">{num}</a>&nbsp;\n'.format( num=i )
        self.response += "</p>"

        self.response += "<p>CCDs without object detection: "
        for i in range(1, 63):
            if not hasobj[i]:
                self.response += '&nbsp;<a href="#{num}">{num}</a>&nbsp;\n'.format( num=i )
        self.response += "</p>"

        self.response += "<p>CCDs with errors logged: "
        for i in range(1, 63):
            if haserr[i]:
                self.response += '&nbsp;<a href="#{num}">{num}</a>&nbsp;\n'.format( num=i )
        self.response += "</p>"
        
        self.response += "<p>CCDs with info logged: "
        for i in range(1, 63):
            if hasinf[i]:
                self.response += '&nbsp;<a href="#{num}">{num}</a>&nbsp;\n'.format( num=i )
        self.response += "</p>"
        
        
        self.response += "<p>Jump to CCD:\n"
        for i in range(1, 63):
            self.response += '&nbsp;<a href="#{num}">{num}</a>&nbsp;\n'.format( num=i )
        self.response += "</p>\n"

        for i in range(1, 63):
            self.response += "<h3 id=\"{num}\">CCD {num}</h3>\n".format( num=i  )

            self.response += "<table class=\"logtable\">\n"
            self.response += "<tr><th>CCD</th><th>Rank</th><th>Time</th><th>Event</th><th>Notes</th></tr>\n"
            
            for row in rows:
                if row["ccdnum"] == -1 or row["ccdnum"] == i:
                    self.response += "<tr><td>{}</td>\n".format( row["ccdnum"] )
                    self.response += "  <td>{}</td>\n".format( row["mpi_rank"] )
                    # self.response += "  <td>{}</td>\n".format( row["created_at"].isoformat( timespec="seconds") )
                    self.response += "  <td>{}</td>\n".format( row["created_at"].strftime( "%Y-%m-%d %H:%M:%S" ) )
                    self.response += "  <td>{}</td>\n".format( row["description"] )
                    if row["notes"] is not None:
                        self.response += "  <td>{}</td>\n".format( row["notes"] )
                    else:
                        self.response += "  <td></td>\n"
                    self.response += "</tr>\n"

            self.response += "</table>\n"


# ======================================================================

class ShowCandidate(HandlerBase):
    def do_the_things( self ):
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        self.set_state()
        rbtype = 1
        
        self.back_to_home()

        self.response += "<h3>Candidate: {}</h3>\n".format( self.state["candidate"] )
        self.response += ( "<p>(<a href=\"{}showcand?candidate={}\">Share Link</a>)</p>\n"
                           .format( webapfullurl, self.state[ "candidate" ] ) )

        cursor = self.db.cursor( cursor_factory = psycopg2.extras.RealDictCursor )
        query = ( "SELECT * FROM candidates WHERE id=%s" )
        cursor.execute( query, ( self.state["candidate"], ) )
        rows = cursor.fetchall()
        if len(rows) == 0:
            self.db.close()
            self.response += "<p>No candidate {}</p>\n".format( self.state["candidate"] )
            self.htmlbottom()
            return self.response
        candidate = rows[0]

        self.response += f"<h4>RA: {candidate['ra']}&nbsp;&nbsp;&nbsp;&nbsp;dec: {candidate['dec']}</h4>\n"
        
        query= ( "SELECT o.id,o.ra,o.dec,o.created_at,o.modified,o.mag,o.magerr, "
                 "e.filename,e.filter,e.mjd, "
                 "ENCODE(c.sci_jpeg, 'base64') as sci_jpeg, "
                 "ENCODE(c.ref_jpeg, 'base64') as ref_jpeg, "
                 "ENCODE(c.diff_jpeg, 'base64') as diff_jpeg "
                 "FROM objects o "
                 "INNER JOIN subtractions s ON o.subtraction_id=s.id "
                 "INNER JOIN exposures e ON s.exposure_id=e.id "
                 "LEFT JOIN cutouts c ON c.object_id=o.id "
                 "WHERE o.candidate_id=%s ORDER BY e.mjd,e.filter" )
        # sys.stderr.write( "{}\n".format( cursor.mogrify( query, ( self.state["candidate"], ) ) ) )
        cursor.execute( query, ( self.state["candidate"], ) )
        rows = cursor.fetchall()
        objecttable = pandas.DataFrame( rows )
        objecttable = objecttable.set_index( 'id' )
        oids = objecttable.index.unique(level=0).to_numpy()

        query = "SELECT object_id,rb FROM objectrbs WHERE object_id IN %s AND rbtype_id=%s"
        cursor.execute( query, ( tuple(oids), rbtype ) )
        rbtable = pandas.DataFrame( cursor.fetchall() )
        rbtable = rbtable.set_index( 'object_id' )
        cursor.close()

        objecttable = objecttable.merge( rbtable, left_index=True, right_index=True, how='left' )
        
        self.response += ( "<p><a href=\"https://www.legacysurvey.org/viewer?ra={:.5f}&dec={:.5f}&zoom=16&layer=dr8&mark={:.5f},{:.5f}\">"
                           .format( rows[0]["ra"], rows[0]["dec"], rows[0]["ra"], rows[0]["dec"] ) )
        self.response += "Desi Viewer at this position</a></p>"


        self.response += "<p><b>ROB</b>: Print information about r/b</p>"
        
        self.response += "<form method=\"post\" action=\"{}showexp\"></p>\n".format( webapfullurl )
        self.hidden_state( omit=["exposure"] )
        self.response += "<table class=\"maintable\">\n"
        self.response += "<tr><th>Exposure</th><th>New</th><th>Ref</th><th>Sub</th></tr>\n"

        for dex, row in objecttable.iterrows():
            # Make exposure a button that looks like a link
            self.response += ( "<tr>\n<td>Exposure: <span class=\"{spanclass}\">{filename}</span><br>\n"
                               .format( filename=row["filename"],
                                        spanclass="stack" if "stack" in row["filename"] else "" ) )
            self.response += "MJD: {}<br>\n".format( row["mjd"] )
            self.response += "Filter: {}<br>\n".format( row["filter"] )
            self.response += "Mag: {:.2f}±{:.2f}<br>\n".format( row["mag"], row["magerr"] )
            self.response += "R/B: {:.3f}\n</td>\n".format( row["rb"] )
            if row["sci_jpeg"] is None:
                self.response += "<td>(Sci cutout missing)</td>\n"
            else:
                self.response += ( "<td><img src=\"data:image/jpeg;base64,{}\" width=204 "
                                   "height=204 alt=\"New\"></td>\n" ).format( row["sci_jpeg"] )
            if row["ref_jpeg"] is None:
                self.response += "<td>(Ref cutout missing)</td>\n"
            else:
                self.response += ( "<td><img src=\"data:image/jpeg;base64,{}\" width=204 "
                                   "height=204 alt=\"New\"></td>\n" ).format( row["ref_jpeg"] )
            if row["diff_jpeg"] is None:
                self.response += "<td>(Diff cutout missing)</td>\n"
            else:
                self.response += ( "<td><img src=\"data:image/jpeg;base64,{}\" width=204 "
                                   "height=204 alt=\"New\"></td>\n" ).format( row["diff_jpeg"] )
            self.response += "</tr>\n"
        self.response += "</table>\n"
                
        return self.response

# ======================================================================

class RateCands(HandlerBase):
    def do_the_things( self ):
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        self.set_state()
        rbtype = 1
        self.back_to_home()

        rval = self.check_scan_user( self.webinput )
        if rval is not None:
            rval = json.loads( rval )
            self.response += "<p>Error: {}".format( rval["error"] )
            self.htmlbottom()
            return self.response

        cursor = self.db.cursor( cursor_factory = psycopg2.extras.RealDictCursor )

        # Figure out which objects we're gonna look at
        
        if self.state["vetprerated"] == "yes":
            # Get a random set of candidates ranked by others not by us
            query = ( "SELECT oid,users,exposure_id,cid,ra,dec,ccdnum,filename FROM "
                      " ( SELECT ss.object_id as oid,array_agg(ss.username) as users, "
                      "       subs.exposure_id,c.id as cid,o.ra,o.dec,subs.ccdnum,e.filename "
                      "   FROM scanscore ss "
                      "   INNER JOIN objects o ON ss.object_id=o.id "
                      "   INNER JOIN candidates c ON o.candidate_id=c.id "
                      "   INNER JOIN subtractions subs ON o.subtraction_id=subs.id "
                      "   INNER JOIN exposures e ON subs.exposure_id=e.id " )
            if self.state["vetfieldtype"] == "gal":
                query += "   WHERE gallat < 20 AND gallat > -20 "
            else:
                query += "   WHERE gallat >= 20 OR gallat <= -20 "
            query += ( "   GROUP BY ss.object_id,subs.exposure_id,subs.ccdnum,c.id,o.ra,o.dec,e.filename) x "
                       "WHERE %s != ALL(users) "
                       "ORDER BY RANDOM() LIMIT 100" )
            # sys.stderr.write( 'Candidate selection query: {}\n'
            #                   .format( cursor.mogrify(query, (self.state['user'],)) ) )
            cursor.execute( query, ( self.state['user'], ) )
            objtable = pandas.DataFrame( cursor.fetchall() )
            # sys.stderr.write( "Objects fetched from query.\n" )
            
        else:
            # First, get the list of relevant exposures
            query = "SELECT id,filename FROM exposures "
            if self.state["vetfieldtype"] == "gal":
                query += "WHERE gallat < 20 AND gallat > -20 "
            else:
                query += "WHERE gallat >= 20 OR gallat <= -20 "
            query += "ORDER BY id"
            cursor.execute( query )
            rows = cursor.fetchall()
            expids = []
            exposurenames = {}
            for row in rows:
                expids.append( row["id"] )
                exposurenames[row["id"]] = row["filename"]

            # Randoly select 100 objects from this list of exposures
            query = ( "SELECT c.id as cid,o.id as oid,o.ra,o.dec,s.ccdnum,s.exposure_id "
                      "FROM OBJECTS o "
                      "INNER JOIN candidates c ON o.candidate_id=c.id "
                      "INNER JOIN subtractions s ON o.subtraction_id=s.id "
                      "WHERE s.exposure_id IN %s "
                      "ORDER BY RANDOM() LIMIT 100" )
            # sys.stderr.write( "Bigass query started at {}\n".format( time.ctime() ) )
            cursor.execute( query, ( tuple(expids), ) )
            objtable = pandas.DataFrame( cursor.fetchall() )
            objtable["filename"] = objtable["exposure_id"].apply( lambda x: exposurenames[x] )
            # sys.stderr.write( "Objects fetched from query at {}\n".format( time.ctime() ) )
            
        if len(objtable) == 0:
            self.response += "<p>No objects found matching criteria.</p>"
            self.htmlbottom()
            return self.response
            
        objtable = objtable.set_index( "oid" )
        oids = objtable.index.unique(level=0).to_numpy()

        # Get the cutouts
        
        query = ( "SELECT o.id as oid, "
                  "ENCODE(cu.sci_jpeg, 'base64') as scijpg, "
                  "ENCODE(cu.ref_jpeg, 'base64') as refjpg, "
                  "ENCODE(cu.diff_jpeg, 'base64') as diffjpg "
                  "FROM cutouts cu INNER JOIN objects o ON cu.object_id=o.id "
                  "WHERE o.id IN %s" )
        # sys.stderr.write( "Sending query {}\n".format( cursor.mogrify( query, ( tuple(oids), ) ) ) )
        cursor.execute( query, ( tuple(oids), ) )
        cuttable = pandas.DataFrame( cursor.fetchall() )
        cuttable = cuttable.set_index( "oid" )
        objtable = objtable.merge( cuttable, left_index=True, right_index=True, how='left' )

        # Get the rb values

        query = "SELECT object_id,rb FROM objectrbs WHERE object_id IN %s AND rbtype_id=%s"
        cursor.execute( query, ( tuple( oids ), rbtype ) )
        rbtable = pandas.DataFrame( cursor.fetchall() )
        rbtable = rbtable.set_index( "object_id" )
        objtable = objtable.merge( rbtable, left_index=True, right_index=True, how='left' )
        
        # Get the goodbads
        
        query = ( "SELECT object_id,goodbad FROM scanscore WHERE username=%s AND object_id IN %s" )
        cursor.execute( query, ( self.webinput["user"], tuple(oids) ) )
        rows = cursor.fetchall()
        goodbad = {}
        for row in rows:
            goodbad[ row["object_id"] ] = row["goodbad"]

        cursor.close()
            
        self.response += '<form method=\"POST\" action=\"{}ratecands\">\n'.format( webapfullurl )
        self.hidden_state()
        self.response += "<p><button type=\"submit\">Give Me More</button></p>\n"
        self.response += "</form>"
            
        self.show_objecttable( objtable, goodbad=goodbad, user=self.webinput["user"], passwd=self.webinput["password"] )
        
        self.response += '<form method=\"POST\" action=\"{}ratecands\">\n'.format( webapfullurl )
        self.hidden_state()
        self.response += "<p><button type=\"submit\">Give Me More</button></p>\n"
        self.response += "</form>"
            
        self.htmlbottom()
        return self.response

# ======================================================================

class ShowRatedObjs(HandlerBase):
    matchcondtype = re.compile( "^vetcond (.*) type$" )
    
    def do_the_things( self ):
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        self.set_state()
        rbtype = 1
        
        warning = ""
        
        query = "SELECT COUNT(id) FROM ( SELECT object_id AS id FROM scanscore GROUP BY object_id ) AS x";
        querywhere = ""
        queryargs = []
        queryusers = []
        querygbs = []

        userquery = False
        countquery = False
        for key, val in self.state.items():
            match = self.matchcondtype.search( key )
            if match is not None:
                n = match.group(1)
                condtype = self.state[key]
                if countquery:
                    warning += "<p><b>Warning:</b> Multiple conditions w/ unanimous or majority, not supported, using only first.</p>"
                    break
                if userquery and ( ( condtype == "unanimous" ) or ( condtype == "majority" ) ):
                    warning += "<p><b>Warning:</b> Uanimous or majority conditions with user conditions, not supported, using only user.</p>"
                if condtype == "unanimous":
                    countquery = True
                    if self.state[f'vetcond {n} gb'] == "good":
                        querywhere = "WHERE goods>=%s AND bads=0"
                    else:
                        querywhere += "WHERE bads>=%s AND goods=0"
                    queryargs.append( self.state[f'vetcond {n} numvets'] )
                elif condtype == "majority":
                    countquery = True
                    if self.state[f'vetcond {n} gb'] == "good":
                        querywhere = "WHERE goods-bads>=%s"
                    else:
                        querywhere = "WHERE bads-goods>=%s"
                    querywhere += " AND bads+goods>=%s"
                    queryargs.append( self.state[f'vetcond {n} mindiff'] )
                    queryargs.append( self.state[f'vetcond {n} numvets'] )
                else:
                    userquery = True
                    queryusers.append( self.state[f'vetcond {n} user'] )
                    querygbs.append( self.state[f'vetcond {n} gb'] )                    
            
        cursor = self.db.cursor( cursor_factory = psycopg2.extras.RealDictCursor )

        # Get relevant object ids

        if countquery:
            query = ( f"SELECT id FROM ( SELECT object_id AS id "
                                             f",COUNT(goodbad) FILTER (WHERE goodbad='good') AS goods"
                                             f",COUNT(goodbad) FILTER (WHERE goodbad='bad') AS bads "
                                      f"FROM scanscore GROUP BY object_id ) AS x {querywhere}" )
            # sys.stderr.write( f'Sending query: {cursor.mogrify( query, tuple( queryargs ) )}\n' )
            cursor.execute( query, tuple( queryargs ) )
            rows = cursor.fetchall()
            ids = tuple( [ row['id'] for row in rows ] )
        elif userquery:
            query = ( f"SELECT object_id,username,goodbad FROM scanscore WHERE username IN %s" )
            # sys.stderr.write( f'Sending user query {cursor.mogrify( query, ( tuple( queryusers ), ) )}\n' )
            cursor.execute( query, ( tuple( queryusers ), ) )
            rows = cursor.fetchall()
            fulldf = pandas.DataFrame( rows ).set_index( ['object_id','username'] ).unstack()
            sys.stderr.write( f'fulldf:\n{fulldf}\n' )
            sys.stderr.write( f'indices: {fulldf.index.values}; columns: {fulldf.columns.values}\n' )
            for user, gb in zip(queryusers, querygbs):
                sys.stderr.write( f'Looking for username={user} and goodbad={gb}...\n' )
                fulldf = fulldf[ fulldf[ 'goodbad' ][ user ] == gb ]
                sys.stderr.write( f'fulldf:\n{fulldf}\n' )
            ids = tuple( fulldf.index.values )
        else:
            query = "SELECT DISTINCT object_id FROM scanscore"
            cursor.execute( query )
            rows = cursor.fetchall()
            ids = tuple( [ row['object_id'] for row in rows ] )

        numobj = len(ids)
            
        if numobj == 0:
            self.response += "<h2>No objects!</h2>\n"
            self.htmlbottom
            return self.response

        # Get the ranked things

        orderclause = ""
        if "vetted order" in self.state:
            if self.state["vetted order"] == "standard":
                orderclause = "ORDER BY e.mjd,subs.ccdnum,oid"
            elif self.state["vetted order"] == "random":
                orderclause = "ORDER BY RANDOM()"
                warning += "<p><b>Note</b>: Random ordering, each \"Next\" or \"Prev\" really just gets another random set.</p>\n"
            else:
                ordercaluse = "ORDER BY e.mjd,subs.ccdnum,oid"
                warning += f'<p><b>Warning</b>: Unknown ordering {self.state["vetted order"]}, ordering by mjd, ccdnum, object_id</p>\n'
        
        query = ( "SELECT oid,users,goodbads,exposure_id,cid,ra,dec,ccdnum,filename,scijpg,refjpg,diffjpg FROM "
                  "  ( SELECT ss.object_id AS oid,subs.exposure_id,o.candidate_id AS cid,o.ra,o.dec,"
                  "           subs.ccdnum,e.filename,"
                  "           array_agg(ss.username) as users,"
                  "           array_agg(ss.goodbad) as goodbads,"
                  "           ENCODE(cu.sci_jpeg, 'base64') AS scijpg, "
                  "           ENCODE(cu.ref_jpeg, 'base64') AS refjpg, "
                  "           ENCODE(cu.diff_jpeg, 'base64') AS diffjpg "
                  "    FROM scanscore ss "
                  "    INNER JOIN objects o ON ss.object_id=o.id "
                  "    INNER JOIN subtractions subs ON o.subtraction_id=subs.id "
                  "    INNER JOIN exposures e ON subs.exposure_id=e.id "
                  "    LEFT JOIN cutouts cu ON cu.object_id=o.id "
                  "    WHERE ss.object_id IN %(idlist)s "
                  "    GROUP BY ss.object_id,e.id,e.filename,o.id,o.candidate_id,o.ra,o.dec,"
                  "             subs.exposure_id,subs.ccdnum,"
                  "             cu.sci_jpeg,cu.ref_jpeg,cu.diff_jpeg "
                 f"    {orderclause} ) x "
                  "LIMIT %(numperpage)s OFFSET %(offset)s" )
        cursor.execute(query, { 'idlist': ids,
                                'numperpage': self.state["numperpage"],
                                'offset': self.state["offset"] } )
        objtable = pandas.DataFrame( cursor.fetchall() )
        objtable = objtable.set_index( "oid" )
        oids = objtable.index.unique(level=0).to_numpy()
        
        # Get the rbs

        query = "SELECT object_id,rb FROM objectrbs WHERE object_id IN %s AND rbtype_id=%s"
        cursor.execute( query, ( tuple(oids), rbtype ) )
        rbtable = pandas.DataFrame( cursor.fetchall() )
        rbtable = rbtable.set_index( 'object_id' )
        objtable = objtable.merge( rbtable, left_index=True, right_index=True, how='left' )
        
        # Wrangle the vetting info into what HandlerBase.objecttable expects

        vetdict = {}
        for objid, row in objtable.iterrows():
            vetdict[objid] = {}
            for username, score in zip( row["users"], row["goodbads"] ):
                vetdict[objid][username] = score

        # Show

        self.response += warning
        
        self.prevnext( self.state, numobj, "showrated" )
        self.show_objecttable( objtable, vetdict=vetdict )
        self.prevnext( self.state, numobj, "showrated" )
                
        self.htmlbottom
        return self.response

# ======================================================================

class SetGoodBad(HandlerBase):
    def do_the_things( self ):
        web.header('Content-Type', 'application/json')

        data = json.loads( web.data().decode(encoding='utf-8') )
        rval = self.check_scan_user( data )
        if rval is not None:
            return rval

        query = ( "SELECT user,password FROM scanusers WHERE username=%s" )
        cursor = self.db.cursor( cursor_factory = psycopg2.extras.RealDictCursor )
        cursor.execute( query, ( data["user"], ) )
        rows = cursor.fetchall()
        if len(rows) == 0:
            rval = { "error": "No such user \"{}\"".format( data["user"] ) }
            return json.dumps(rval)
        if len(rows) > 1:
            rval =  { "error": "User \"{}\" multiply defined!".format( user ) }
        if rows[0]["password"] != data["password"]:
            rval = { "error": "Incorrect password for {}".format(user) }

        cursor = self.db.cursor()
        for status, strobjid in zip(data["status"], data["obj"]):
            objid = int(strobjid)
            if status == "good": goodbad = True
            elif status == "bad": goodbad = False
            else:
                rval = { "error": "{} is neither good nor bad".format( data["status"] ) }
                return json.dumps(rval)

            pkey = "{}_{}".format( objid, data["user"] )
            query = ( "INSERT INTO scanscore(id,object_id,username,goodbad) VALUES(%s,%s,%s,%s) "
                      "ON CONFLICT ON CONSTRAINT scanscore_pkey DO UPDATE SET goodbad=%s" )
            cursor.execute( query, ( pkey, objid, data["user"], status, status ) )
        self.db.commit()
        cursor.close()

        # This is hopefully reudndant, but read the status back
        cursor = self.db.cursor(  cursor_factory = psycopg2.extras.RealDictCursor )
        query = ( "SELECT object_id,username,goodbad FROM scanscore "
                  "WHERE username=%(user)s AND object_id IN %(objids)s" )
        cursor.execute( query, { 'user': data["user"], 'objids': tuple(data["obj"]) } )
        rows = cursor.fetchall()
        if len(rows) != len(data["obj"]):
            rval = { "error": f"Read back {len(rows)} statuses when expected {len(data['obj'])}; "
                     f"the database might be OK, but this page is mucked up now.  Go back." }
            return json.dumps(rval)
        rval = { "objid": [ row['object_id'] for row in rows ],
                 "status": [ row['goodbad'] for row in rows ] }
        cursor.close()
        return json.dumps(rval)
        

# ======================================================================
    
class DumpData(HandlerBase):
    def do_the_things( self ):
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()

        data = web.input()
        self.response += "<ul>\n"
        for i in data:
            self.response += "<li>{} = \"{}\"</li>\n".format( i, data[i] )
        self.response += "</ul>\n"
        self.htmlbottom()
        return self.response

# ======================================================================

urls = (
    '/', "FrontPage",
    "/findcands", "FindCandidates",
    "/listexp", "ListExposures",
    "/showexp", "ShowExposure" ,
    "/showcand", "ShowCandidate" ,
    "/ratecands", "RateCands",
    "/setgoodbad", "SetGoodBad" ,
    "/showrated", "ShowRatedObjs",
    "/dumpdata", "DumpData" ,
    )

web.config.session_parameters["samesite"] = "lax"
app = web.application(urls, globals())
application = app.wsgifunc()

if __name__ == "__main__":
    app.run()
