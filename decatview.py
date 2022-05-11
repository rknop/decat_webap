#!/usr/bin/python
# -*- coding: utf-8 -*-

# https://www.legacysurvey.org/viewer?ra=30.1530962&dec=-5.0236864&zoom=16&layer=dr8

import sys
import math
import os
import pathlib
import re
import time
import json
import dateutil.parser
import pytz
import web
import numpy
import pandas
from web import form

scriptdir = str( pathlib.Path( __file__ ).parent )
if scriptdir not in sys.path:
    sys.path.insert(0, scriptdir )

from webapconfig import webapfullurl, webapdir, webapdirurl, DBdata, DBname
from util import dtohms, dtodms, radectolb, mjd, dateofmjd, parsedms, sanitizeHTML
from decatdb import DB, ObjectTable, ExposureTable, RBTypeTable, Exposure, Candidate
from decatdb import check_scan_user, set_user_object_scanscore

session = None

# ======================================================================

def secure_session():
    global session, app
    sessinit = {
        "state": {
            "date0": "",
            "date1": "",
            "minb": 0,
            "maxb": 90,
            "stackorindiv": "all",
            "allpropornot": "inc",
            "whichprops": [ '2022A-724693' ],
            "showvetting": False,
            "offset": 0,
            "numperpage": 100,
            "exposure": "",
            "orderby": "real/bogus",
            "ccds": "",
            "showrb": False,
            "whattodo": "Show Objects",        # ROB THINK ABOUT THIS
            "mindet": 5,
            "rbtype": 2,
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
            "candidate": "",
            "user": None,
            "password": None,
            "vetfieldtype": None,
            "vetprerated": None,
            "vetted order": "random",
        }
    }
    session = web.session.Session( app, web.session.DiskStore( "/sessions" ), initializer=sessinit )

def init_session_state():
    global session
    if session is not None:
        session.kill()
        session = None
    secure_session()
    
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
        # self.db = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'"
        #                            .format( database, user, host, password ))
        # sys.stderr.write("db.status is {}\n".format(self.db.status))

    def finalize(self):
        # self.db.close()
        pass
        
    def htmltop(self):
        self.response = "<!DOCTYPE html>\n"
        self.response += "<html lang=\"en\">\n<head>\n<meta charset=\"UTF-8\">\n"
        self.response += "<link rel=\"stylesheet\" href=\"{}decat.css\">\n".format( webapdirurl )
        self.response += "<script src=\"{}decatview.js\" type=\"module\"></script>\n".format( webapdirurl )
        self.response += "<title>DECaT LBL Pipeline Candidate Viewer</title>\n"
        self.response += "</head>\n<body>\n"

    def htmlbottom(self):
        self.response += "\n</body>\n</html>\n"

    # def hidden_state( self, omit=[] ):
    #     global session
    #     for key, val in session.state.items():
    #         if not key in omit:
    #             self.response += form.Input( name=key, id=f"{key}{self.idordinal}"
    #                                          type="hidden", value=val ).render() + "\n"
    #     self.idordinal += 1
                
    def back_to_home( self ):
        self.response += '<form method=\"POST\" action=\"{}\">\n'.format( webapfullurl )
        self.response += "<p><button class=\"link\" type=\"submit\">Back to Home</button></p>\n"
        self.response += "</form>\n"
                
    def set_state( self ):
        global session
        # ROB!  Remember to give defaults for the array quantities
        self.webinput = web.input( whichprops=session.state['whichprops'] )
        for stateval in session.state:
            if stateval in self.webinput:
                session.state[stateval] = self.webinput[stateval]
        # ROB!  This is terrible!  You really need a totally different
        #  infrastructure
        for key, value in self.webinput.items():
            if key[0:7] == "vetcond":
                session.state[key] = value
        # ARGH.  web.py has, I think, irritating handling of list parameters.
        # I'd call it a bug.
        if ( len(session.state["whichprops"]) == 1 ) and ( session.state["whichprops"][0][0:2] == "['" or
                                                           session.state["whichprops"][0][0:2] == '["' ):
            session.state["whichprops"][0] = session.state["whichprops"][0][2:-2]
                
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
                selsel = 'checked="checked"'
                self.response += "<td>\n"
                self.response += ( f'<input type="radio" value="good" name="{objid}statusset" '
                                   f'id="{objid}statusgood" data-user="{user}" data-password="{passwd}" '
                                   f'{selsel if isgood else ""} '
                                   f'data-objid="{objid}" data-goodbad="good">\n' )
                self.response += "<label for=\"{obj}statussgood\">Good</label><br>\n".format( obj=objid )
                self.response += ( f'<input type="radio" value="bad" name="{objid}statusset" '
                                   f'id="{objid}statusbad" data-user="{user}" data-password="{passwd}" '
                                   f'{selsel if isbad else ""} '
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
        global session
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
        self.response += form.Input( name="date0", value=session.state["date0"], type="text" ).render()
        self.response += "\n&nbsp;&nbsp;to&nbsp;&nbsp;\n"
        self.response += form.Input( name="date1", value=session.state["date1"], type="text" ).render() + "</p>\n"

        self.response += "<p>Galactic latitude between ± "
        self.response += form.Input( name="minb", type="text", size="4", value=session.state["minb"] ).render()
        self.response += "°&nbsp;and&nbsp;"
        self.response += form.Input( name="maxb", type="text", size="4", value=session.state["maxb"] ).render()
        self.response += "°</p>\n"

        self.response += "<p><br>Use real/bogus type: "
        rbtypes = RBTypeTable.get()
        args = []
        for dex, row in rbtypes.df.iterrows():
            args.append( ( dex, f'{dex} — {row.description}' ) )
        self.response += form.Dropdown( name="rbtype", id="rbtype", args=args,
                                        value=session.state["rbtype"] ).render()
        
        self.response += "<br><br>" + form.Dropdown( name="stackorindiv", id="stackorindiv",
                                                     args=[ ("all", "Show both stacks and individual images"),
                                                            ("indiv", "Show only individual images, not stacks"),
                                                            ("stack", "Show only stacks, not individual images") ],
                                                 value=session.state["stackorindiv"] ).render()
        self.response += "<br><br>" + form.Dropdown( name="allpropornot", id="allpropornot",
                                                args=[ ("inc","Include all proposal IDs"),
                                                       ("some","Only proposal IDs:") ],
                                                value=session.state["allpropornot"] ).render()
        self.response += "<div id=\"whichprops_div\" style=\"display: none\">\n"
        for propid,displayprop in [ ('2021A-0113', '2021A-0113: Graham DDF Spring 2021'),
                                    ('2021B-0149', '2021B-0149: Graham DDF Fall 2021'),
                                    ('2022A-724693', '2022A-724693: Graham DDF Spring 2022'),
                                    ('2022A-388025', '2022A-388025: DESI-RT Spring 2022'),
                                    ('2021A-0275', '2021A-0275: Rest YSE'),
                                    ('2020B-0053', '2020B-0053: Brount DEBASS') ]:
            self.response += "\n<br>" + form.Checkbox( id=f"whichprops_{propid}", name="whichprops", value=propid,
                                                       checked=(propid in session.state["whichprops"]) ).render()
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
                                     min="1", max="1000", step="1", value=session.state["mindet"] ).render()
        self.response += "</td></tr>\n<tr><td>Min # rb≥"
        self.response += form.Input( name="rbcut", type="number", min="0", max="1",
                                     step="any", size="4", value=session.state["rbcut"] ).render()
        self.response += "</td><td>"
        self.response += form.Input( name="minrb", type="number", min="0", max="1000",
                                     step="1", value=session.state["minrb"] ).render()
        self.response += "</td></tr>\n<tr><td>Min diff. days detected:</td><td>"
        self.response += form.Input( name="numdays", type="number", min="0", max="1000",
                                     step="1", value=session.state["numdays"] ).render()
        self.response += "</td></tr>\n<tr><td>Min (brightest) mag ≤</td><td>"
        self.response += form.Input( name="minmag", type="number", min="15", max="30",
                                     step="0.1", value=session.state["minmag"] ).render()
        self.response += "</td></tr>\n<tr><td>Max (dimmest) mag ≥</td><td>"
        self.response += form.Input( name="maxmag", type="number", min="15", max="30",
                                     step="0.1", value=session.state["maxmag"] ).render()
        self.response += "</td></tr>\n<tr><td>Limit by RA/Dec?</td><td class=\"left\">"
        self.response += form.Input( name="limitbyradec", type="checkbox",
                                     value=session.state["limitbyradec"] ).render()
        self.response += "</td></tr>\n<tr><td>RA ("
        self.response += form.Dropdown( name="raformat", value=session.state["raformat"],
                                        args=[ ("hms","h:m:s"), ("decimalhrs", "decimal hrs"),
                                               ("decimaldeg", "decimal deg") ] ).render()
        self.response += "):</td><td>"
        self.response += form.Input( name="ra", type="text", value=session.state["ra"] ).render()
        self.response += "</td></tr>\n<tr><td>Dec ("
        self.response += form.Dropdown( name="decformat", value=session.state["decformat"],
                                        args=[ ("dms","d:m:s"), ("decimaldeg", "decimal °") ] ).render()
        self.response += "):</td><td>"
        self.response += form.Input( name="dec", type="text", value=session.state["dec"] ).render()
        self.response += "</td></tr>\n<tr><td>Search radius in "
        self.response += form.Dropdown( name="searcunit", value=session.state["searchunit"],
                                        args=[ ("arcsec", "″"), ("arcmin", "′"), ("degree", "°") ] ).render()
        self.response += "</td><td>: "
        self.response += form.Input( name="searchradius", type="text", value=session.state["searchradius"] ).render()
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
                                        value="gal" ).render()
        self.response += "</td></tr>\n"
        self.response += "<tr><td>Objects vetted by others?</td><td class=\"left\">"
        self.response += form.Dropdown( name="vetprerated", args=[ ("yes", "Yes"), ("no", "No") ],
                                        value="no" ).render()
        self.response += "</td></tr>\n<tr><td>"
        self.response += form.Input( name="submitvet", type="submit", value="Show Objects" ).render()
        self.response += "</td><td>(This may take a little while, be patient.)</td></tr></table>\n"
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
        global session
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        self.response += "<h2>Broken</h2>\n"
        self.response += "<p>Finding candidates is currently broken, until Rob fixes its rb handling.</p>\n"
        return self.response
        
        self.set_state()
        
        minnum = int( session.state["mindet"] )
        rbcut = float( session.state["rbcut"] )
        minrbnum = int( session.state["minrb"] )
        mindays = int( session.state["numdays"] )
        minmag = float( session.state["minmag"] )
        maxmag = float( session.state["maxmag"] )
        useradec = session.state["limitbyradec"]
        raformat = session.state["raformat"]
        rastr = session.state["ra"]
        decformat = session.state["decformat"]
        decstr = session.state["dec"]
        radformat = session.state["searchunit"]
        try:
            radius = float( session.state["searchradius"] )
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
        global session
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        self.set_state()
        
        if len(session.state["date0"].strip()) == 0:
            session.state["date0"] = "1970-01-01"
        if len(session.state["date1"].strip()) == 0:
            session.state["date1"] = "2999-12-31"
        date0 = session.state["date0"].strip()
        date1 = session.state["date1"].strip()
        whichprops = session.state["whichprops"]
        minb = math.fabs( float( session.state["minb"] ) )
        maxb = math.fabs( float( session.state["maxb"] ) )
            
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

        if session.state['allpropornot'] == "some":
            self.response += ( f"<p style=\"font-weight: bold\">Including proposal"
                               f"{'' if len(session.state['whichprops'])==1 else 's'} "
                               f" {','.join(session.state['whichprops'])}</p>\n" )
            

        if session.state["allpropornot"] == "some":
            propids = session.state['whichprops']
        else:
            propids = None
        if session.state["stackorindiv"] == "indiv":
            includestack=False
        else:
            includestack = True
        if session.state["stackorindiv"] == "stack":
            onlystack=True
        else:
            onlystack=False
            
        exptab = ExposureTable.load( t0, t1, propids=propids, includestack=includestack, onlystack=onlystack,
                                     rbtype=session.state["rbtype"], mingallat=minb, maxgallat=maxb )
        if len(exptab) == 0:
            self.response += "<p>No exposures!</p>\n"
            self.htmlbottom()
            return self.response
        exptab.load_nchips_checkpoint( checkpoint=27, column="nfinished" )
        exptab.load_nerrors()
        
        self.response += "<p>Number of objects per page: <input type=\"number\" name=\"numperpage\" value=100><br>\n"
        self.response += "Only include ccd numbers (comma-sep): "
        self.response += form.Input( name="ccds", type="text", value=session.state["ccds"] ).render()

        self.response += "\n<br>\n  Use real/bogus type: "
        rbtypes = RBTypeTable.get()
        args = []
        for dex, row in rbtypes.df.iterrows():
            args.append( ( dex, f'{dex} — {row.description}' ) )
        self.response += form.Dropdown( name="rbtype", id="rbtype", args=args,
                                        value=session.state["rbtype"] ).render()

        self.response += "\n<br>\n  Order by:\n"
        self.response += form.Radio( name="orderby", value=session.state["orderby"],
                                     args=[ ("real/bogus", "Real/Bogus"), ("objnum", "ObjectNum.") ] ).render()
        self.response += "\n<br>\n"
        self.response += form.Checkbox( id="showvetting", name="showvetting",
                                        value=session.state["showvetting"] ).render()
        self.response += " show manual vetting?</p>\n<p>"

        self.response += f"{len(exptab)} exposures<br>\n"
        self.response += "<table class=\"exposurelist\">\n"
        self.response += ( "<tr><th>Exposure</th><th>Filter</th><th>propid</th><th>t_exp</th>"
                           "<th>ra</th><th>dec</th><th>l</th><th>b</th>"
                           "<th>#Subs</th><th>#Done</th><th>N. Objects</th>"
                           "<th>rb≥cut</th></tr>\n" )

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
        global session
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        self.set_state()

        # sys.stderr.write( "ShowExposure webinput = {}, state = {}\n".format( json.dumps( self.webinput ),
        #                                                                      json.dumps( session.state ) ) )
                
        if session.state["whattodo"] == "Show Objects":
            self.show_objects()
        elif session.state["whattodo"] == "Show Log":
            self.show_log()
        else:
            self.response += "<p>Error, unknown whattodo \"{}\"</p>\n".format( session.state["whattodo"] )
            
        self.htmlbottom()
        return self.response

    # ========================================
    
    def show_objects( self ):
        global session
        offset = int( session.state["offset"] )
        if offset < 0: offset = 0
        numperpage = int( session.state["numperpage"] )
        filename = session.state["exposure"]
        orderby = session.state["orderby"]
        showvetting = session.state["showvetting"]

        try:
            ccds = session.state["ccds"].strip()
            if len(ccds) > 0:
                ccdarr = [ int(x) for x in session.state["ccds"].split(",") ]
                if len(ccdarr) == 0: ccdarr = None
            else:
                ccdarr = None
        except Exception as e:
            self.response += f"<p><b>ERROR</b> parsing ccdlist {sanitizeHTML(ccds)}</p>\n"
            return
            
        objtab = ObjectTable.load_for_exposure( exposure_name=filename, ccdnums=ccdarr,
                                                rbtype=session.state["rbtype"], loadjpg=True )
        if showvetting:
            objtab.loadvetting()
        
        if orderby == "real/bogus":
            objtab.sortrb()
        elif orderby == "objnum":
            objtab.sortoid()

        numobjs = len(objtab)
        objtab.subset( offset, offset+numperpage )
        
        self.prevnext( session.state, numobjs )

        self.response += "<h3>Exposure: {}</h3>\n".format( filename )
        self.response += "<h4>Candidates starting at offset {} out of {}</h4>\n".format( offset, numobjs )

        rbtypes = RBTypeTable.get()
        if int(session.state["rbtype"]) not in rbtypes.df.index.values:
            self.response += f"<p><b>Unknown r/b type {session.state['rbtype']}</b></p>\n"
        else:
            rbtype = rbtypes.df.loc[int(session.state['rbtype'])]
            self.response += f"<p>R/B is type {session.state['rbtype']} — {rbtype['description']} "
            self.response += f"(alert cutoff: {rbtype['rbcut']})</p>\n"
        
        self.show_objecttable( objtab.df, showvetting=showvetting )
        
        self.prevnext( session.state, numobjs )

    # ========================================
        
    def show_log( self ):
        global session
        exposure = Exposure( session.state["exposure"] )
        log = exposure.get_log()

        self.response += f"<h2>{exposure.filename}</h2>\n"

        if len(log) == 0:
            self.response += "<p>No log information.</p>\n"
            return

        self.response += f'<p>Ran on {log["running_node"].unique().tolist()}</p>\n'

        hassub = [False] * exposure.nccds
        hasobj = [False] * exposure.nccds
        haserr = [False] * exposure.nccds
        hasinf = [False] * exposure.nccds

        allerr = ( ( log['ccdnum'] == -1 ) & ( log['event_id'] == 999 ) ).sum() > 0
        allinf = ( ( log['ccdnum'] == -1 ) & ( log['event_id'] == 998 ) ).sum() > 0
        if allerr:
            haserr = [True] * exposure.nccds
        if allinf:
            hasinf = [True] * exposure.nccds
        for ccd in exposure.ccdlist:
            hassub[ccd] = ( ( log['ccdnum'] == ccd ) & ( log['event_id'] == 20 ) ).sum() > 0
            hasobj[ccd] = ( ( log['ccdnum'] == ccd ) & ( log['event_id'] == 27 ) ).sum() > 0
            if ( ( log['ccdnum'] == ccd ) & ( log['event_id'] == 999 ) ).sum() > 0:
                haserr[ccd] = True
            if ( ( log['ccdnum'] == ccd ) & ( log['event_id'] == 998 ) ).sum() > 0:
                hasinf[ccd] = True
                
        self.response += "<p>CCDs without subtraction: "
        for i in exposure.ccdlist:
            if not hassub[i]:
                self.response += '&nbsp;<a href="#{num}">{num}</a>&nbsp;\n'.format( num=i )
        self.response += "</p>"

        self.response += "<p>CCDs without object detection: "
        for i in exposure.ccdlist:
            if not hasobj[i]:
                self.response += '&nbsp;<a href="#{num}">{num}</a>&nbsp;\n'.format( num=i )
        self.response += "</p>"

        self.response += "<p>CCDs with errors logged: "
        for i in exposure.ccdlist:
            if haserr[i]:
                self.response += '&nbsp;<a href="#{num}">{num}</a>&nbsp;\n'.format( num=i )
        self.response += "</p>"
        
        self.response += "<p>CCDs with info logged: "
        for i in exposure.ccdlist:
            if hasinf[i]:
                self.response += '&nbsp;<a href="#{num}">{num}</a>&nbsp;\n'.format( num=i )
        self.response += "</p>"
        
        
        self.response += "<p>Jump to CCD:\n"
        for i in exposure.ccdlist:
            self.response += '&nbsp;<a href="#{num}">{num}</a>&nbsp;\n'.format( num=i )
        self.response += "</p>\n"

        for i in exposure.ccdlist:
            self.response += "<h3 id=\"{num}\">CCD {num}</h3>\n".format( num=i  )

            self.response += "<table class=\"logtable\">\n"
            self.response += "<tr><th>CCD</th><th>Rank</th><th>Time</th><th>Event</th><th>Notes</th></tr>\n"

            sublog = log[ ( log["ccdnum"] == -1 ) | ( log["ccdnum"] == i ) ]
            for dex, row in sublog.iterrows():
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
        global session
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        self.set_state()
        
        self.back_to_home()

        self.response += "<h3>Candidate: {}</h3>\n".format( session.state["candidate"] )
        self.response += ( "<p>(<a href=\"{}showcand?candidate={}\">Share Link</a>)</p>\n"
                           .format( webapfullurl, session.state[ "candidate" ] ) )

        candidate = Candidate.load( session.state["candidate"] )
        self.response += f"<h4>RA: {candidate.ra}&nbsp;&nbsp;&nbsp;&nbsp;dec: {candidate.dec}</h4>\n"

        objecttable = ObjectTable.load_for_candidate( candidate.id, rbtype=session.state["rbtype"], loadjpg=True )

        self.response += ( f"<p><a href=\"https://www.legacysurvey.org/viewer"
                           f"?ra={candidate.ra:.5f}&dec={candidate.dec:.5f}"
                           f"&zoom=16&layer=dr8&mark={candidate.ra:.5f},{candidate.dec:.5f}\">" )
        self.response += "Desi Viewer at this position</a></p>"

        self.response += "<p><b>ROB</b>: Print information about r/b</p>"
        
        self.response += "<form method=\"post\" action=\"{}showexp\"></p>\n".format( webapfullurl )
        self.response += "<table class=\"maintable\">\n"
        self.response += "<tr><th>Exposure</th><th>New</th><th>Ref</th><th>Sub</th></tr>\n"

        for dex, row in objecttable.df.iterrows():
            # Make exposure a button that looks like a link
            self.response += ( "<tr>\n<td>Exposure: <span class=\"{spanclass}\">{filename}</span><br>\n"
                               .format( filename=row["exposurename"],
                                        spanclass="stack" if "stack" in row["exposurename"] else "" ) )
            self.response += f"MJD: {row['mjd']}<br>\n"
            self.response += f"Filter: {row['filter']}<br>\n"
            self.response += f"Mag: {row['mag']:.2f}±{row['magerr']:.2f}<br>\n"
            self.response += f"R/B: {row['rb']:.3f}\n</td>\n"
            if row["scijpg"] is None:
                self.response += "<td>(Sci cutout missing)</td>\n"
            else:
                self.response += ( "<td><img src=\"data:image/jpeg;base64,{}\" width=204 "
                                   "height=204 alt=\"New\"></td>\n" ).format( row["scijpg"] )
            if row["refjpg"] is None:
                self.response += "<td>(Ref cutout missing)</td>\n"
            else:
                self.response += ( "<td><img src=\"data:image/jpeg;base64,{}\" width=204 "
                                   "height=204 alt=\"New\"></td>\n" ).format( row["refjpg"] )
            if row["diffjpg"] is None:
                self.response += "<td>(Diff cutout missing)</td>\n"
            else:
                self.response += ( "<td><img src=\"data:image/jpeg;base64,{}\" width=204 "
                                   "height=204 alt=\"New\"></td>\n" ).format( row["diffjpg"] )
            self.response += "</tr>\n"
        self.response += "</table>\n"
                
        return self.response

# ======================================================================

class RateCands(HandlerBase):
    def do_the_things( self ):
        global session
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        self.set_state()
        self.back_to_home()

        rval = check_scan_user( self.webinput )
        if rval is not None:
            rval = json.loads( rval )
            self.response += "<p>Error: {}".format( rval["error"] )
            self.htmlbottom()
            return self.response

        sys.stderr.write( 'In RateCands, about to get objects\n' )

        # Figure out which objects we're gonna look at

        mingallat = 20 if session.state["vetfieldtype"] == "exgal" else None
        maxgallat = 20 if session.state["vetfieldtype"] == "gal" else None
        if session.state["vetprerated"] == "yes":
            objtab = ObjectTable.load_by_rated( loadjpg=True, random=True, mingallat=mingallat, maxgallat=maxgallat )
        else:
            objtab = ObjectTable.load_random_set( loadjpg=True, mingallat=mingallat, maxgallat=maxgallat )
        sys.stderr.write( 'In ratecands, about to get goodbad for user\n' )
        goodbad = objtab.get_goodbad_for_user( self.webinput["user"] )
            
        self.response += '<form method=\"POST\" action=\"{}ratecands\">\n'.format( webapfullurl )
        self.response += f"<input type=\"hidden\" name=\"user\" value=\"{self.webinput['user']}\">"
        self.response += f"<input type=\"hidden\" name=\"password\" value=\"{self.webinput['password']}\">"
        self.response += "<p><button type=\"submit\">Give Me More</button></p>\n"
        self.response += "</form>"

        sys.stderr.write( 'In RateCands, about to call show_objecttable\n' )
        
        self.show_objecttable( objtab.df, goodbad=goodbad,
                               user=self.webinput["user"], passwd=self.webinput["password"] )
        
        self.response += '<form method=\"POST\" action=\"{}ratecands\">\n'.format( webapfullurl )
        self.response += f"<input type=\"hidden\" name=\"user\" value=\"{self.webinput['user']}\">"
        self.response += f"<input type=\"hidden\" name=\"password\" value=\"{self.webinput['password']}\">"
        self.response += "<p><button type=\"submit\">Give Me More</button></p>\n"
        self.response += "</form>"
            
        self.htmlbottom()
        return self.response

# ======================================================================

class ShowRatedObjs(HandlerBase):
    matchcondtype = re.compile( "^vetcond (.*) type$" )
    
    def do_the_things( self ):
        global session
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        self.set_state()
        
        # Parsing the conditions is a bit of a mess.  I'm not really
        # happy with how I maintain state.  Right now, I stuff a million
        # variables in the session.  But, for stuff that's handled
        # client side (like the conditions) that state is lost.  But, it
        # may still be here!  So, what's here will not match what the
        # user is expecting.  I need to do state better.  (Previously,
        # I'd just had a million hidden inputs client side and reread
        # the state all the time up here.  That was cumbersome.
        # Probably the right thing to do is to completely refactor it in
        # a MCV sort of way so that all the UI rendering and UI-like
        # state is stored client side, and server side just serves up
        # data.)  Right now, I ignore all the vetcond stuff in the session
        # and just look at what the user sent me.
        conditions = []
        for key, val in self.webinput.items():
            match = self.matchcondtype.search( key )
            if match is not None:
                n = match.group(1)
                condtype = session.state[key]
                cond = { 'type': condtype }
                if ( condtype == "unanimous" ) or ( condtype == "majority" ):
                    cond['goodbad'] = session.state[f'vetcond {n} gb']
                    cond['minvets'] = session.state[f'vetcond {n} numvets']
                    cond['mindiff'] = session.state[f'vetcond {n} mindiff']
                elif condtype == "user":
                    cond['user'] = session.state[f'vetcond {n} user']
                    cond['userrate'] = session.state[f'vetcond {n} usergb']
                conditions.append( cond )

        orderrandom = session.state["vetted order"] == "random"
        objtab = ObjectTable.load_by_rated( conditions=conditions, rbtype=session.state["rbtype"],
                                            loadjpg=True, random=orderrandom )
        numobj = len(objtab)
            
        if numobj == 0:
            self.response += "<h2>No objects!</h2>\n"
            self.htmlbottom
            return self.response

        self.prevnext( session.state, numobj, "showrated" )
        self.show_objecttable( objtab.df, showvetting=True )
        self.prevnext( session.state, numobj, "showrated" )
                
        self.htmlbottom
        return self.response

# ======================================================================

class SetGoodBad(HandlerBase):
    def do_the_things( self ):
        global session
        web.header('Content-Type', 'application/json')

        data = json.loads( web.data().decode(encoding='utf-8') )
        rval = check_scan_user( data )
        if rval is not None:
            return rval

        return set_user_object_scanscore( data["user"], data["obj"], data["status"] )

# ======================================================================
    
class ReInit(HandlerBase):
    def do_the_things( self ):
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        init_session_state()
        self.back_to_home()
        self.response += "<p>Session cleared.</p>"
        self.htmlbottom()
        return self.response
    
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
    "/reinit", "ReInit" ,
    )

web.config.session_parameters["samesite"] = "lax"
app = web.application(urls, globals())
application = app.wsgifunc()
secure_session()

if __name__ == "__main__":
    app.run()
