#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import math
import os
import re
import json
import psycopg2
import psycopg2.extras
import base64
from io import BytesIO
import uuid
import datetime
import dateutil.parser
import pytz
import gzip
import web
import numpy
from web import form
import PIL
import PIL.Image

# I want to find a way not to have to
#   hardcode a path here.
# web.ctx has the filename, but doesn't
#   exist until an application's been called.
if not "/var/www/raknop/decat/view" in sys.path:
    sys.path.insert(0, "/var/www/raknop/decat/view")

from webapconfig import webapfullurl, webapdir, webapdirurl, DBdata
from util import dtohms, dtodms, radectolb, mjd, dateofmjd

# sys.stderr.write("About to import astropy\n")
# s.environ["XDG_CONFIG_HOME"] = "/var/www/astropy"
# os.environ["XDG_CACHE_HOME"] = "/var/www/astropy"
# from astropy.coordinates import SkyCoord
# sys.stderr.write("Imported astropy\n")
# import fitsio

# rightpassword = "Lambdaneq0"

# ======================================================================

class HandlerBase(object):
    def __init__(self):
        self.response = ""
        
        with open(DBdata) as ifp:
            user = ifp.readline().strip()
            password = ifp.readline().strip()
            host = ifp.readline().strip()
            port = ifp.readline().strip()
            database = ifp.readline().strip()
        # sys.stderr.write("viewxps.py connecting to database with dbname='{}' user='{}' host='{}' password='{}'\n"
        #                  .format( database, user, host, password ))
        self.db = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'"
                                   .format( database, user, host, password ))
        # sys.stderr.write("db.status is {}\n".format(self.db.status))

    def finalize(self):
        self.db.close()
        
    def htmltop(self):
        self.response = "<!DOCTYPE html>\n"
        self.response += "<html>\n<head>\n<meta charset=\"UTF-8\">\n"
        self.response += "<link rel=\"stylesheet\" href=\"{}decat.css\">\n".format( webapdirurl )
        self.response += "<script src=\"{}decatview.js\"></script>\n".format( webapdirurl )
        self.response += "<title>DECaT LBL Pipeline Candidate Viewer</title>\n"
        self.response += "</head>\n<body>\n"

    def htmlbottom(self):
        self.response += "\n</body>\n</html>\n"
        
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

        data = web.input()
        date0 = data["date0"] if "date0" in data else ""
        date1 = data["date1"] if "date1" in data else ""
        
        self.response += "<h1>DECAT subtraction viewer</h1>\n"

        self.response += "<h2>Exposure Search</h3>\n"
        self.response += "<p>Enter dates as yyyy-mm-dd or yyyy-mm-dd hh:mm:ss or yyyy-mm-dd hh:mm:ss-05:00\n"
        self.response += "(the last one indicating a time zone that is 5 hours before UTC)."
        self.response += "<form method=\"POST\" action=\"{}/listexp\"><p>\n".format( webapfullurl )
        self.response += "<p>List exposures from date\n";
        self.response += "<input type=\"text\" name=\"date0\" value=\"{}\">\n".format( date0 )
        self.response += "&nbsp;&nbsp;to&nbsp;&nbsp;"
        self.response += "<input type=\"text\" name=\"date1\" value=\"{}\"></p>\n".format( date1 )
        self.response += "<p><input type=\"submit\" name=\"submit\" value=\"List Exposures\"></p>\n"
        self.response += "</form>\n"

        self.response += "<h2>Candidate Search</h3>\n"
        self.response += "<form method=\"POST\" action=\"{}/findcands\"></p>\n".format( webapfullurl )
        self.response += "<table class=\"candsearchparams\">\n"
        self.response += "<tr><td>Min # Detections:</td>"
        self.response += ( "<td><input name=\"mindet\" type=\"number\" "
                           "min=\"1\" max=\"1000\" step=\"1\" value=\"5\"></td></tr>\n" )
        self.response += "<tr><td>Min # rb≥"
        self.response += ( "<input name=\"rbcut\" type=\"number\" min=\"0\" max=\"1\" step=\"any\" "
                           "size=\"4\" value=\"0.6\"></td>" )
        self.response += ( "<td><input name=\"minrb\" type=\"number\" min=\"0\" max=\"1000\" step=\"1\" "
                           "value=\"5\"></td></tr>\n" )
        self.response += "<tr><td>Min diff. days detected:</td>"
        self.response += ( "<td><input name=\"numdays\" type=\"number\" min=\"0\" max=\"1000\" step=\"1\" "
                           "value=\"3\"></td></tr>\n" )
        self.response += "<tr><td>Min (brightest) mag ≤</td>"
        self.response += ( "<td><input name=\"minmag\" type=\"number\" min=\"15\" max=\"30\" step=\"0.1\" "
                           "value=\"25\"></td></tr>\n" )
        self.response += "<tr><td>Max (dimmest) mag ≥</td>"
        self.response += ( "<td><input name=\"maxmag\" type=\"number\" min=\"15\" max=\"30\" step=\"0.1\" "
                           "value=\"15\"></td></tr>\n" )
        self.response += "<tr><td><input name=\"submit\" type=\"submit\" value=\"Search\"></td></tr>"
        self.response += "</table>\n</form>\n"
        
        
        self.htmlbottom()
        return self.response
        
# ======================================================================

class FindCandidates(HandlerBase):
    def do_the_things(self):
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        
        data = web.input()
        minnum = int( data["mindet"] )
        rbcut = float( data["rbcut"] )
        minrbnum = int( data["minrb"] )
        mindays = int( data["numdays"] )
        minmag = float( data["minmag"] )
        maxmag = float( data["maxmag"] )

        self.response += "<p><a href=\"{}\">Back to Home</a></p>\n".format( webapfullurl )

        self.response += ( "<h4>Candidates with ≥{} detections (≥{} with rb≥{:.2f})<br>\n"
                           .format( minnum, minrbnum, rbcut ) )
        self.response += ( "seen at ≥{} days apart with brightest mag. ≤{:.2f} and dimmest mag. ≥{:.2f}</h4>\n"
                           .format( mindays, minmag, maxmag ) )
                           
        cursor = self.db.cursor( cursor_factory = psycopg2.extras.DictCursor )

        # One early days check suggested this returned 1/20 of the whole candidates table
        query = ( "SELECT c.id,count(o.id) AS countobj,max(o.rb) AS maxrb "
                  "FROM candidates c "
                  "INNER JOIN objects o ON o.candidate_id=c.id "
                  "GROUP BY (c.id) "
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
        sys.stderr.write( "Initial query returned {} candidates\n".format( len(rows) ) )
        
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

        query = ( "SELECT c.id AS candid,o.id AS objid,o.rb,o.mag,e.mjd,e.filter "
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
            cands[cid]["jdlist"].append( mjd )
            cands[cid]["filterlist"].append( filt )
            cands[cid]["maglist"].append( mag )
            cands[cid]["rblist"].append( rb )
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
        sys.stderr.write( "After cuts, {} remain.\n".format( len(candlist) ) )
        
        self.response += "<form method=\"POST\" action=\"{}showcand\">\n".format( webapfullurl )
        self.response += "<table class=\"candlist\">"
        self.response += "<tr><th>Candidate</th>"
        self.response += "<th># Det</th>"
        self.response += "<th>rb≥{:.2f}</th>".format(rbcut)
        self.response += "<th>dimmest</th>"
        self.response += "<th>brightest</th>"
        self.response += "<th>t0</th>"
        self.response += "<th>t1</th>"
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
        
        data = web.input()
        if "date0" in data and len(data["date0"].strip())>0:
            date0 = data["date0"]
        else:
            date0 = "1970-01-01"
        if "date1" in data and len(data["date1"].strip())>0:
            date1 = data["date1"]
        else:
            date1 = "2999-12-31"

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

        mjd0 = mjd( t0.year, t0.month, t0.day, t0.hour, t0.minute, t0.second )
        mjd1 = mjd( t1.year, t1.month, t1.day, t1.hour, t1.minute, t1.second )

        # self.response += "<p>{} is {},{},{},{},{} and mjd {}</p>\n".format( date0, t0.year, t0.month, t0.day,
        #                                                                     t0.hour, t0.minute, mjd0 )
        # self.response += "<p>{} is {},{},{},{},{} and mjd {}</p>\n".format( date1, t1.year, t1.month, t1.day,
        #                                                                     t1.hour, t1.minute, mjd1 )
        
        self.response += '<form method=\"POST\" action=\"{}\">\n'.format( webapfullurl )
        self.response += '<input type=\"hidden\" name=\"date0\" value=\"{}\">\n'.format( date0 )
        self.response += '<input type=\"hidden\" name=\"date1\" value=\"{}\">\n'.format( date1 )
        self.response += "<p><button class=\"link\" type=\"submit\">Back to Home</button></p>\n"
        self.response += "</form>\n"
        
        self.response += "<h4>Exposures from {} to {}</h4>\n".format( t0.isoformat(), t1.isoformat() )

        exposures = {}
        exporder = []
        
        # sys.stderr.write("ListExposures about to send DB query\n")
        cursor = self.db.cursor( )
        query = ( "SELECT e.filename,e.filter,COUNT(s.id) FROM exposures e "
                  "LEFT JOIN subtractions s ON s.exposure_id=e.id "
                  "WHERE e.mjd>=%s AND e.mjd<=%s"
                  "GROUP BY e.filename,e.filter,e.mjd "
                  "ORDER BY e.mjd" )
        cursor.execute(query, (mjd0, mjd1) )
        rows = cursor.fetchall()
        for row in rows:
            exporder.append( row[0] )
            exposures[ row[0] ] = { "filter": row[1], "nsubs": row[2] }

        if len(exporder) == 0:
            self.response += "<p>No exposures!</p>\n"
            self.htmlbottom()
            cursor.close()
            return self.response
            
        query = ( "SELECT filename,ra,dec,header->'EXPTIME' FROM exposures WHERE filename IN %s" )
        # sys.stderr.write( "{}\n".format( cursor.mogrify( query, ( tuple(exporder), ) ) ) )
        cursor.execute( query, ( tuple(exporder), ) )
        rows = cursor.fetchall()
        for row in rows:
            if not row[0] in exposures:
                sys.stderr.write("WARNING: exposure {} found in object query, not ra/dec query!"
                                 .format( row[0] ))
            exposures[ row[0] ]["ra"] = float( row[1] )
            exposures[ row[0] ]["dec"] = float( row[2] )
            exposures[ row[0] ]["exptime"] = float( row[3] )
            
        query = ( "SELECT e.filename,COUNT(o.id) FROM OBJECTS o "
                  "INNER JOIN subtractions s ON o.subtraction_id=s.id "
                  "RIGHT JOIN exposures e ON s.exposure_id=e.id "
                  "WHERE e.filename IN %s "
                  "GROUP BY e.filename,e.filter,e.mjd "
                  "ORDER BY e.mjd " )
        cursor.execute( query, ( tuple(exporder), ) )
        rows = cursor.fetchall()
        for row in rows:
            if not row[0] in exposures:
                sys.stderr.write("WARNING: exposure {} found in object query, not subtraction query!"
                                 .format( row[0] ))
                exposures[ row[0] ] = { "nsubs": "??" }
                exporder.append( row[0] )    # will be out of order!!!!!
            exposures[ row[0] ]["nobjs"] = row[1]

        query = ( "SELECT e.filename,COUNT(o.id) FROM OBJECTS o "
                  "INNER JOIN subtractions s ON o.subtraction_id=s.id "
                  "RIGHT JOIN exposures e ON s.exposure_id=e.id "
                  "WHERE o.rb>=0.6 AND e.filename IN %s"
                  "GROUP BY e.filename,e.filter,e.mjd "
                  "ORDER BY e.mjd " )
        cursor.execute( query, ( tuple(exporder), ) )
        rows = cursor.fetchall()
        for row in rows:
            exposures[ row[0] ]["nhighrb"] = row[1]

        # Count the number that have made it through event id type "objectslogged" (27)
        query = ( "SELECT e.filename,COUNT(p.id) FROM exposures e "
                  "INNER JOIN processcheckpoints p ON e.id=p.exposure_id "
                  "WHERE e.filename in %s AND p.event_id=27 "
                  "GROUP BY e.filename" )
        cursor.execute( query, ( tuple(exporder), ) )
        rows = cursor.fetchall()
        cursor.close()
        for row in rows:
            exposures[ row[0] ]["nfinished"] = row[1]
            
        cursor.close()
            
        self.response += "Number of objects per page: <input type=\"number\" name=\"numperpage\" value=100><br>\n"
        self.response += "Only include ccd numbers (comma-sep): "
        self.response += "<input type=\"text\" name=\"ccds\" id=\"ccds\" value=\"\">\n<br>\n"
        self.response += "  Order by:\n"
        self.response += "<input type=\"radio\" name=\"orderby\" id=\"real/bogus\" value=\"real/bogus\" checked=1>\n"
        self.response += "  <label for=\"real/bogus\">Real/Bogus</label>\n"
        self.response += "<input type=\"radio\" name=\"orderby\" id=\"objnum\" value=\"objnum\">\n"
        self.response += "  <label for=\"objnum\">Object Num.</label><br>\n"
        self.response += "<input type=\"checkbox\" id=\"showrb\" name=\"showrb\" value=\"showrb\" checked=1>\n"
        self.response += "  <label for=\"showrb\">Show r/b?</label></p>\n"
        self.response += "<input type=\"hidden\" name=\"offset\" id=\"offset\" value=0>\n</p>\n"
        self.response += "<input type=\"hidden\" name=\"date0\" id=\"date0\" value={}>\n</p>\n".format(date0)
        self.response += "<input type=\"hidden\" name=\"date1\" id=\"date1\" value={}>\n</p>\n".format(date1)
        self.response += "<table class=\"exposurelist\">\n"
        self.response += ( "<tr><th>Exposure</th><th>Filter</th><th>t_exp</th>"
                           "<th>ra</th><th>dec</th><th>l</th><th>b</th>"
                           "<th>#Subs</th><th>#Done</th><th>N. Objects</th>"
                           "<th>rb>=0.6</th></tr>\n" )
        for exp in exporder:
            ra = exposures[exp]["ra"]
            dec = exposures[exp]["dec"]
            l,b = radectolb( ra, dec )
            self.response += '<tr><td>{exp}</td>\n'.format(exp=exp);
            self.response += "  <td>{}</td>\n".format( exposures[exp]["filter"] )
            self.response += "  <td>{}</td>\n".format( exposures[exp]["exptime"] )
            self.response += '  <td>{}</td>\n'.format(dtohms(ra))
            self.response += '  <td>{}</td>\n'.format(dtodms(dec))
            self.response += '  <td>{:.02f}</td>\n'.format(l)
            self.response += '  <td>{:.02f}</td>\n'.format(b)
            self.response += "  <td>{}</td>\n".format( exposures[exp]["nsubs"] )
            if "nfinished" in exposures[exp]:
                self.response += "  <td>{}</td>\n".format( exposures[exp]["nfinished"] )
            else:
                self.response += "  <td>—</td>\n"
            self.response += "  <td>{}</td>\n".format( exposures[exp]["nobjs"] )
            if "nhighrb" in exposures[exp]:
                self.response += "  <td>{}</td>\n".format( exposures[exp]["nhighrb"] )
            else:
                self.response += "  <td>—</td>\n"
            self.response += ( "  <td><button type=\"submit\" name=\"submit\" value=\"Show Objects\" "
                               "onclick=\"showobjects('{}')\">Show Objects</button></td>\n".format( exp ) )
            self.response += ( "  <td><button type=\"submit\" name=\"submit\" value=\"Show Log\" "
                               "onclick=\"showlog('{}')\">Show Log</button></td>\n".format( exp ) )
            self.response += "</tr>\n"
            
        self.response += "</table>\n"

        self.htmlbottom()
        return self.response

# ======================================================================

class ShowExposure(HandlerBase):
    
    def prevnext( self, state, numobjs ):
        nextarr = []
        prevarr = []
        for key in state:
            if key=="offset":
                value = int(state["offset"]) + int(state["numperpage"])
            else:
                value = state[key]
            nextarr.append( form.Hidden( key, value=value ) )
            if key=="offset":
                value = int(state["offset"]) - int(state["numperpage"])
                value = value if value > 0 else 0
            prevarr.append( form.Hidden( key, value=value ) )
        nextarr.append( form.Button( "Next {}".format( state["numperpage"] ),
                                     type="submit",
                                     formaction="{}/showexp".format( webapfullurl ) ) )
        nextarr.append( form.Button( "Previous {}".format( state["numperpage"] ),
                                     type="submit",
                                     formaction="{}/showexp".format( webapfullurl ) ) )
        
        nextform = form.Form( *nextarr )
        prevform = form.Form( *prevarr )


        self.response += "<form method=\"Post\" action=\"{}\">\n".format( webapfullurl );
        self.response += "<input type=\"hidden\" name=\"date0\" value=\"{}\">\n".format( state[ "date0" ] )
        self.response += "<input type=\"hidden\" name=\"date1\" value=\"{}\">\n".format( state[ "date1" ] )
        self.response += "<p><input type=\"submit\" name=\"submit\" value=\"Back to home\" class=\"link\"></p>\n"
        self.response += "</form>\n"
        # self.response += "<p><a href=\"{}/\">Back to home</a></p>\n".format(webapfullurl);

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
        
    def do_the_things(self):
        # global rightpassword
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()

        state = { "date0": "",
                  "date1": "",
                  "offset": 0,
                  "numperpage": 100,
                  "exposure": "",
                  "orderby": "real/bogus",
                  "ccds": "",
                  "showrb": False,
                  "whattodo": "Show Objects",
                  }
        data = web.input()
        for stateval in state:
            if stateval in data:
                state[stateval] = data[stateval]

        sys.stderr.write( "ShowExposure data = {}, state = {}\n".format( json.dumps( data ),
                                                                         json.dumps( stateval ) ) )
                
        if state["whattodo"] == "Show Objects":
            self.show_objects( state )
        elif state["whattodo"] == "Show Log":
            self.show_log( state )
        else:
            self.response += "<p>Unknown action {}, go back.</p>".format( state["whattodo"] )

        self.htmlbottom()
        return self.response

    # ========================================
    
    def show_objects( self, state ):
        offset = int( state["offset"] )
        if offset < 0: offset = 0
        numperpage = int( state["numperpage"] )
        filename = state["exposure"]
        orderby = state["orderby"]
        showrb = state["showrb"]
            
        ccdarr = state["ccds"].split(",")
        if len(ccdarr) == 0: ccds = None
        else:
            ccdnums = []
            for i in ccdarr:
                try:
                    val = int(i)
                except ValueError:
                    continue
                else:
                    ccdnums.append( val )
            if len(ccdnums) == 0:
                ccds = None
            else:
                ccds = "("
                first = True
                for num in ccdnums:
                    if first: first=False
                    else: ccds += ","
                    ccds += '{}'.format(num)
                ccds += ")"

        query = ( "SELECT COUNT(o.id) "
                  "FROM objects o "
                  "INNER JOIN subtractions s ON o.subtraction_id=s.id "
                  "INNER JOIN exposures e ON s.exposure_id=e.id "
                  "WHERE e.filename=%s " )
        if ccds is not None:
            query += " AND s.ccdnum IN {} ".format(ccds)
        cursor = self.db.cursor( )
        # sys.stderr.write( "Sending query \"{}\"\n".format( cursor.mogrify( query, [ filename ] ) ) )
        cursor.execute( query, [ filename ] )
        row = cursor.fetchone()
        cursor.close()
        numobjs = int(row[0])

        self.prevnext( state, numobjs )

        self.response += "<h3>Exposure: {}</h3>\n".format( filename )
        self.response += "<h4>Candidates starting at offset {} out of {}</h4>\n".format( offset, numobjs )

        query = ( "SELECT c.id as cid,o.id as oid,o.rb,o.ra,o.dec,s.ccdnum,e.filename, "  # tg.knopgood,tg.nugentgood,"
                  "ENCODE(cu.sci_jpeg, 'base64') as scijpg, "
                  "ENCODE(cu.ref_jpeg, 'base64') as refjpg, "
                  "ENCODE(cu.diff_jpeg, 'base64') as diffjpg "
                  "FROM objects o "
                  "INNER JOIN candidates c ON o.candidate_id=c.id "
                  "INNER JOIN subtractions s ON o.subtraction_id=s.id "
                  "INNER JOIN exposures e ON s.exposure_id=e.id "
                  "LEFT JOIN cutouts cu ON cu.object_id=o.id "
                  # "LEFT JOIN tmpobjgood tg ON tg.objid=o.id "
                  "WHERE e.filename=%s " );
        if ccds is not None:
            query += ' AND s.ccdnum IN {} '.format(ccds)
        if orderby == "real/bogus":
            query += " ORDER BY o.rb DESC "
        elif orderby == "objnum":
            query += " ORDER BY o.id "
        query += " LIMIT %s OFFSET %s"
        # sys.stderr.write("Sending query \"{}\"\n".format(query))
        cursor = self.db.cursor(  cursor_factory = psycopg2.extras.DictCursor )
        cursor.execute( query, ( filename, numperpage, offset ) )
        rows = cursor.fetchall()
        cursor.close()

        self.response += "<form method=\"POST\" action=\"{}showcand\">\n".format( webapfullurl )
        for key in state:
            if key != "candidate":
                self.response += ( "<input type=\"hidden\" name=\"{}\" value=\"{}\">\n"
                                   .format( key, state[key] ) )
        
        self.response += "<table class=\"maintable\">\n"
        self.response += "<tr><th>Info</th><th>New</th><th>Ref</th><th>Sub</th></tr>\n"
        
        for row in rows:
            candid = row["cid"]
            objid = row["oid"]
            rb = row["rb"]
            ra = row["ra"]
            dec = row["dec"]
            ccdnum = row["ccdnum"]
            filename = row["filename"]
            # good = { "knop": row[7], "nugent": row[8] }
            scib64 = row["scijpg"]
            refb64 = row["refjpg"]
            diffb64 = row["diffjpg"]
            
            self.response += "<tr>\n"
            self.response += "<td>Candidate: "
            self.response += ( "<button class=\"link\" name=\"candidate\" value=\"{candid}\">{candid}</button><br>\n"
                               .format( candid=candid ) )
            if showrb:
                self.response += "<b>rb: {:.4f}</b><br>\n".format( rb )
            self.response += u"α: {} &nbsp;&nbsp; δ: {}<br>\n".format( dtohms( ra ), dtodms( dec ) )
            self.response += "File: {} ccd: {}\n".format(filename.replace(".fz",""), ccdnum)

            if scib64 is None:
                self.response += "<td>(Sci cutout missing)</td>\n"
            else:
                # sciimg = self.pngify( scib64 )
                # self.response += ( "<td><img src=\"data:image/jpeg;base64,{}\" width=204 height=204 alt=\"New\"></td>\n"
                #                    .format( base64.b64encode( sciimg ).decode("ascii" ) ) )
                self.response += ( "<td><img src=\"data:image/jpeg;base64,{}\" width=204 height=204 alt=\"New\"></td>\n"
                                   .format( scib64 ) )

            if refb64 is None:
                self.response += "<td>(Ref cutout missing)</td>\n"
            else:
                self.response += ( "<td><img src=\"data:image/jpeg;base64,{}\" width=200 height=200 alt=\"Ref\"></td>\n"
                                   .format( refb64 ) )

            if diffb64 is None:
                self.response += "<td>(Diff cutout missing)</td>\n"
            else:
                self.response += ( "<td><img src=\"data:image/jpeg;base64,{}\" width=200 height=200 alt=\"Sub\"></td>\n"
                                   .format( diffb64 ) )

            # if ( user == "knop" or user == "nugent" ) and passwd == rightpassword:
            #     self.response += "<td>\n"
            #     self.response += "<input type=\"radio\" value=\"good\" "
            #     self.response += "name=\"{obj}statusset\" id=\"{obj}statusgood\" ".format( obj=objid )
            #     self.response += ( "onchange=\"sendgoodbad( &quot;{}&quot;, &quot;{}&quot;, &quot;{}&quot;, "
            #                        "&quot;good&quot; )\">\n".format( user, passwd, objid ) )
            #     self.response += "  <label for=\"{obj}statussgood\">Good</label><br>\n"
            #     self.response += "<input type=\"radio\" value='bad' "
            #     self.response += "name=\"{obj}statusset\" id=\"{obj}statusbad\" ".format( obj=objid )
            #     self.response += ( "onchange=\"sendgoodbad( &quot;{}&quot;, &quot;{}&quot;, &quot;{}&quot;, "
            #                        "&quot;bad&quot; )\">\n".format( user, passwd, objid ) )
            #     self.response += "  <label for=\"{obj}statussbad\">Bad</label><br>\n"
            #     self.response += "Current: "
            #     if good[user] is None:
            #         self.response += "<span id=\"{}status\"><i>(not set)</i></span>\n".format(objid)
            #     elif good[user]:
            #         self.response += "<span id=\"{}status\">Good</span>\n".format(objid)
            #     else:
            #         self.response += "<span id=\"{}status\">Bad</span>\n".format(objid)
            # else:
            #     self.response += "<td>\n";
            #     for user in ( "knop", "nugent" ):
            #         self.response += "{}: ".format(user)
            #         if good[user] is None:
            #             self.response += "<i>(not set)</i>"
            #         elif good[user]:
            #             self.response += "<span class=\"good\">good</span>"
            #         else:
            #             self.response += "<span class=\"bad\">bad</span>"
            #         if user == "knop":
            #             self.response += "<br>\n"
            #     self.response += "\n</td>\n"

            self.response += "</tr>\n"
        self.response += "</table>\n"

        self.prevnext( state, numobjs )

    # ========================================
        
    def show_log( self, state ):
        query = ( "SELECT p.created_at,p.ccdnum,p.running_node,p.mpi_rank,p.notes,c.description,p.event_id "
                  "FROM processcheckpoints p "
                  "INNER JOIN exposures e ON p.exposure_id=e.id "
                  "LEFT JOIN checkpointeventdefs c ON p.event_id=c.id "
                  "WHERE e.filename=%s ORDER BY p.created_at" )
        cursor = self.db.cursor( cursor_factory = psycopg2.extras.DictCursor )
        cursor.execute( query, ( state["exposure"], ) )
        rows = cursor.fetchall()
        cursor.close()

        self.response += "<h2>{}</h2>\n".format( state["exposure"] )

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

class SetGoodBad(HandlerBase):
    def do_the_things( self ):
        global rightpassword
        web.header('Content-Type', 'application/json')

        data = json.loads( web.data().decode(encoding='utf-8') )
        if "user" not in data:
            rval = { "error": "No user specified" }
            return json.dumps(rval)
        if data["user"] not in ["knop","nugent"]:
            rval = { "error": "Unknown user {}".format(data["user"]) }
            return json.dumps(rval)
        if ( "password" not in data or data["password"] != rightpassword ):
            rval = { "error": "Incorrect password" }
            return json.dumps(rval)
        if ( "obj" not in data ) or ( "status" not in data ):
            rval = { "error": "Mal-formed post data" }
            return json.dumps(rval)

        objid = int(data["obj"])
        if data["status"] == "good": goodbad = True
        elif data["status"] == "bad": goodbad = False
        else:
            rval = { "error": "{} is neither good nor bad".format( data["status"] ) }
            return json.dumps(rval)

        # There's a bobby tables issue here, except that I've already made sure
        #   that data["user"] is one of "knop","nugent"
        query = ( "INSERT INTO tmpobjgood(objid,{user}good) VALUES(%s, {state}) "
                  "ON CONFLICT ON CONSTRAINT tmpobjgood_pkey DO UPDATE SET "
                  "{user}good={state}".format( user=data["user"], state="True" if goodbad else "False" ) )
        cursor = self.db.cursor()
        cursor.execute( query, [ objid ] )
        self.db.commit()
        cursor.close()

        rval = { "objid": objid, "status": "good" if goodbad else "bad" }
        return json.dumps(rval)
        

# ======================================================================

class ShowCandidate(HandlerBase):
    def do_the_things( self ):
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()
        data = web.input()

        # I'm typing this a second time so I know I'm Doing It Wrong
        state = { "date0": "",
                  "date1": "",
                  "offset": 0,
                  "numperpage": 100,
                  "exposure": "",
                  "candidate": "",
                  "orderby": "real/bogus",
                  "ccds": "",
                  "showrb": False }
        for stateval in state:
            if stateval in data:
                state[stateval] = data[stateval]

        self.response += '<form method=\"POST\" action=\"{}\">\n'.format( webapfullurl )
        self.response += '<input type=\"hidden\" name=\"date0\" value=\"{}\">\n'.format( state[ "date0" ] )
        self.response += '<input type=\"hidden\" name=\"date1\" value=\"{}\">\n'.format( state[ "date1" ] )
        self.response += "<p><button class=\"link\" type=\"submit\">Back to Home</button></p>\n"
        self.response += "</form>\n"

        self.response += "<h3>Candidate: {}</h3>\n".format( state["candidate"] )
        self.response += ( "<p>(<a href=\"{}/showcand?candidate={}\">Share Link</a>)</p>\n"
                           .format( webapfullurl, state[ "candidate" ] ) )
        
        cursor = self.db.cursor( cursor_factory = psycopg2.extras.DictCursor )
        query = ( "SELECT * FROM candidates WHERE id=%s" )
        cursor.execute( query, ( state["candidate"], ) )
        rows = cursor.fetchall()
        if len(rows) == 0:
            self.db.close()
            self.response += "<p>No candidate {}</p>\n".format( state["candidate"] )
            self.htmlbottom()
            return self.response
        candidate = rows[0]

        query= ( "SELECT o.id,o.ra,o.dec,o.created_at,o.modified,o.rb,o.mag,o.magerr, "
                 "e.filename,e.filter,e.mjd, "
                 "ENCODE(c.sci_jpeg, 'base64') as sci_jpeg, "
                 "ENCODE(c.ref_jpeg, 'base64') as ref_jpeg, "
                 "ENCODE(c.diff_jpeg, 'base64') as diff_jpeg "
                 "FROM objects o "
                 "INNER JOIN subtractions s ON o.subtraction_id=s.id "
                 "INNER JOIN exposures e ON s.exposure_id=e.id "
                 "LEFT JOIN cutouts c ON c.object_id=o.id "
                 "WHERE o.candidate_id=%s ORDER BY e.mjd,e.filter" )
        sys.stderr.write( "{}\n".format( cursor.mogrify( query, ( state["candidate"], ) ) ) )
        cursor.execute( query, ( state["candidate"], ) )
        rows = cursor.fetchall()
        cursor.close()
        
        self.response += "<form method=\"post\" action=\"{}/showexp\"></p>\n".format( webapfullurl )

        for key in state:
            if key != "exposure":
                self.response += ( "<input type=\"hidden\" name=\"{}\" value=\"{}\">\n"
                                   .format( key, state[key] ) )

        self.response += "<table class=\"maintable\">\n"
        self.response += "<tr><th>Exposure</th><th>New</th><th>Ref</th><th>Sub</th></tr>\n"

        for row in rows:
            # Make exposure a button that looks like a link
            self.response += "<tr>\n<td>Exposure: {}<br>\n".format( row["filename"] )
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
    "/dumpdata", "DumpData" ,
    "/setgoodbad", "SetGoodBad" ,
    )

application = web.application(urls, globals()).wsgifunc()

