#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import math
import os
import re
import json
import psycopg2
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
# sys.stderr.write("About to import astropy\n")
# s.environ["XDG_CONFIG_HOME"] = "/var/www/astropy"
# os.environ["XDG_CACHE_HOME"] = "/var/www/astropy"
# from astropy.coordinates import SkyCoord
# sys.stderr.write("Imported astropy\n")
# import fitsio


webapfullurl = "https://c3.lbl.gov/raknop/viewexps.py"
webapdir = "/var/www/raknop"
DBdata = "/home/raknop/secret/dbinfo.txt"
rightpassword = "Lambdaneq0"

# ======================================================================

def sanitizeHTML(text, oneline = False):
    tagfinder = re.compile("^\<(\S+)\>$")
    ampfinder = re.compile("\&([^;\s]*\s)")
    ampendfinder = re.compile("\&([^;\s]*)$")
    ltfinder = re.compile("<((?!a\s*href)[^>]*\s)")
    ltendfinder = re.compile("<([^>]*)$")
    gtfinder = re.compile("((?<!\<a)\s[^<]*)>")
    gtstartfinder = re.compile("^([<]*)>");
    
    def tagfilter(text):
        tagfinder = re.compile("^\<(\S+)\>$")
        # linkfinder = re.compile("^\s*a\s+\"[^\"]+\"\s*")     # I think this didn't work
        linkfinder = re.compile("^\s*a\s+href\s*=\s*\"[^\"]+\"\s*((target|style)\s*=\s*\"[^\"]*\"\s*)*")
        imgfinder = re.compile("^\s*img\s+((src|style|width|height|alt)\s*=\s*\"[^\"]*\"\s*)*$")
        match = tagfinder.match(text)
        if match is None:
            return None
        contents = match.group(1)
        if linkfinder.match(contents) is not None:
            return text
        if imgfinder.match(contents) is not None:
            return text
        if ( (contents.lower() == "i") or (contents.lower() == "b") or (contents.lower() == "tt") ):
            return text
        elif ( (contents.lower() == "/i") or (contents.lower() == "/b") or
               (contents.lower() == "/tt") or (contents.lower() == "/a") ):
            return text
        elif contents.lower() == "sup":
            return "<span class=\"sup\">"
        elif contents.lower() == "/sup":
            return "</span>"
        elif contents.lower() == "sub":
            return "<span class=\"sub\">"
        elif contents.lower() == "/sub":
            return "</span>"
        else:
            return "&lt;{}&rt;".format(contents)

    newtext = tagfinder.sub(tagfilter, text)
    newtext = ampfinder.sub("&amp;\g<1>", newtext, count=0)
    newtext = ampendfinder.sub("&amp;\g<1>", newtext, count=0)
    newtext = ltfinder.sub("&lt;\g<1>", newtext, count=0)
    newtext = ltendfinder.sub("&lt;\g<1>", newtext, count=0)
    newtext = gtfinder.sub("\g<1>&gt;", newtext, count=0)
    newtext = gtstartfinder.sub("\g<1>&gt;", newtext, count=0)

    if oneline:
        pass   # I hope I don't regret this
    else:
        newtext = re.sub("^(?!\s*<p>)", "<p>", newtext, count=0)
        newtext = re.sub("([^\n])$", "\g<1>\n", newtext, count=0)
        newtext = re.sub("\s*\n", "</p>\n", newtext, count=0)
        newtext = re.sub("</p></p>", "</p>", newtext, count=0)
        newtext = re.sub("\n(?!\s*<p>)([^\n]*</p>)", "\n<p>\g<1>", newtext, count=0)
        newtext = re.sub("^\s*<p></p>\s*$", "", newtext, count=0)
        newtext = re.sub("\n", "\n\n", newtext, count=0)
    
    return newtext;

def dtohms( degrees ):
    fullhours = degrees/15.
    hours = int(math.floor(fullhours))
    minutes = int( math.floor( (fullhours - hours) * 60 ) )
    seconds = int( math.floor( ((fullhours - hours)*60 - minutes) * 60  + 0.5 ) )
    return "{:02d}:{:02d}:{:02d}".format(hours, minutes, seconds)

def dtodms( degrees ):
    sign = '+' if degrees > 0 else '-'
    degrees = math.fabs( degrees )
    degs = int(math.floor(degrees))
    mins = int( math.floor( (degrees-degs) * 60 ) )
    secs = int( math.floor( ((degrees-degs)*60 - mins) * 600 + 0.5 ) ) / 10.
    return "{:1s}{:02d}:{:02d}:{:04.1f}".format(sign, degs, mins, secs)

def radectolb( ra, dec ):
    """ra and dec in degrees"""
    decngp = 27.12825 * math.pi/180.
    rangp = 192.85948 * math.pi/180.
    lncp = 122.93192 * math.pi/180.
    ra *= math.pi/180.
    dec *= math.pi/180.
    b = math.asin( math.sin(dec)*math.sin(decngp) + math.cos(dec)*math.cos(decngp)*math.cos(ra-rangp) )
    l = lncp - math.atan2( math.cos(dec)*math.sin(ra-rangp)/math.cos(b) ,
                           (math.sin(dec)*math.cos(decngp) - math.cos(dec)*math.sin(decngp)*math.cos(ra-rangp))
                           / math.cos(b) )
    if l < 0: l += 2.*math.pi
    return l*180./math.pi, b*180./math.pi

def mjd( y,m,d,h,minute,s ):
    # Trusting Wikipedia....
    if ( h < 12 ): d -= 1
    jd = ( int( 1461 * ( y + 4800 + int( (m - 14) / 12 ) ) / 4 ) +
           + int( (367 * (m - 2 - 12 * int( (m - 14) / 12 ) ) ) / 12 )
           - int( (3 * int( (y + 4900 + int( (m - 14) / 12) ) / 100 ) ) / 4 )  + d - 32075 )
    jd += ( (h-12) + minute/60. + s/3600. ) / 24.
    if (h < 12):
        jd += 1.
    return jd - 2400000.5

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
        self.response += "<link rel=\"stylesheet\" href=\"/raknop/viewexps.css\">\n"
        self.response += "<script src=\"/raknop/viewexps.js\"></script>\n"
        self.response += "<title>Quick-n-dirty candidate viewer</title>\n"
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

        self.response += "<h1>DECAT subtraction viewer</h1>\n"
        self.response += "<p>Enter dates as yyyy-mm-dd or yyyy-mm-dd hh:mm:ss or yyyy-mm-dd hh:mm:ss-05:00\n"
        self.response += "(the last one indicating a time zone that is 5 hours before UTC)."
        self.response += "<form method=\"POST\" action=\"{}/listexp\"><p>\n".format( webapfullurl )
        self.response += "<p>List exposures from date\n";
        self.response += "<input type=\"text\" name=\"date0\" value=\"\">\n"
        self.response += "&nbsp;&nbsp;to&nbsp;&nbsp;"
        self.response += "<input type=\"text\" name=\"date1\" value=\"\"></p>\n"
        self.response += "<p><input type=\"submit\" name=\"submit\" value=\"List Exposures\"></p>\n"
        self.response += "</form>\n"

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
            self.response += "<p>...error parsing your dates...</p>\n"
            self.htmlbottom()
            return self.response

        mjd0 = mjd( t0.year, t0.month, t0.day, t0.hour, t0.minute, t0.second )
        mjd1 = mjd( t1.year, t1.month, t1.day, t1.hour, t1.minute, t1.second )

        self.response += "<p><a href=\"{}/\">Back to home</a></p>\n".format(webapfullurl);
        self.response += "<h4>Exposures from {} to {}</h4>\n".format( t0.isoformat(), t1.isoformat() )

        exposures = {}
        exporder = []
        
        # sys.stderr.write("ListExposures about to send DB query\n")
        cursor = self.db.cursor()
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
            
        query = ( "SELECT filename,ra,dec FROM exposures WHERE filename IN %s" )
        sys.stderr.write( "{}\n".format( cursor.mogrify( query, ( tuple(exporder), ) ) ) )
        cursor.execute( query, ( tuple(exporder), ) )
        rows = cursor.fetchall()
        for row in rows:
            if not row[0] in exposures:
                sys.stderr.write("WARNING: exposure {} found in object query, not ra/dec query!"
                                 .format( row[0] ))
            exposures[ row[0] ]["ra"] = float( row[1] )
            exposures[ row[0] ]["dec"] = float( row[2] )
            
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
        cursor.close()
        for row in rows:
            exposures[ row[0] ]["nhighrb"] = row[1]

        cursor.close()
            
        # sys.stderr.write("ListExposures building forms\n")
        self.response += "<form method=\"POST\" action=\"{}/showexp\"><p>\n".format( webapfullurl )
        # self.response += "<form method=\"POST\" action=\"{}/dumpdata\"><p>\n".format( webapfullurl )
        # self.response += "User:\n"
        # self.response += "<input type=\"radio\" name=\"user\" id=\"useridknop\" value=\"knop\">\n"
        # self.response += "  <label for=\"useridknop\">Rob Knop</label>\n";
        # self.response += "<input type=\"radio\" name=\"user\" id=\"useridnugent\" value=\"nugent\">\n"
        # self.response += "  <label for=\"useridnugent\">Peter Nugent</label><br>\n"
        # self.response += "Password: <input type=\"text\" name=\"password\"><br>\n"
        self.response += "Number of objects per page: <input type=\"number\" name=\"numperpage\" value=100><br>\n"
        self.response += "Only include ccd numbers (comma-sep): "
        self.response += "<input type=\"text\" name=\"ccds\" value=\"\">\n<br>\n"
        self.response += "  Order by:\n"
        self.response += "<input type=\"radio\" name=\"orderby\" id=\"real/bogus\" value=\"real/bogus\" checked=1>\n"
        self.response += "  <label for=\"real/bogus\">Real/Bogus</label>\n"
        self.response += "<input type=\"radio\" name=\"orderby\" id=\"objnum\" value=\"objnum\">\n"
        self.response += "  <label for=\"objnum\">Object Num.</label><br>\n"
        self.response += "<input type=\"checkbox\" id=\"showrb\" name=\"showrb\" value=\"showrb\" checked=1>\n"
        self.response += "  <label for=\"showrb\">Show r/b?</label></p>\n"
        self.response += "<input type=\"hidden\" name=\"offset\" value=0>\n</p>\n"
        self.response += "<table class=\"exposurelist\">\n"
        self.response += ( "<tr><th>Exposure</th><th>Filter</th><th>ra</th><th>dec</th><th>l</th><th>b</th>"
                           "<th>N. Subtractions</th><th>N. Objects</th>"
                           "<th>rb>=0.6</th></tr>\n" )
        for exp in exporder:
            ra = exposures[exp]["ra"]
            dec = exposures[exp]["dec"]
            l,b = radectolb( ra, dec )
            self.response += ( "<tr><td><input type=\"submit\" name=\"exposure\" value=\"{}\"></td>\n"
                               .format( exp ) )
            self.response += "  <td>{}</td>\n".format( exposures[exp]["filter"] )
            self.response += '  <td>{}</td>\n'.format(dtohms(ra))
            self.response += '  <td>{}</td>\n'.format(dtodms(dec))
            self.response += '  <td>{:.02f}</td>\n'.format(l)
            self.response += '  <td>{:.02f}</td>\n'.format(b)
            self.response += "  <td>{}</td>\n".format( exposures[exp]["nsubs"] )
            self.response += "  <td>{}</td>\n".format( exposures[exp]["nobjs"] )
            if "nhighrb" in exposures[exp]:
                self.response += "  <td>{}</td>\n".format( exposures[exp]["nhighrb"] )
            else:
                self.response += "  <td>—</td>\n"
            self.response += "</tr>\n"
            
        self.response += "</table>\n"
        self.response += "</form>\n"

        self.htmlbottom()
        return self.response

# ======================================================================

class ShowExposure(HandlerBase):
    
    def prevnext( self, offset, numobjs, numperpage, filename, ccds, showrb, orderby, user, password ):
        nextform = form.Form(
            form.Hidden("offset", value=offset+numperpage),
            form.Hidden("numperpage", value=numperpage),
            form.Hidden("exposure", value=filename),
            form.Hidden("ccds", value=ccds),
            form.Hidden("orderby", value=orderby),
            form.Hidden("user", value=user),
            form.Hidden("password", value=password),
            form.Hidden("showrb", value=( "showrb" if showrb else "no" ) ),
            form.Button("Next {}".format(numperpage), type="submit", formaction="{}/showexp".format(webapfullurl))
        )
        prevform = form.Form(
            form.Hidden("offset", value=offset-numperpage),
            form.Hidden("numperpage", value=numperpage),
            form.Hidden("exposure", value=filename),
            form.Hidden("ccds", value=ccds),
            form.Hidden("orderby", value=orderby),
            form.Hidden("user", value=user),
            form.Hidden("password", value=password),
            form.Hidden("showrb", value=( "showrb" if showrb else "no" ) ),
            form.Button("Previous {}".format(numperpage), type="submit", formaction="{}/showexp".format(webapfullurl))
        )

        self.response += "<p><a href=\"{}/\">Back to home</a></p>\n".format(webapfullurl);
        if offset > 0:
            self.response += "<form method=\"Post\">\n"
            self.response += prevform.render()
            self.response += "</form>\n"
        if offset+numperpage < numobjs:
            self.response += "<form method=\"Post\">\n"
            self.response += nextform.render()
            self.response += "</form>\n"
    
    def do_the_things(self):
        global rightpassword
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        self.htmltop()

        data = web.input()
        offset = int( data["offset"] )
        if offset < 0: offset = 0
        numperpage = int( data["numperpage"] )
        filename = data["exposure"]
        orderby = data["orderby"]
        if "showrb" in data and data["showrb"]=="showrb":
            showrb = True
        else:
            showrb = False
        user = data["user"] if "user" in data else None
        passwd = None
        if user is not None:
            passwd = data["password"] if "password" in data else None
        # sys.stderr.write("user is {} and password is {}\n".format( user, passwd ) )
            
        ccdarr = data["ccds"].split(",")
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
        cursor = self.db.cursor()
        cursor.execute( query, [ filename ] )
        row = cursor.fetchone()
        cursor.close()
        numobjs = int(row[0])

        self.prevnext( offset, numobjs, numperpage, filename, data["ccds"], showrb, orderby, user, passwd )

        self.response += "<h3>Exposure: {}</h3>\n".format( filename )
        self.response += "<h4>User: {}</h3>\n".format( user )
        self.response += "<h4>Candidates starting at offset {} out of {}</h4>\n".format( offset, numobjs )

        query = ( "SELECT c.id,o.id,o.rb,o.ra,o.dec,s.ccdnum,e.filename,tg.knopgood,tg.nugentgood,"
                  "ENCODE(cu.sci_jpeg, 'base64'),ENCODE(cu.ref_jpeg, 'base64'),ENCODE(cu.diff_jpeg, 'base64') "
                  "FROM objects o "
                  "INNER JOIN candidates c ON o.candidate_id=c.id "
                  "INNER JOIN subtractions s ON o.subtraction_id=s.id "
                  "INNER JOIN exposures e ON s.exposure_id=e.id "
                  "LEFT JOIN cutouts cu ON cu.object_id=o.id "
                  "LEFT JOIN tmpobjgood tg ON tg.objid=o.id "
                  "WHERE e.filename=%s " );
        if ccds is not None:
            query += ' AND s.ccdnum IN {} '.format(ccds)
        if orderby == "real/bogus":
            query += " ORDER BY o.rb DESC "
        elif orderby == "objnum":
            query += " ORDER BY o.id "
        query += " LIMIT %s OFFSET %s"
        sys.stderr.write("Sending query \"{}\"\n".format(query))
        cursor = self.db.cursor()
        cursor.execute( query, ( filename, numperpage, offset ) )
        rows = cursor.fetchall()
        cursor.close()

        self.response += "<table class=\"maintable\">\n"
        self.response += "<tr><th>Info</th><th>New</th><th>Ref</th><th>Sub</th></tr>\n"
        
        for row in rows:
            candid = row[0]
            objid = row[1]
            rb = row[2]
            ra = row[3]
            dec = row[4]
            ccdnum = row[5]
            filename = row[6]
            good = { "knop": row[7], "nugent": row[8] }
            scib64 = row[9]
            refb64 = row[10]
            diffb64 = row[11]
            
            self.response += "<tr>\n"
            self.response += "<td>Candidate: {}<br>\n".format( candid )
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

        self.prevnext( offset, numobjs, numperpage, filename, data["ccds"], showrb, orderby, user, passwd )

        self.htmlbottom()
        return self.response

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
    "/listexp", "ListExposures",
    "/showexp", "ShowExposure" ,
    "/dumpdata", "DumpData" ,
    "/setgoodbad", "SetGoodBad" ,
    )

application = web.application(urls, globals()).wsgifunc()
