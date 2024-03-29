import sys
import math
import re
import datetime
import dateutil.parser
import pytz

# ======================================================================
# Utility functions

tagfinder = re.compile("^\<(\S+)\>$")
ampfinder = re.compile("\&([^;\s]*\s)")
ampendfinder = re.compile("\&([^;\s]*)$")
ltfinder = re.compile("<((?!a\s*href)[^>]*\s)")
ltendfinder = re.compile("<([^>]*)$")
gtfinder = re.compile("((?<!\<a)\s[^<]*)>")
gtstartfinder = re.compile("^([<]*)>");

mjdre = re.compile( "^\s*\d{5}\.?\d*\s*$" )

class ErrorMsg(Exception):
    def __init__( self, text="error" ):
        self.text = text

def asDateTime( string ):
    try:
        if string is None:
            return None
        if isinstance( string, datetime.datetime ):
            return string
        string = string.strip()
        if len(string) == 0:
            return None
        if mjdre.search( string ):
            return mjdtodatetime( float(string) )
        dateval = dateutil.parser.parse( string )
        return dateval
    except Exception as e:
        if hasattr( e, 'message' ):
            sys.stderr.write( f'Exception in asDateTime: {e.message}\n' )
        else:
            sys.stderr.write( f'Exception in asDateTime: {e}\n' )
        raise ErrorMsg( f'Error, {string} is not a valid date and time.' )


def sanitizeHTML(text, oneline = False):
    global tagfinder, ampfinder, ampendfinder, ltfinder, ltendfinder, gtfinder, gtstartfinder
    if text is None:
        return ''
    
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
    seconds = int( math.floor( ((fullhours - hours)*60 - minutes) * 6000  + 0.5 ) ) / 100.
    return "{:02d}:{:02d}:{:05.2f}".format(hours, minutes, seconds)

def dtodms( degrees ):
    sign = '+' if degrees > 0 else '-'
    degrees = math.fabs( degrees )
    degs = int(math.floor(degrees))
    mins = int( math.floor( (degrees-degs) * 60 ) )
    secs = int( math.floor( ((degrees-degs)*60 - mins) * 600 + 0.5 ) ) / 10.
    return "{:1s}{:02d}:{:02d}:{:04.1f}".format(sign, degs, mins, secs)

def parsedms( val ):
    parser = re.compile( '([\+\-−]?) *([0-9]+)\s*:?\s*([0-9]+)\s*:?\s*([0-9]+)' )
    match = parser.search( val )
    if match is None:
        return None
    sgn = match.group(1)
    val = float(match.group(2)) + float(match.group(3))/60. + float(match.group(4))/3600.
    if sgn in ['-', '−' ]:
        val *= -1
    return val
    
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

def dateofmjd( mjd ):
    # Again trusting Wikipeida...
    jd = mjd + 2400000.5
    y = 4716
    j = 1401
    m = 2
    n = 12
    r = 4
    p = 1461
    v = 3
    u = 5
    s = 153
    w = 2
    B = 274277
    C = -38

    f = jd + j + (((4 * jd + B) // 146097) * 3) // 4 + C
    e = r * f + v
    g = ( e % p ) // r 
    h = u * g + w
    D = ( h % s ) // u + 1
    M = ( ( h // s + m ) % n ) + 1
    Y = (e // p) - y + (n + m - M) // n
    return (int(Y), int(M) ,int(D))

def mjdtodatetime( modjuldat ):
    Y, M, D = dateofmjd( modjuldat )
    secs = ( modjuldat - math.floor( modjuldat ) ) * 24 * 3600
    h = int( math.floor( secs / 3600 ) )
    m = int( math.floor( ( secs - 3600*h ) / 60 ) )
    s = int( secs - 3600*h - 60*m )
    mus = int( 1e6 * ( secs - 3600*h - 60*m - s ) + 0.5 )
    return datetime.datetime( Y, M, D, h, m, s, mus, tzinfo=datetime.timezone.utc )

    
