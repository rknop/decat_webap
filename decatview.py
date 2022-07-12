import sys
import os
import io
import pathlib
import math
import numbers
import web
import json
import traceback
import base64

import sqlalchemy as sa
import psycopg2.extras

rundir = pathlib.Path(__file__).parent
if not str(rundir) in sys.path:
    sys.path.insert(0, str(rundir) )
import db
import auth
import util

# ======================================================================

def logerr( cls, e ):
    out = io.StringIO()
    traceback.print_exc( file=out )
    sys.stderr.write( out.getvalue() )
    return json.dumps( { "error": f"Exception in {cls}: {e}" } )

# ======================================================================

class HandlerBase:
    def __init__( self ):
        self.response = ""
        self.db = None

    def __del__( self ):
        self.closedb()

    def opendb( self ):
        self.closedb()
        self.db = db.DB.get()

    def closedb( self ):
        if self.db is not None:
            self.db.close()
        self.db = None
        
    def GET( self, *args, **kwargs ):
        self.opendb()
        retval= self.do_the_things( *args, **kwargs )
        self.closedb()
        return retval

    def POST( self, *args, **kwargs ):
        self.opendb()
        retval = self.do_the_things( *args, **kwargs )
        self.closedb()
        return retval

    def verifyauth( self ):
        if ( not hasattr( web.ctx.session, "authenticated" ) ) or ( not web.ctx.session.authenticated ):
            raise RuntimeError( "User not authenticated" )
        
    def jsontop( self ):
        web.header('Content-Type', 'application/json')

    def jsontop_verifyauth( self ):
        self.jsontop()
        self.verifyauth()
    
    def htmltop( self, title=None, h1=None, statusdiv=False, includejsstart=False,
                 jsstart="decatview_start.js", addjs=[] ):
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        webapdirurl = str( pathlib.Path( web.ctx.homepath ).parent )
        # Gah... hate all the special case crap
        if webapdirurl[-1] != '/':
            webapdirurl += "/"
        if title is None:
            title = "DECAT LBL webap"
        if h1 is None:
            h1 = "DECAT LBL Pipeline Candidate Viewer"
        self.response = "<!DOCTYPE html>\n"
        self.response += "<html>\n<head>\n<meta charset=\"UTF-8\">\n"
        self.response += f"<title>{title}</title>\n"
        self.response += f"<link href=\"{webapdirurl}decat.css\" rel=\"stylesheet\" type=\"text/css\">\n"
        self.response += f"<script src=\"{webapdirurl}aes.js\"></script>\n"
        self.response += f"<script src=\"{webapdirurl}jsencrypt.min.js\"></script>\n"
        # self.response += f"<script src=\"{webapdirurl}decatview.js\" type=\"module\"></script>\n"
        for js in addjs:
            self.response += f"<script src=\"{webapdirurl}{js}\"></script>\n"
        if includejsstart:
            self.response += f"<script src=\"{webapdirurl}{jsstart}\" type=\"module\"></script>\n"
        self.response += "</head>\n<body>\n"
        if statusdiv:
            self.htmlstatusdiv()
        if h1 != "":
            self.response += f"<h1>{h1}</h1>\n";

    def htmlbottom( self ):
        self.response += "</body>\n</html>\n"

    def htmlstatusdiv( self ):
        self.response += "<div id=\"status-div\" name=\"status-div\"></div>\n"

# ======================================================================

class FrontPage(HandlerBase):
    def do_the_things( self ):
        self.htmltop( includejsstart=True, statusdiv=True )
        self.response += "<div id=\"main-div\"></div>\n"
        self.htmlbottom()
        return self.response
        

# ======================================================================

class ShowCand(HandlerBase):
    def do_the_things( self, candid ):
        self.htmltop( includejsstart=True, statusdiv=True, jsstart="decatview_showcand_start.js" )
        self.response += f"<input type=\"hidden\" id=\"showcand_candid\" value=\"{candid}\">\n"
        webinput = web.input()
        if "rbtype" in webinput.keys():
            self.response += f"<input type=\"hidden\" id=\"rbtype\" value=\"{webinput.rbtype}\">\n"
        else:
            self.response += "<input type=\"hidden\" id=\"rbtype\" value=\"None\">\n"
        self.response += "<div id=\"main-div\"></div>\n"
        self.htmlbottom()
        return self.response

# ======================================================================

class GetRBTypes(HandlerBase):
    def do_the_things( self ):
        try:
            self.jsontop()
            rbtypes = []
            rbtypedefs = self.db.db.query(db.RBType)
            for rb in rbtypedefs:
                thisrb = {}
                for column in rb.__table__.columns:
                    thisrb[column.name] = getattr( rb, column.name )
                rbtypes.append( thisrb )
            return json.dumps( { "status": "ok",
                                 "rbtypes": rbtypes } )
        except Exception as e:
            return logerr( self.__class__, e )
        

# ======================================================================

class FindExposures(HandlerBase):
    checkpointerrorvalue = 999
    checkpointdonevalue = 27
    checkpointcopyoutvalue = 28
    
    def do_the_things( self ):
        try:
            conn = db.DB._engine.raw_connection()

            self.jsontop()
            data = json.loads( web.data() )
            t0 = util.asDateTime( data['t0'] )
            t1 = util.asDateTime( data['t1'] )

            sql = ( "SELECT e.id AS id,e.filename AS filename,e.filter AS filter,e.proposalid AS proposalid,"
                    "e.header->'EXPTIME' AS exptime,e.ra AS ra,e.dec AS dec,e.gallat AS gallat,"
                    "e.gallong AS gallong,COUNT(DISTINCT s.id) AS numsubs,COUNT(o.id) AS numobjs "
                    "INTO TEMP TABLE temp_find_exposures "
                    "FROM exposures e "
                    "LEFT JOIN subtractions s ON e.id=s.exposure_id "
                    "LEFT JOIN objects o ON o.subtraction_id=s.id " )
            clauses = []
            subs = {}
            if t0 is not None:
                clauses.append( f"e.mjd >= {util.mjd( t0.year, t0.month, t0.day, t0.hour, t0.minute, t0.second )}" )
            if t1 is not None:
                clauses.append( f"e.mjd <= {util.mjd( t1.year, t1.month, t1.day, t1.hour, t1.minute, t1.second )}" )
            if data['maxgallat'] is not None and ( float(data['maxgallat']) < 90 ):
                clauses.append( "(e.gallat <= %(maxgallat)s AND e.gallat >= %(negmaxgallats)) " )
                subs['maxgallat'] = float(data['maxgallat'])
                subs['negmaxgallat'] = -float(data['maxgallat'])
            if data['mingallat'] is not None and ( float(data['mingallat']) > 0. ):
                clauses.append( "(e.gallat >= %(mingallat)s OR e.gallat <= %(negmingallat)s) " )
                subs['mingallat'] = float(data['mingallat'])
                subs['negmingallat'] = -float(data['mingallat'])
            if data['allorsomeprops'] != all:
                clauses.append( "proposalid IN %(props)s" )
                subs['props'] = tuple( data['props'] )
            if len(clauses) > 0:
                sql += "WHERE " + " AND ".join(clauses)
            sql += " GROUP BY e.id,e.filename,e.filter,e.proposalid,e.header,e.ra,e.dec,e.gallat,e.gallong "
            sql += "ORDER BY e.mjd"

            cursor = conn.cursor( cursor_factory=psycopg2.extras.DictCursor )
            cursor.execute( sql, subs )
            cursor.execute( "SELECT * FROM temp_find_exposures" )
            rows = cursor.fetchall()
            # I'm going to depend on dictionaries being ordered here
            exposures = {}
            for row in rows:
                exposures[row['id']] = dict(row)
                exposures[row['id']]['numdone'] = 0
                exposures[row['id']]['numerrors'] = 0

            sql = ( "SELECT e.id AS id,COUNT(p.id) AS numdone "
                    "FROM temp_find_exposures e "
                    "INNER JOIN processcheckpoints p ON p.exposure_id=e.id "
                    "WHERE p.event_id=%(donevalue)s "
                    "GROUP BY e.id")
            subs = { 'donevalue': self.checkpointdonevalue }
            cursor = conn.cursor( cursor_factory=psycopg2.extras.DictCursor )
            cursor.execute( sql, subs )
            rows = cursor.fetchall()
            for row in rows:
                exposures[row['id']]['numdone'] = row['numdone']

            sql = ( "SELECT e.id AS id,COUNT(p.id) AS numcopyout "
                    "FROM temp_find_exposures e "
                    "INNER JOIN processcheckpoints p ON p.exposure_id=e.id "
                    "WHERE p.event_id=%(copyoutvalue)s "
                    "GROUP BY e.id")
            subs = { 'copyoutvalue': self.checkpointcopyoutvalue }
            cursor = conn.cursor( cursor_factory=psycopg2.extras.DictCursor )
            cursor.execute( sql, subs )
            rows = cursor.fetchall()
            for row in rows:
                exposures[row['id']]['numcopyout'] = row['numcopyout']
                
            sql = ( "SELECT e.id AS id,COUNT(p.id) AS numerrors "
                    "FROM temp_find_exposures e "
                    "INNER JOIN processcheckpoints p ON p.exposure_id=e.id "
                    "WHERE p.event_id=%(errorvalue)s "
                    "GROUP BY e.id" )
            subs = { 'errorvalue': self.checkpointerrorvalue }
            cursor = conn.cursor( cursor_factory=psycopg2.extras.DictCursor )
            cursor.execute( sql, subs )
            rows = cursor.fetchall()
            for row in rows:
                exposures[row['id']]['numerrors'] = row['numerrors']

            for rbtype, rbcut in zip( data['rbtypes'], data['rbcuts'] ):
                sql = ( "SELECT e.id AS id,COUNT(o.id) AS numhighrb "
                        "FROM temp_find_exposures e "
                        "INNER JOIN subtractions s ON e.id=s.exposure_id "
                        "INNER JOIN objects o ON s.id=o.subtraction_id "
                        "INNER JOIN objectrbs r ON o.id=r.object_id "
                        "WHERE r.rbtype_id=%(rbtype)s AND r.rb>=%(rbcut)s "
                        "GROUP BY e.id" )
                subs = { 'rbtype': rbtype, 'rbcut': rbcut }
                sys.stderr.write( f"Sending query: {sql}\n" )
                sys.stderr.write( f"Subs: {subs}\n" )
                cursor = conn.cursor( cursor_factory=psycopg2.extras.DictCursor )
                cursor.execute( sql, subs )
                rows = cursor.fetchall()
                sys.stderr.write( f"Got {len(rows)} results\n" )
                for row in rows:
                    exposures[row['id']][f"numhighrb{rbtype}"] = row['numhighrb']

            return json.dumps( { "status": "ok",
                                 "exposures": [ val for key, val in exposures.items() ] } )

            # errorchk = sa.orm.aliased( db.ProcessCheckpoint )
            # donechk = sa.orm.aliased( db.ProcessCheckpoint )
            # q = self.db.db.query( db.Exposure, sa.func.count(sa.distinct(db.Subtraction.id)),
            #                       sa.func.count(sa.distinct(errorchk.id)),
            #                       sa.func.count(sa.distinct(db.Object.id)),
            #                       sa.func.count(sa.distinct(db.ObjectRB.id)),
            #                       sa.func.count(sa.distinct(donechk.id)) )
            # q = q.join( db.Subtraction, db.Exposure.id==db.Subtraction.exposure_id, isouter=True )
            # q = q.join( errorchk, sa.and_( db.Exposure.id==errorchk.exposure_id,
            #                                errorchk.event_id==self.checkpointerrorvalue ),
            #             isouter=True )
            # q = q.join( donechk, sa.and_( db.Exposure.id==donechk.exposure_id,
            #                               donechk.event_id==self.checkpointdonevalue ),
            #             isouter=True )
            # q = q.join( db.Object, db.Subtraction.id==db.Object.subtraction_id, isouter=True )
            # q = q.join( db.ObjectRB, sa.and_( db.ObjectRB.object_id==db.Object.id,
            #                                   db.ObjectRB.rbtype_id==data['rbtype'],
            #                                   db.ObjectRB.rb>=data['rbcut'] ), isouter=True )
            # if t0 is not None:
            #     q = q.filter( db.Exposure.mjd >= util.mjd( t0.year, t0.month, t0.day, t0.hour, t0.minute, t0.second ) )
            # if t1 is not None:
            #     q = q.filter( db.Exposure.mjd <= util.mjd( t1.year, t1.month, t1.day, t1.hour, t1.minute, t1.second ) )
            # if ( data['maxgallat'] is not None ) and ( float(data['maxgallat']) < 90 ):
            #     q = q.filter( sa.and_( db.Exposure.gallat <= float(data['maxgallat']),
            #                            db.Exposure.gallat >= -float(data['maxgallat']) ) )
            # if ( data['mingallat'] is not None ) and ( float(data['mingallat']) > 0 ):
            #     q = q.filter( sa.or_( db.Exposure.gallat >= float(data['mingallat']),
            #                           db.Exposure.gallat <= -float(data['mingallat']) ) );
            # if data['allorsomeprops'] != 'all':
            #     q = q.filter( db.Exposure.proposalid.in_( data['props'] ) )
            # q = q.group_by( db.Exposure )
            # q = q.order_by( db.Exposure.mjd )
            # res = q.all()
            # exps = []
            # for row in res:
            #     exposure = row[0]
            #     exps.append( { 'id': exposure.id,
            #                    'filename': exposure.filename,
            #                    'filter': exposure.filter,
            #                    'proposalid': exposure.proposalid,
            #                    'exptime': exposure.header['EXPTIME'],
            #                    'ra': exposure.ra,
            #                    'dec': exposure.dec,
            #                    'gallat': exposure.gallat,
            #                    'gallong': exposure.gallong,
            #                    'numsubs': row[1],
            #                    'numerrors': row[2],
            #                    'numobjs': row[3],
            #                    'numhighrbobjs': row[4],
            #                    'numdone': row[5] } )
            # return json.dumps( { "status": "ok",
            #                      "exposures": exps } )
        except Exception as e:
            return logerr( self.__class__, e )
        finally:
            conn.close()

# ======================================================================

class CheckpointDefs(HandlerBase):
    def do_the_things( self ):
        try:
            self.jsontop()
            q = self.db.db.query(db.CheckpointEventDef).order_by(db.CheckpointEventDef.id)
            results = q.all()
            res = {}
            for row in results:
                res[row.id] = row.description
            return json.dumps( res )
        except Exception as e:
            return logerr( self.__class__, e )

# ======================================================================

class ExposureLog(HandlerBase):
    def do_the_things( self, expid ):
        try:
            self.jsontop()
            q = ( self.db.db.query(db.ProcessCheckpoint)
                  .filter( db.ProcessCheckpoint.exposure_id==expid )
                  .order_by( db.ProcessCheckpoint.created_at, db.ProcessCheckpoint.ccdnum ) )
            results = q.all()
            res = { "status": "ok",
                    "checkpoints": [] }
            for chkpt in results:
                res["checkpoints"].append(
                    { "id": chkpt.id,
                      "exposure_id": chkpt.exposure_id,
                      "ccdnum": chkpt.ccdnum,
                      "created_at": chkpt.created_at.isoformat(),
                      "event_id": chkpt.event_id,
                      "running_node": chkpt.running_node,
                      "notes": chkpt.notes,
                      "mpi_rank": chkpt.mpi_rank } )
            return json.dumps( res )
        except Exception as e:
            return logerr( self.__class__, e )


# ======================================================================

class Cutouts(HandlerBase):
    def get_cutouts( self, expid=None, candid=None, sort="rb", rbtype=None, offset=None, limit=None,
                     mingallat=None, maxgallat=None, onlyvetted=False, proposals=None, notvettedby=None ):
        q = ( self.db.db.query( db.Object, db.Subtraction.ccdnum, db.Subtraction.magzp,
                                db.Exposure.filename, db.Exposure.mjd, db.Exposure.proposalid,db.Exposure.filter )
              .join( db.Subtraction, db.Subtraction.id==db.Object.subtraction_id )
              .join( db.Exposure, db.Exposure.id==db.Subtraction.exposure_id ) )
        if onlyvetted:
            # Just joining should limit this, as it's an inner join
            q = q.join( db.ScanScore, db.ScanScore.object_id==db.Object.id )
            if notvettedby is not None:
                # This won't omit things already vetted by the user, it just makes sure
                #  to get things that have been vetted by somebody else
                q = q.filter( db.ScanScore.username != notvettedby )

        if expid is not None:
            q = q.filter( db.Exposure.id == expid )
        elif candid is not None:
            q = q.filter( db.Object.candidate_id == candid )

        if proposals is not None:
            q = q.filter( db.Exposure.proposalid.in_( proposals ) )

        if mingallat is not None:
            q = q.filter( sa.or_( db.Exposure.gallat >= mingallat, db.Exposure.gallat <= -mingallat ) )
        if maxgallat is not None:
            q = q.filter( sa.and_( db.Exposure.gallat <= maxgallat, db.Exposure.gallat >= -maxgallat ) )

        if onlyvetted:
            # Throw this in to avoid getting duplicate objects
            q = q.group_by( db.Object, db.Subtraction.ccdnum, db.Subtraction.magzp, db.Exposure.filename,
                            db.Exposure.mjd, db.Exposure.proposalid, db.Exposure.filter )
            
        if sort == "random":
            q = q.order_by( sa.func.random() )
            if limit is not None:
                q = q.limit( limit )

        objres = q.all()
        totnobjs = len(objres)

        objids =[]
        objs = {}
        for row in objres:
            obj, ccdnum, zp, filename, mjd, propid, band = row
            objids.append( obj.id )
            objs[ obj.id ] = {
                "object_id": obj.id,
                "ra": obj.ra,
                "dec": obj.dec,
                "candid": obj.candidate_id,
                "filename": filename,
                "mjd": mjd,
                "proposalid": propid,
                "zp": zp,
                "ccdnum": ccdnum,
                "filter": band,
                "flux": obj.flux,
                "fluxerr": obj.fluxerr,
                "mag": obj.mag,
                "magerr": obj.magerr,
                "rb": None,
                "sci_jpeg": None,
                "ref_jpeg": None,
                "diff_jpeg": None
            }

        # Get RBs and sort objects
        if rbtype is not None:
            q = ( self.db.db.query( db.ObjectRB )
                  .filter( db.ObjectRB.rbtype_id==rbtype )
                  .filter( db.ObjectRB.object_id.in_( objids ) ) )
            res = q.all()
            for rb in res:
                objs[ rb.object_id ]['rb'] = rb.rb
        if sort == "rb":
            objids.sort( key=lambda i: ( 9999 if objs[i]['rb'] is None else -objs[i]['rb'], i ) )
        elif sort == "mjd":
            objids.sort( key=lambda i: objs[i]['mjd'] )
        elif sort == "random":
            # Already sorted
            pass
        else:
            raise ValueError( f"Unknown sort scheme {sort}" )

        # Trim if requested.  (If random, already trimmed.)
        start = 0
        end = totnobjs
        if sort != "random":
            if offset is not None:
                start = min( totnobjs, max( offset, 0 ) )
            if limit is not None:
                end = min( totnobjs, start + limit )
        objids = objids[start:end]

        # Get cutouts for trimmed list
        q = ( self.db.db.query( db.Cutout )
              .filter( db.Cutout.object_id.in_( objids ) ) )
        cutouts = q.all()
        for cutout in cutouts:
            objs[cutout.object_id]["sci_jpeg"] = base64.b64encode(cutout.sci_jpeg).decode('ascii')
            objs[cutout.object_id]["ref_jpeg"] = base64.b64encode(cutout.ref_jpeg).decode('ascii')
            objs[cutout.object_id]["diff_jpeg"] = base64.b64encode(cutout.diff_jpeg).decode('ascii')

        results = { "totnobjs": totnobjs,
                    "offset": start,
                    "num": end-start,
                    "objs": [ objs[i] for i in objids  ] }
        return results
        

class CutoutsForExposure(Cutouts):
    def do_the_things( self, expid ):
        try:
            self.jsontop()
            data = json.loads( web.data() )
            rbtype = data["rbtype"] if "rbtype" in data else None
            limit = data["limit"] if "limit" in data else None
            offset = data["offset"] if "offset" in data else None
            return json.dumps( self.get_cutouts( expid=expid, sort="rb", rbtype=rbtype, offset=offset, limit=limit ) )
        except Exception as e:
            return logerr( self.__class__, e )
            
class CutoutsForCandidate(Cutouts):
    def do_the_things( self, candid ):
        try:
            self.jsontop()
            data = json.loads( web.data() )
            rbtype = data["rbtype"] if "rbtype" in data else None
            results = self.get_cutouts( candid=candid, rbtype=rbtype, sort="mjd" )
            q = self.db.db.query( db.Candidate ).filter( db.Candidate.id==candid )
            cand = q.all()[0]
            results["candra"] = cand.ra
            results["canddec"] = cand.dec
            return json.dumps( results )
        except Exception as e:
            return logerr( self.__class__, e )
            
# ======================================================================
# ROB WORRY ABOUT TIME ZONES
                
class SearchCandidates(HandlerBase):
    def do_the_things( self ):
        try:
            nfound = -1
            conn = db.DB._engine.raw_connection()
            
            sys.stderr.write( "Starting SearchCandidates\n" )
            self.jsontop()
            data = json.loads( web.data() )
            # sys.stderr.write( f"Data is: {data}" )
            
            subs = {}

            # First query : get all candidates that are detected
            #   according between startdate and enddate with
            #   at least the minimum S/N (if requested), considering
            #   only high-r/b objects (if requested), also using
            #   gallat and ra/dec cuts, and proposal cuts
            
            query = ( "SELECT DISTINCT o.candidate_id AS id "
                      "INTO TEMP TABLE temp_findcands "
                      "FROM objects o "
                      "INNER JOIN subtractions s ON o.subtraction_id=s.id "
                      "INNER JOIN exposures e ON s.exposure_id=e.id " )
            if data['useuserbcut']:
                query += ( "INNER JOIN objectrbs r ON o.id=r.object_id "
                           "INNER JOIN rbtypes t ON r.rbtype_id=t.id " )
            conds = []
            if data['allorsome'] != "all":
                conds.append( f"e.proposalid IN %(props)s" )
                subs['props'] = tuple( data['proposals'] )
            if data['usestartdate']:
                conds.append( f"e.mjd>=%(startdate)s" )
                d = util.asDateTime( data['startdate'] )
                subs['startdate'] = util.mjd( d.year, d.month, d.day, d.hour, d.minute, d.second )
            if data['useenddate']:
                conds.append( f"e.mjd<=%(enddate)s" )
                d = util.asDateTime( data['enddate'] )
                subs['enddate'] = util.mjd( d.year, d.month, d.day, d.hour, d.minute, d.second )
            if data['usegallatmin']:
                conds.append( f"( e.gallat>%(gallatmin)s OR e.gallat<%(neggallatmin)s )" )
                subs['gallatmin'] = data['gallatmin']
                subs['neggallatmin'] = -float( data['gallatmin'] )
            if data['usegallatmax']:
                conds.append( f"( e.gallat<%(gallatmax)s AND e.gallat>%(neggallatmax)s )" )
                subs['gallatmax'] = data['gallatmax']
                subs['neggallatmax'] = -float( data['gallatmax'] )
            if data['usera']:
                conds.append( f"q3c_radial_query(o.ra,o.dec,%(ra)s,%(dec)s,%(radius)s)" )
                subs['ra'] = data['ra']
                subs['dec'] = data['dec']
                subs['radius'] = data['radius']
            if data['useuserbcut']:
                conds.append( "t.id=%(rbtype)s AND r.rb>=t.rbcut " )
            if data['usesncut']:
                conds.append( "(o.flux/o.fluxerr)>%(sncut)s " )
            if len( conds ) > 0:
                query += "WHERE " + " AND ".join( conds )
            else:
                raise RuntimeError( "You just asked *all candidates*.  You do not want to do that." )
            subs['rbtype'] = data['rbtype']
            subs['sncut'] = data['sncut']
            
            sys.stderr.write( f'Starting massive object finding query\n' )
            # sys.stderr.write( f'{str(saq)}\n' )
            # sys.stderr.write( f'subs: {subs}\n' )
            cursor = conn.cursor( )
            cursor.execute( query, subs )
            cursor.execute( "SELECT COUNT(*) FROM temp_findcands" )
            res = cursor.fetchone()
            ninitiallyfound = res[0]
            sys.stderr.write( f'Done with massive object finding query; found {res[0]} candidates.\n' )

            # Second and third queries: count the number of objects and high r/b objects
            # within the date range

            dateconds = []
            if data['usestartcount']:
                d = util.asDateTime( data['startcount'] )
                dateconds.append( "e.mjd>=%(startcount)s" )
                subs['startcount'] = util.mjd( d.year, d.month, d.day, d.hour, d.minute, d.second )
            elif data['usestartdate']:
                dateconds.append( "e.mjd>=%(startdate)s" )
            if data['useendcount']:
                d = util.asDateTime( data['endcount'] )
                dateconds.append( "e.mjd<=%(endcount)s" )
                subs['endcount'] = util.mjd( d.year, d.month, d.day, d.hour, d.minute, d.second )
            elif data['useenddate']:
                dateconds.append( "e.mjd<=%(enddate)s" )

            query = ( "SELECT c.id AS id,COUNT(o.id) AS numhighrb,COUNT(DISTINCT e.filter) AS highrbfiltcount,"
                      "  MIN(e.mjd) AS highrbminmjd,MAX(e.mjd) AS highrbmaxmjd,"
                      "  MIN(o.mag) AS highrbminmag,MAX(o.mag) AS highrbmaxmag "
                      "INTO temp_filtercands "
                      "FROM temp_findcands c "
                      "INNER JOIN objects o ON o.candidate_id=c.id "
                      "INNER JOIN objectrbs r ON o.id=r.object_id "
                      "INNER JOIN rbtypes t ON r.rbtype_id=t.id "
                      "INNER JOIN subtractions s ON o.subtraction_id=s.id "
                      "INNER JOIN exposures e ON s.exposure_id=e.id " )
            query += "WHERE t.id=%(rbtype)s AND r.rb>=t.rbcut "
            if len(dateconds) > 0:
                query += " AND " + ( " AND ".join(dateconds) )
            if data['usesncut']:
                query += " AND (o.flux/o.fluxerr)>%(sncut)s "
            query += " GROUP BY c.id"

            sys.stderr.write( "Starting high rb count query\n" )
            cursor = conn.cursor()
            cursor.execute( query, subs )
            # Do this to simplify grouping later
            cursor.execute( "ALTER TABLE temp_filtercands ADD PRIMARY KEY (id)" )
            cursor.execute( "SELECT COUNT(*) FROM temp_filtercands" )
            res = cursor.fetchone()
            nhighrb = res[0]
            sys.stderr.write( f"Done with high rb count query, {res[0]} rows in table.\n" )
            
            query = ( "SELECT c.*,COUNT(o.id) AS numhighsn,COUNT(DISTINCT e.filter) AS filtcount,"
                      " MIN(e.mjd) AS highsnminmjd,MAX(e.mjd) AS highsnmaxmjd,"
                      " MIN(o.mag) AS highsnminmag,MAX(o.mag) AS highsnmaxmag "
                      "INTO temp_filtercands2 "
                      "FROM temp_filtercands c "
                      "INNER JOIN objects o ON o.candidate_id=c.id "
                      "INNER JOIN subtractions s ON o.subtraction_id=s.id "
                      "INNER JOIN exposures e ON s.exposure_id=e.id " )
            conds = []
            if len( dateconds ) > 0:
                conds.extend( dateconds )
            if data['usesncut']:
                conds.append( "(o.flux/o.fluxerr)>%(sncut)s" )
            if len( conds ) > 0:
                query += " WHERE " + ( " AND ".join(conds) )
            query += " GROUP BY c.id"

            sys.stderr.write( "Starting object (maybe high s/n) count query\n" )
            cursor = conn.cursor()
            cursor.execute( query, subs )
            # Do this to simplify grouping later
            cursor.execute( "ALTER TABLE temp_filtercands2 ADD PRIMARY KEY (id)" )
            cursor.execute( "SELECT COUNT(*) FROM temp_filtercands2" )
            res = cursor.fetchone()
            nobjcount = res[0]
            sys.stderr.write( f"Object count query done, {res[0]} rows in table.\n" )

            # Fourth query: filter this list
            query = "SELECT * INTO temp_filtercands3 FROM temp_filtercands2 c "
            conds = []
            if data['usediffdays']:
                conds.append( 'highrbmaxmjd-highrbminmjd>=%(diffdays)s' )
                subs['diffdays'] = data['diffdays']
            if data['usebrightest']:
                conds.append( "highrbminmag>=%(brightest)s" )
                subs['brightest'] = data['brightest']
            if data['usedimmest']:
                conds.append( "highrbmaxmag<=%(dimmest)s" )
                subs['dimmest'] = data['dimmest']
            if data['usenumdets']:
                conds.append( "numhighsn>=%(numdets)s" )
                subs['numdets'] = data["numdets"]
            if data['usehighrbdets']:
                conds.append( "numhighrb>=%(numhighrb)s" )
                subs['numhighrb'] = data['highrbdets']
            if data['usenumfilters']:
                conds.append( "filtcount>=%(numfilters)s" )
                subs['numfilters'] = data['numfilters']
            if len(conds) > 0:
                query += "WHERE " + ( " AND ".join( conds ) )
            query += " ORDER BY c.id"

            sys.stderr.write( "Starting filter query\n" )
            cursor = conn.cursor()
            cursor.execute( query, subs )
            cursor.execute( "ALTER TABLE temp_filtercands3 ADD PRIMARY KEY (id)" )
            cursor.execute( "SELECT COUNT(*) FROM temp_filtercands3" )
            res = cursor.fetchone()
            ndatemagrbsnfiltered = res[0]
            sys.stderr.write( f"Filter query done, {res[0]} candidates remain.\n" )
            
            # POSSIBLE fifth and sixth queries.  Reject candidates with too many detections *outside* the filtered
            #   range.

            noutsidedate = None
            if ( ( data["usestartcount"] or data["useendcount"] )
                 and
                 ( data["useoutsidehighrbdets"] or data["useoutsidedets"] )
            ):
                outdateconds = []
                if data['usestartcount']:
                    d = util.asDateTime( data['startcount'] )
                    outdateconds.append( "e.mjd<%(startcount)s" )
                if data['useendcount']:
                    d = util.asDateTime( data['endcount'] )
                    outdateconds.append( "e.mjd>%(endcount)s" )
                outdateconds = "(" + ( " OR ".join( outdateconds ) ) + ")"

                sys.stderr.write( "Starting outside-date-range rejection filter\n" )
                
                if data["useoutsidehighrbdets"]:
                    query = ( "SELECT c.id,COUNT(o.id) AS highrboutside "
                              "INTO temp_filteroutdate1 "
                              "FROM temp_filtercands3 c "
                              "INNER JOIN objects o ON c.id=o.candidate_id "
                              "INNER JOIN subtractions s ON o.subtraction_id=s.id "
                              "INNER JOIN exposures e ON s.exposure_id=e.id "
                              "INNER JOIN objectrbs r ON o.id=r.object_id "
                              "INNER JOIN rbtypes t ON r.rbtype_id=t.id "
                              f"WHERE {outdateconds} "
                              "AND t.id=%(rbtype)s AND r.rb>=t.rbcut " )
                    if data['usesncut']:
                        query += " AND (o.flux/o.fluxerr)>%(sncut)s "
                    query += "GROUP BY c.id"
                else:
                    query = ( "SELECT c.id,0 AS highrboutside INTO temp_filteroutdate1 FROM temp_filtercands3 c" )
                    
                cursor = conn.cursor()
                cursor.execute( query, subs )

                if data["useoutsidedets"]:
                    query = ( "SELECT c.id,COUNT(o.id) AS numoutside "
                              "INTO temp_filteroutdate2 "
                              "FROM temp_filteroutdate1 c "
                              "INNER JOIN objects o ON c.id=o.candidate_id "
                              "INNER JOIN subtractions s ON o.subtraction_id=s.id "
                              "INNER JOIN exposures e ON s.exposure_id=e.id "
                              f"WHERE {outdateconds} " )
                    if data['usesncut']:
                        query += " AND (o.flux/o.fluxerr)>%(sncut)s "
                    query += "GROUP BY c.id"
                else:
                    query = ( "SELECT c.id,c.highrboutside,0 AS numoutside "
                              "INTO temp_filteroutdate2 FROM temp_filteroutdate1 c" )

                cursor = conn.cursor()
                cursor.execute( query, subs )

                query = ( "SELECT c.* "
                          "INTO temp_filtercands4 "
                          "FROM temp_filtercands3 c "
                          "INNER JOIN temp_filteroutdate2 od ON c.id=od.id " )
                conds = []
                if data["useoutsidehighrbdets"]:
                    conds.append( "highrboutside<=%(highrboutside)s" )
                    subs['highrboutside'] = data['outsidehighrbdets']
                if data["useoutsidedets"]:
                    conds.append( "numoutside<=%(numoutside)s" )
                    subs['numoutside'] = data['outsidedets']
                query += "WHERE " + ( " AND ".join( conds ) )

                cursor = conn.cursor()
                cursor.execute( query, subs )
                cursor.execute( "ALTER TABLE temp_filtercands4 ADD PRIMARY KEY (id)" )
                cursor.execute( "SELECT COUNT(*) FROM temp_filtercands4" )
                res = cursor.fetchone()
                noutsidedate = res[0]
                sys.stderr.write( f"Outside-date-rejection filter done, {res[0]} candidates remain.\n" )
            else:
                query = ( "ALTER TABLE temp_filtercands3 RENAME TO temp_filtercands4" )
                cursor.execute( query )
                
            # Last query: full counts and pull
            
            sys.stderr.write( "Starting final query to pull data\n" )
            query = ( "SELECT c.*,COUNT(o.id) AS totnobjs, "
                      "  MAX(e.mjd) AS totmaxmjd,MIN(e.mjd) AS totminmjd, "
                      "  MAX(o.mag) AS totmaxmag,MIN(o.mag) AS totminmag "
                      "FROM temp_filtercands4 c "
                      "INNER JOIN objects o ON c.id=o.candidate_id "
                      "INNER JOIN subtractions s ON s.id=o.subtraction_id "
                      "INNER JOIN exposures e ON e.id=s.exposure_id "
                      "GROUP BY c.id ORDER BY c.id" )
            cursor = conn.cursor( cursor_factory=psycopg2.extras.DictCursor )
            cursor.execute( query, subs )
            rows = cursor.fetchall()
            rows = [ dict(row) for row in rows ]

            # AUGH.  JSON makes NaN a pain in the butt
            for row in rows:
                for key, val in row.items():
                    if isinstance( val, numbers.Number ) and math.isnan( val ):
                        row[key] = None
            
            nfound = len(rows)
            # with open( "/sessions/test.txt", "w" ) as ofp:
            #     ofp.write( json.dumps( list( rows ) ) )
            return json.dumps( {
                'ninitiallyfound': ninitiallyfound,
                'nhighrb': nhighrb,
                'nobjcount': nobjcount,
                'ndatemagrbsnfiltered': ndatemagrbsnfiltered,
                'noutsidedate': noutsidedate,
                'n': nfound,
                'candidates': list( rows ) } )
        except Exception as e:
            return logerr( self.__class__, e )
        finally:
            conn.close()
            sys.stderr.write( f'All done with candidate finding, for better or worse; nfound={nfound}\n' )
                
# ======================================================================

class GetObjectsToVet(Cutouts):
    def do_the_things( self ):
        try:
            self.jsontop()
            if not web.ctx.session.authenticated:
                raise PermissionError( "Must log in to vet objects" )
            
            data = json.loads( web.data() )

            if data[ "galorexgal" ] == "Galactic":
                mingallat = None
                maxgallat = 20
            else:
                mingallat = 20
                maxgallat = None
            onlyvetted = data[ "alreadyvetted" ] == "Yes"

            # Get objects and cutouts
            results = self.get_cutouts( sort="random", limit=100, mingallat=mingallat, maxgallat=maxgallat,
                                        onlyvetted=onlyvetted, notvettedby=web.ctx.session.username )
            # Get user's current vet status
            q = ( self.db.db.query( db.ScanScore )
                  .filter( db.ScanScore.object_id.in_( [ i["object_id"] for i in results["objs"] ] ) )
                  .filter( db.ScanScore.username==web.ctx.session.username ) )
            them = q.all()
            score = {}
            for row in them:
                score[row.object_id] = row.goodbad

            for obj in results["objs"]:
                obj["goodbad"] = score[obj["object_id"]] if obj["object_id"] in score else "unset"
            
            return json.dumps( results )
        except Exception as e:
            return logerr( self.__class__, e )

# ======================================================================

class SetGoodBad(Cutouts):
    def do_the_things( self ):
        try:
            self.jsontop()
            if not web.ctx.session.authenticated:
                raise PermissionError( "Must login in to vet objects" )
            username = web.ctx.session.username
            data = json.loads( web.data() )

            gbs = {}
            ssids = []
            res = []
            for oid, gb in zip( data['object_ids'], data['goodbads'] ):
                if ( gb != "good" ) and ( gb != "bad" ):
                    # sounds like ValueJudgementError
                    raise ValueError( f'Status {gb} is neither good nor bad' )
                gbs[oid] = gb
                ssids.append( f"{username}_{oid}" )
            mustmakenew = set( data['object_ids'] )

            q = self.db.db.query( db.ScanScore ).filter( db.ScanScore.id.in_( ssids ) )
            scanscores = q.all()
            for onescore in scanscores:
                mustmakenew.remove( onescore.object_id )
                onescore.goodbad = gbs[ onescore.object_id ]
                res.append( { 'object_id': onescore.object_id, 'goodbad': gbs[ onescore.object_id ] } )

            for oid in mustmakenew:
                ssid = f'{username}_{oid}'
                newscore = db.ScanScore( id=ssid, object_id=oid, username=username, goodbad=gbs[oid] )
                self.db.db.add( newscore )
                res.append( { 'object_id': oid, 'goodbad': gbs[ oid ] } )

            self.db.db.commit()
            
            return json.dumps( res )
        except Exception as e:
            return logerr( self.__class__, e )

# ======================================================================

urls = (
    '/', "FrontPage",
    '/cand/(.+)', "ShowCand",
    '/getrbtypes', "GetRBTypes",
    '/findexposures', "FindExposures",
    '/checkpointdefs', "CheckpointDefs",
    '/exposurelog/(.+)', "ExposureLog",
    '/cutoutsforexp/(.+)', "CutoutsForExposure",
    '/cutoutsforcand/(.+)', "CutoutsForCandidate",
    '/searchcands', "SearchCandidates",
    '/getobjstovet', "GetObjectsToVet",
    '/setgoodbad', "SetGoodBad",
    "/auth", auth.app
    )

app = web.application( urls, locals() )
web.config.session_parameters["samesite"] = "lax"

# ROB MAKE THIS CONFIGURABLE
web.config.smtp_server = 'smtp.lbl.gov'
web.config.smtp_port = 25

initializer = {}
initializer.update( auth.initializer )
session = web.session.Session( app, web.session.DiskStore( "/sessions" ), initializer=initializer )
def session_hook(): web.ctx.session = session
app.add_processor( web.loadhook( session_hook ) )

application = app.wsgifunc()

# ======================================================================
# This won't be run from within apache, but it's here for a smoke test

def main():
    global app
    sys.stderr.write( "Running webapp.\n" )
    sys.stderr.flush()
    app.run()

if __name__ == "__main__":
    main()
