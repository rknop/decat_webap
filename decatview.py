import sys
import os
import io
import pathlib
import math
import numbers
import collections.abc
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

class GetCameraInfo(HandlerBase):
    def do_the_things( self, telescope, camera ):
        try:
            self.jsontop()
            cams = ( self.db.db.query( db.Camera )
                     .filter( db.Camera.telescope==telescope )
                     .filter( db.Camera.name==camera ) ).all()
            if len( cams ) == 0:
                return json.dumps( { "error": f"Unknown camera {camera} (telescope {telescope})" } )
            cam = cams[0]
            resp = { "status": "ok",
                     "id": cam.id,
                     "nx": cam.nx,
                     "ny": cam.ny,
                     "orientation": cam.orientation,
                     "pixscale": cam.pixscale,
                     "telescope": cam.telescope,
                     "name": cam.name,
                     "chips": [] }
            # I can't do this becasue I don't have the full db, just a reflection thingy,
            #  and I have no clue how the reflection thingy will put in relationships
            # for cc in cam.chips:
            ccs = self.db.db.query( db.CameraChip ).filter( db.CameraChip.camera_id==cam.id )
            for cc in ccs:
                thiscc = { column.name: getattr( cc, column.name ) for column in cc.__table__.columns }
                resp["chips"].append( thiscc )
            resp["chips"].sort( key=lambda cc: cc["chipnum"] )
            return json.dumps( resp )
        except Exception as e:
            return logerr( self.__class__, e )

# ======================================================================

class GetCameraChips(HandlerBase):
    def do_the_things( self, camid ):
        try:
            self.jsontop()
            camerachips = []
            ccs = ( self.db.db.query( db.CameraChip )
                    .filter( db.CameraChip.camera_id==camid )
                    .order_by( db.CameraChip.chipnum ) )
            for cc in ccs:
                thiscc = {}
                for column in cc.__table__.columns:
                    thiscc[column.name] = getattr( cc, column.name )
                camerachips.append( thiscc )
            return json.dumps( { "status": "ok",
                                 "camera_id": camid,
                                 "camerachips": camerachips } )
        except Exception as e:
            return logerr( self.__class__, e )

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

class GetVersionTags(HandlerBase):
    def do_the_things( self ):
        try:
            self.jsontop()
            versiontags = []
            vts = self.db.db.query( db.VersionTag ).order_by( db.VersionTag.id )
            for vt in vts:
                versiontags.append( {'id': vt.id,
                                     'tag': vt.tag,
                                     'description': vt.description } )
            return json.dumps( { "status": "ok",
                                 "versiontags": versiontags } )
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
            versiontag = data[ 'versiontagid' ]

            sys.stderr.write( "Finding exposures...\n" )

            sql = ( "SELECT e.id AS id,e.filename AS filename,e.proposalid AS proposalid,"
                    "e.header->'EXPTIME' AS exptime,e.ra AS ra,e.dec AS dec,e.gallat AS gallat,"
                    "e.gallong AS gallong "
                    "INTO TEMP TABLE temp_find_exposures "
                    "FROM exposures e " )
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
            sql += "ORDER BY e.mjd"

            cursor = conn.cursor( cursor_factory=psycopg2.extras.DictCursor )
            cursor.execute( sql, subs )
            cursor.execute( "SELECT * FROM temp_find_exposures" )
            rows = cursor.fetchall()
            # I'm going to depend on dictionaries being ordered here
            exposures = {}
            for row in rows:
                exposures[row['id']] = dict(row)
                exposures[row['id']]['filter'] = 'unknown'
                exposures[row['id']]['numimages'] = 0
                exposures[row['id']]['numsubs'] = 0
                exposures[row['id']]['numdone'] = 0
                exposures[row['id']]['numobjs'] = 0
                exposures[row['id']]['numcopyout'] = 0
                exposures[row['id']]['numerrors'] = 0

            sys.stderr.write( "...counting images...\n" )

            sql = ( "SELECT t.id,i.filter,COUNT(DISTINCT i.id) AS nimages "
                    "FROM temp_find_exposures t "
                    "INNER JOIN images i ON i.exposure_id=t.id "
                    "GROUP BY t.id,i.filter " )
            cursor.execute( sql )
            rows = cursor.fetchall()
            for row in rows:
                exposures[row['id']]['filter'] = row['filter']
                exposures[row['id']]['numimages'] = row['nimages']

            sys.stderr.write( "...counting subtractions & done...\n" )

            # Doing this in a separate query so that we will count images
            # that don't have subtractions.  (Could I have done this in one
            # query with a LEFT JOIN on subtractions?)
            sql = ( "SELECT t.id,COUNT(DISTINCT s.id) AS nsubs,COUNT(c.id) AS ndone,COUNT(DISTINCT c2.id) AS ncopy "
                    "FROM temp_find_exposures t "
                    "INNER JOIN images i ON i.exposure_id=t.id "
                    "INNER JOIN subtractions s ON s.image_id=i.id AND s.complete=TRUE "
                    "INNER JOIN subtraction_versiontag v ON v.subtraction_id=s.id AND v.versiontag_id=%(versiontag)s "
                    "LEFT JOIN processcheckpoints c ON c.subtraction_id=s.id AND c.event_id=%(done)s "
                    "LEFT JOIN processcheckpoints c2 ON c2.exposure_id=t.id AND c2.event_id=%(copy)s "
                    "GROUP BY t.id " )
            cursor.execute( sql, { "versiontag": versiontag,
                                   "done": self.checkpointdonevalue,
                                   "copy": self.checkpointcopyoutvalue } )
            rows = cursor.fetchall()
            for row in rows:
                exposures[row['id']]['numsubs'] = row['nsubs']
                exposures[row['id']]['numdone'] = row['ndone']
                exposures[row['id']]['numcopyout'] = row['ncopy']

            sys.stderr.write( "...counting objectdatas...\n" )

            # And yet another query so that in the previous query we would
            # have counted subtractions that don't have any objects.
            # (The inner join would have gotten rid of that, I think.)
            sql = ( "SELECT t.id,COUNT(o.id) AS nobjs "
                    "FROM temp_find_exposures t "
                    "INNER JOIN images i ON i.exposure_id=t.id "
                    "INNER JOIN subtractions s ON s.image_id=i.id "
                    "INNER JOIN objectdatas o ON o.subtraction_id=s.id "
                    "INNER JOIN objectdata_versiontag v ON v.objectdata_id=o.id "
                    "WHERE v.versiontag_id=%(versiontag)s "
                    "GROUP BY t.id " )
            cursor.execute( sql, { "versiontag": versiontag } )
            rows = cursor.fetchall()
            for row in rows:
                exposures[row['id']]['numobjs'] = row['nobjs']

            sys.stderr.write( "...counting high rb...\n" )

            # ...and yet another...
            for rbtype, rbcut in zip( data['rbtypes'], data['rbcuts'] ):
                sql = ( "SELECT t.id,COUNT(o.id) AS nhighrb "
                        "FROM temp_find_exposures t "
                        "INNER JOIN images i ON i.exposure_id=t.id "
                        "INNER JOIN subtractions s ON s.image_id=i.id "
                        "INNER JOIN objectdatas o ON o.subtraction_id=s.id "
                        "INNER JOIN objectdata_versiontag v ON v.objectdata_id=o.id "
                        "INNER JOIN objectrbs r ON r.objectdata_id=o.id "
                        "WHERE v.versiontag_id=%(versiontag)s AND r.rbtype_id=%(rbtype)s AND r.rb>=%(rbcut)s "
                        "GROUP BY t.id " )
                cursor.execute( sql, { "versiontag": versiontag, "rbtype": rbtype, "rbcut": rbcut } )
                rows = cursor.fetchall()
                for row in rows:
                    exposures[row['id']][f'numhighrb{rbtype}'] = row['nhighrb']

            sys.stderr.write( "...counting errors...\n" )

            # ...finally, count errors; do this in three passes for sanity...
            # (I first tried to be all fancy with left joins, but was getting
            # duplicates, so threw my hands up and said whatever.)

            # Later, I think I've figured it out:
            # SELECT filename, terrors
            # FROM ( SELECT COALESCE(subqe.filename,subqi.filename,subqs.filename) as filename,
            #               COALESCE(enerrors,0)+COALESCE(inerrors,0)+COALESCE(snerrors,0) AS terrors
            #        FROM ( SELECT e.filename,COUNT(p.id) AS enerrors
            #               FROM exposures e
            #               INNER JOIN processcheckpoints p
            #                 ON e.id=p.exposure_id AND p.event_id=999
            #               GROUP BY e.filename
            #             ) subqe
            #        FULL OUTER JOIN ( SELECT e.filename,COUNT(p.id) AS inerrors
            #                    FROM exposures e
            #                    INNER JOIN images i ON e.id=i.exposure_id
            #                    INNER JOIN processcheckpoints p
            #                      ON p.image_id=i.id AND p.event_id=999
            #                    GROUP BY e.filename ) subqi
            #          ON subqe.filename=subqi.filename
            #        FULL OUTER JOIN ( SELECT e.filename,COUNT(p.id) AS snerrors
            #                    FROM exposures e
            #                    INNER JOIN images i ON i.exposure_id=e.id
            #                    INNER JOIN subtractions s ON i.id=s.image_id
            #                    INNER JOIN processcheckpoints p
            #                      ON p.subtraction_id=s.id AND p.event_id=999
            #                    GROUP BY e.filename ) subqs
            #          ON subqe.filename=subqs.filename
            #      ) mastersubq
            # WHERE ....

            sql = ( "SELECT t.id,COUNT(c.id) AS nerr "
                    "FROM temp_find_exposures t "
                    "INNER JOIN processcheckpoints c ON c.exposure_id=t.id "
                    "WHERE c.event_id=%(error)s "
                    "GROUP BY t.id " )
            cursor.execute( sql, { "error": self.checkpointerrorvalue } )
            rows = cursor.fetchall()
            for row in rows:
                exposures[row['id']]['numerrors'] += row['nerr']

            sql = ( "SELECT t.id,COUNT(c.id) AS nerr "
                    "FROM temp_find_exposures t "
                    "INNER JOIN images i ON i.exposure_id=t.id "
                    "INNER JOIN processcheckpoints c ON c.image_id=i.id "
                    "WHERE c.event_id=%(error)s "
                    "GROUP BY t.id " )
            cursor.execute( sql, { "error": self.checkpointerrorvalue } )
            rows = cursor.fetchall()
            for row in rows:
                exposures[row['id']]['numerrors'] += row['nerr']

            sql = ( "SELECT t.id,COUNT(c.id) AS nerr "
                    "FROM temp_find_exposures t "
                    "INNER JOIN images i ON i.exposure_id=t.id "
                    "INNER JOIN subtractions s ON s.image_id=i.id "
                    "INNER JOIN subtraction_versiontag v ON v.subtraction_id=s.id AND v.versiontag_id=%(ver)s "
                    "INNER JOIN processcheckpoints c ON c.subtraction_id=s.id "
                    "WHERE c.event_id=%(error)s "
                    "GROUP BY t.id " )
            cursor.execute( sql, { "error": self.checkpointerrorvalue, "ver": versiontag } )
            rows = cursor.fetchall()
            for row in rows:
                exposures[row['id']]['numerrors'] += row['nerr']

            sys.stderr.write( "...done finding exposures.\n" )

            cursor.close()
            return json.dumps( { "status": "ok",
                                 "exposures": [ val for key, val in exposures.items() ] } )

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

            #... I just find it easier to figure out joins using SQL rather than trying
            # to figure out the more-byzantine way of doing it with SQLAlchemy

            try:
                conn = db.DB._engine.raw_connection()
                cursor = conn.cursor( cursor_factory=psycopg2.extras.DictCursor )

                q = ( "SELECT p.id,p.created_at,p.exposure_id,p.image_id,p.subtraction_id,i.ccdnum AS ccdnum,"
                      " si.ccdnum AS subccdnum,p.event_id,p.running_node,p.mpi_rank,p.notes "
                      "FROM processcheckpoints p "
                      "LEFT JOIN images i ON p.image_id=i.id "
                      "LEFT JOIN ( subtractions s INNER JOIN images si ON s.image_id=si.id ) ON p.subtraction_id=s.id "
                      "WHERE p.exposure_id=%(expid)s OR i.exposure_id=%(expid)s OR si.exposure_id=%(expid)s "
                      "ORDER BY p.created_at" )
                cursor.execute( q, { "expid": expid } )
                rows = cursor.fetchall()
                res = { "status": "ok",
                        "checkpoints": [] }
                for chkpt in rows:
                    res["checkpoints"].append( dict( chkpt ) )
                    res["checkpoints"][-1]["created_at"] = chkpt["created_at"].isoformat()
                cursor.close()
                return json.dumps( res )
            except Exception as e:
                return logerr( self.__class__, e )
            finally:
                conn.close()


            # This code is left over from when processcheckpoints only had
            # exposure_id, and the joins weren't as complicated
            # q = ( self.db.db.query(db.ProcessCheckpoint)
            #       .filter( db.ProcessCheckpoint.exposure_id==expid )
            #       .order_by( db.ProcessCheckpoint.created_at, db.ProcessCheckpoint.ccdnum ) )
            # results = q.all()
            # res = { "status": "ok",
            #         "checkpoints": [] }
            # for chkpt in results:
            #     res["checkpoints"].append(
            #         { "id": chkpt.id,
            #           "exposure_id": chkpt.exposure_id,
            #           "ccdnum": chkpt.ccdnum,
            #           "created_at": chkpt.created_at.isoformat(),
            #           "event_id": chkpt.event_id,
            #           "running_node": chkpt.running_node,
            #           "notes": chkpt.notes,
            #           "mpi_rank": chkpt.mpi_rank } )
            # return json.dumps( res )
        except Exception as e:
            return logerr( self.__class__, e )


# ======================================================================
# ROB.  You need to think about this when it comes to stacks, because
#  stacks won't have an associate exposure!  This is an issue for
#  proposalid.
# Think about versiontags.  Right now, if you don't give one, it
#  defaults to "latest".

class Cutouts(HandlerBase):
    def get_cutouts( self, expid=None, candid=None, sort="rb", rbtype=None, offset=None, limit=None,
                     mingallat=None, maxgallat=None, versiontag=1,
                     onlyvetted=False, proposals=None, notvettedby=None ):

        try:
            conn = db.DB._engine.raw_connection()

            subs = {}
            conds = []
            q = ( "SELECT od.id,o.id AS object_id,od.ra,od.dec,o.candidate_id,"
                  "  e.filename,i.basename,i.meanmjd,e.proposalid,"
                  "  i.magzp,i.ccdnum,i.filter,od.flux,od.fluxerr,od.mag,od.magerr" )
            if onlyvetted:
                q += ",COUNT(ss.id) AS nscores "
            else:
                q += ",0 AS nscores "
            q += ( "INTO TEMP TABLE temp_cutout_objs "
                   "FROM objectdatas od "
                   "INNER JOIN objectdata_versiontag v ON od.id=v.objectdata_id AND v.versiontag_id=%(vtag)s "
                   "INNER JOIN objects o ON od.object_id=o.id "
                   "INNER JOIN subtractions s ON od.subtraction_id=s.id "
                   "INNER JOIN subtraction_versiontag sv ON s.id=sv.subtraction_id AND sv.versiontag_id=%(vtag)s "
                   "INNER JOIN images i ON s.image_id=i.id "
                   "INNER JOIN exposures e ON i.exposure_id=e.id "
                   )
            subs['vtag'] = versiontag
            if onlyvetted:
                q += "INNER JOIN scanscore ss ON ss.objectdata_id=od.id "
                if notvettedby is not None:
                    q += "LEFT JOIN scanscore ss2 ON ss2.objectdata_id=od.id AND ss2.username=%(notvettedby)s"
                    subs['notvettedby'] = notvettedby
                    conds.append( "ss2.username IS NULL" )

            if expid is not None:
                conds.append( "e.id=%(expid)s" )
                subs['expid'] = expid
            elif candid is not None:
                conds.append( "o.candidate_id=%(candid)s" )
                subs['candid'] = candid

            if proposals is not None:
                conds.append( "e.proposaid IN %(proposals)s" )
                if isinstance( proposals, collections.abc.Iterable ) and ( type(proposals) != str ):
                    proposals = tuple( proposals )
                else:
                    proposals = ( proposals, )
                subs['proposals'] = proposals

            if mingallat is not None:
                conds.append( "e.gallat>=%(mingallat)s" )
                subs['mingallat'] = mingallat
            if maxgallat is not None:
                conds.append( "e.gallat<=%(maxgallat)s" )
                subs['maxgallat'] = maxgallat

            if len(conds) > 0:
                q += "WHERE " + " AND ".join( conds )

            if onlyvetted:
                # Avoid duplicates
                q += ( " GROUP BY od.id,o.id,od.ra,od.dec,o.candidate_id,e.filename,i.basename,i.meanmjd,"
                       "e.proposalid,i.magzp,i.ccdnum,i.filter,od.flux,od.fluxerr,od.mag,od.magerr " )

            if sort == "manyscore_random":
                if onlyvetted:
                    q += "ORDER BY nscores DESC,random() "
                else:
                    q += "ORDER BY random() "
                if limit is not None:
                    q += "LIMIT %(limit)s "
                    subs['limit'] = limit

            cursor = conn.cursor( cursor_factory=psycopg2.extras.DictCursor )
            sys.stderr.write( f"query={q} with subs={subs}\n" )
            cursor.execute( q, subs )
            cursor.execute( "SELECT COUNT(id) AS n FROM temp_cutout_objs" )
            rows = cursor.fetchall()
            totnobjs = rows[0]['n']

            # We have the things we want loaded into the temp table temp_cutout_objs
            # Now get the r/b info and cutouts

            subs = {}
            q = ( "SELECT t.id as objectdata_id,t.object_id,t.ra,t.dec,t.candidate_id AS candid,"
                  "  t.filename,t.basename,t.meanmjd,t.proposalid,"
                  "  t.magzp,t.ccdnum,t.filter,t.flux,t.fluxerr,t.mag,t.magerr, "
                  "  c.sci_jpeg,c.ref_jpeg,c.diff_jpeg," )
            if ( rbtype is not None ):
                q += ( "r.rb AS rb "
                       "FROM temp_cutout_objs t "
                       "LEFT JOIN objectrbs r ON t.id=r.objectdata_id AND r.rbtype_id=%(rbtype)s " )
                subs['rbtype'] = rbtype
            else:
                q += ( "NULL AS rb "
                       "FROM temp_cutout_objs t " )
            q += "LEFT JOIN cutouts c ON t.id=c.objectdata_id "

            if ( sort == "rb" ) and ( rbtype is not None ):
                q += "ORDER BY rb DESC "
            elif ( sort == "mjd" ):
                q += "ORDER BY meanmjd "
            elif ( sort == "manyscore_random" ) or ( sort is None ):
                # already sorted
                pass
            else:
                raise RuntimeError( f"Unknown sort order {sort}" )

            # If we did manyscore_random, we've already limited
            if sort != "manyscore_random":
                if limit is not None:
                    q += "LIMIT %(limit)s "
                    subs['limit'] = limit
                    if offset is not None:
                        q += "OFFSET %(offset)s "
                        subs['offset'] = offset

            # sys.stderr.write( f"Sending query: {str(q)}\n" )
            cursor.execute( q, subs )
            rows = cursor.fetchall()
            rows = [ dict(row) for row in rows ]
            cursor.close()

            # AUGH.  JSON makes NaN a pain in the butt
            for row in rows:
                for key, val in row.items():
                    if isinstance( val, numbers.Number ) and math.isnan( val ):
                        row[key] = None

            # Base-64 encode the jpegs
            for row in rows:
                for which in ( "sci", "ref", "diff" ):
                    row[f"{which}_jpeg"] = base64.b64encode( row[f"{which}_jpeg"] ).decode( 'ascii' )

            return { "totnobjs": totnobjs,
                     "offset": offset,
                     "num": len(rows),
                     "rbtype": rbtype,
                     "versiontag": versiontag,
                     "proposals": proposals,
                     "onlyvetted": onlyvetted,
                     "notvettedby": notvettedby,
                     "mingallat": mingallat,
                     "maxgallat": maxgallat,
                     "expid": expid,
                     "candid": candid,
                     "objs": rows }
        except Exception as e:
            raise e
        finally:
            conn.close()

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
#
# TODO : use the "vtag" input from data to limit version tags

class SearchCandidates(HandlerBase):
    def do_the_things( self ):
        try:
            nfound = -1
            conn = db.DB._engine.raw_connection()

            sys.stderr.write( "Starting SearchCandidates\n" )
            self.jsontop()
            data = json.loads( web.data() )
            sys.stderr.write( f"Data is: {data}" )

            subs = {}

            # First query : get all candidates that are detected
            #   according between startdate and enddate with
            #   at least the minimum S/N (if requested), considering
            #   only high-r/b objects (if requested), also using
            #   gallat and ra/dec cuts, and proposal cuts

            query = ( "SELECT DISTINCT o.candidate_id AS id "
                      "INTO TEMP TABLE temp_findcands "
                      "FROM objectdatas od "
                      "INNER JOIN objects o ON od.object_id=o.id "
                      "INNER JOIN subtractions s ON od.subtraction_id=s.id "
                      "INNER JOIN images i ON s.image_id=i.id "
                      "INNER JOIN exposures e ON i.exposure_id=e.id " )
            if data['useuserbcut']:
                query += ( "INNER JOIN objectrbs r ON od.id=r.objectdata_id "
                           "INNER JOIN rbtypes t ON r.rbtype_id=t.id " )
            conds = []
            if data['allorsome'] != "all":
                conds.append( f"e.proposalid IN %(props)s" )
                subs['props'] = tuple( data['proposals'] )
            if data['usestartdate']:
                conds.append( f"e.mjd>=%(startdate)s" )
                d = util.asDateTime( data['startdate'] )
                subs['startdate'] = util.mjd( d.year, d.month, d.day, d.hour, d.minute, d.second )
                sys.stderr.write( f"Got startdate {d} from {data['startdate']}, "
                                  f"translated to mjd {subs['startdate']}\n" )
            if data['useenddate']:
                conds.append( f"e.mjd<=%(enddate)s" )
                d = util.asDateTime( data['enddate'] )
                subs['enddate'] = util.mjd( d.year, d.month, d.day, d.hour, d.minute, d.second )
                sys.stderr.write( f"Got enddate {d} from {data['enddate']}, translated to mjd {subs['enddate']}\n" )
            if data['usegallatmin']:
                conds.append( f"( e.gallat>%(gallatmin)s OR e.gallat<%(neggallatmin)s )" )
                subs['gallatmin'] = data['gallatmin']
                subs['neggallatmin'] = -float( data['gallatmin'] )
            if data['usegallatmax']:
                conds.append( f"( e.gallat<%(gallatmax)s AND e.gallat>%(neggallatmax)s )" )
                subs['gallatmax'] = data['gallatmax']
                subs['neggallatmax'] = -float( data['gallatmax'] )
            if data['usera']:
                conds.append( f"q3c_radial_query(od.ra,od.dec,%(ra)s,%(dec)s,%(radius)s)" )
                subs['ra'] = data['ra']
                subs['dec'] = data['dec']
                subs['radius'] = data['radius']
            if data['useuserbcut']:
                conds.append( "t.id=%(rbtype)s AND r.rb>=t.rbcut " )
            if data['usesncut']:
                conds.append( "(od.flux/od.fluxerr)>%(sncut)s " )
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
                sys.stderr.write( f"Got startcount {d} from {data['startcount']}, translated to mjd {subs['startcount']}\n" )
            elif data['usestartdate']:
                dateconds.append( "e.mjd>=%(startdate)s" )
            if data['useendcount']:
                d = util.asDateTime( data['endcount'] )
                dateconds.append( "e.mjd<=%(endcount)s" )
                subs['endcount'] = util.mjd( d.year, d.month, d.day, d.hour, d.minute, d.second )
                sys.stderr.write( f"Got startcount {d} from {data['endcount']}, translated to mjd {subs['endcount']}\n" )
            elif data['useenddate']:
                dateconds.append( "e.mjd<=%(enddate)s" )

            query = ( "SELECT c.id AS id,COUNT(o.id) AS numhighrb,COUNT(DISTINCT e.filter) AS highrbfiltcount,"
                      "  MIN(e.mjd) AS highrbminmjd,MAX(e.mjd) AS highrbmaxmjd,"
                      "  MIN(od.mag) AS highrbminmag,MAX(od.mag) AS highrbmaxmag "
                      "INTO TEMP TABLE temp_filtercands "
                      "FROM temp_findcands c "
                      "INNER JOIN objects o ON o.candidate_id=c.id "
                      "INNER JOIN objectdatas od ON o.id=od.object_id "
                      "INNER JOIN objectrbs r ON od.id=r.objectdata_id "
                      "INNER JOIN rbtypes t ON r.rbtype_id=t.id "
                      "INNER JOIN subtractions s ON od.subtraction_id=s.id "
                      "INNER JOIN images i ON s.image_id=i.id "
                      "INNER JOIN exposures e ON i.exposure_id=e.id " )
            query += "WHERE t.id=%(rbtype)s AND r.rb>=t.rbcut "
            if len(dateconds) > 0:
                query += " AND " + ( " AND ".join(dateconds) )
            if data['usesncut']:
                query += " AND (od.flux/od.fluxerr)>%(sncut)s "
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
                      " MIN(od.mag) AS highsnminmag,MAX(od.mag) AS highsnmaxmag "
                      "INTO TEMP TABLE temp_filtercands2 "
                      "FROM temp_filtercands c "
                      "INNER JOIN objects o ON o.candidate_id=c.id "
                      "INNER JOIN objectdatas od ON o.id=od.object_id "
                      "INNER JOIN subtractions s ON od.subtraction_id=s.id "
                      "INNER JOIN images i ON s.image_id=i.id "
                      "INNER JOIN exposures e ON i.exposure_id=e.id " )
            conds = []
            if len( dateconds ) > 0:
                conds.extend( dateconds )
            if data['usesncut']:
                conds.append( "(od.flux/od.fluxerr)>%(sncut)s" )
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
            query = "SELECT * INTO TEMP TABLE temp_filtercands3 FROM temp_filtercands2 c "
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
                              "INTO TEMP TABLE temp_filteroutdate1 "
                              "FROM temp_filtercands3 c "
                              "INNER JOIN objects o ON c.id=o.candidate_id "
                              "INNER JOIN objectdatas od ON o.id=od.object_id "
                              "INNER JOIN subtractions s ON od.subtraction_id=s.id "
                              "INNER JOIN images i ON s.image_id=i.id "
                              "INNER JOIN exposures e ON i.exposure_id=e.id "
                              "INNER JOIN objectrbs r ON od.id=r.objectdata_id "
                              "INNER JOIN rbtypes t ON r.rbtype_id=t.id "
                              f"WHERE {outdateconds} "
                              "AND t.id=%(rbtype)s AND r.rb>=t.rbcut " )
                    if data['usesncut']:
                        query += " AND (o.flux/o.fluxerr)>%(sncut)s "
                    query += "GROUP BY c.id"
                else:
                    query = ( "SELECT c.id,0 AS highrboutside INTO TEMP TABLE temp_filteroutdate1 "
                              "FROM temp_filtercands3 c" )

                cursor = conn.cursor()
                sys.stderr.write( f"Sending query: {cursor.mogrify( query, subs)}\n" )
                cursor.execute( query, subs )

                if data["useoutsidedets"]:
                    query = ( "SELECT c.id,COUNT(o.id) AS numoutside "
                              "INTO TEMP TABLE temp_filteroutdate2 "
                              "FROM temp_filteroutdate1 c "
                              "INNER JOIN objects o ON c.id=o.candidate_id "
                              "INNER JOIN objectdatas od ON o.id=od.object_id "
                              "INNER JOIN subtractions s ON od.subtraction_id=s.id "
                              "INNER JOIN image si ON s.image_id=i.id "
                              "INNER JOIN exposures e ON i.exposure_id=e.id "
                              f"WHERE {outdateconds} " )
                    if data['usesncut']:
                        query += " AND (o.flux/o.fluxerr)>%(sncut)s "
                    query += "GROUP BY c.id"
                else:
                    query = ( "SELECT c.id,c.highrboutside,0 AS numoutside "
                              "INTO TEMP TABLE temp_filteroutdate2 FROM temp_filteroutdate1 c" )

                cursor = conn.cursor()
                cursor.execute( query, subs )

                query = ( "SELECT c.* "
                          "INTO TEMP TABLE temp_filtercands4 "
                          "FROM temp_filtercands3 c "
                          "LEFT JOIN temp_filteroutdate2 od ON c.id=od.id " )
                conds = []
                if data["useoutsidehighrbdets"]:
                    conds.append( "(highrboutside IS NULL OR highrboutside<=%(highrboutside)s)" )
                    subs['highrboutside'] = data['outsidehighrbdets']
                if data["useoutsidedets"]:
                    conds.append( "(numoutside IS NULL OR numoutside<=%(numoutside)s)" )
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
                      "  MAX(od.mag) AS totmaxmag,MIN(od.mag) AS totminmag "
                      "FROM temp_filtercands4 c "
                      "INNER JOIN objects o ON c.id=o.candidate_id "
                      "INNER JOIN objectdatas od ON o.id=od.object_id "
                      "INNER JOIN subtractions s ON s.id=od.subtraction_id "
                      "INNER JOIN images i ON s.image_id=i.id "
                      "INNER JOIN exposures e ON e.id=i.exposure_id "
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
            results = self.get_cutouts( sort="manyscore_random", limit=100, mingallat=mingallat, maxgallat=maxgallat,
                                        onlyvetted=onlyvetted, notvettedby=web.ctx.session.username )
            # Get user's current vet status
            sys.stderr.write( f"Going to do the thing.  results is a {type(results)}.\n" )
            sys.stderr.write( f"results['objs'] is a {type(results['objs'])}.\n" )
            q = ( self.db.db.query( db.ScanScore )
                  .filter( db.ScanScore.objectdata_id.in_( [ i["objectdata_id"] for i in results["objs"] ] ) )
                  .filter( db.ScanScore.username==web.ctx.session.username ) )
            them = q.all()
            score = {}
            for row in them:
                score[row.id] = row.goodbad

            for obj in results["objs"]:
                obj["goodbad"] = score[obj["objectdata_id"]] if obj["objectdata_id"] in score else "unset"

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
            for oid, gb in zip( data['objectdata_ids'], data['goodbads'] ):
                if ( gb != "good" ) and ( gb != "bad" ):
                    # sounds like ValueJudgementError
                    raise ValueError( f'Status {gb} is neither good nor bad' )
                gbs[oid] = gb
                ssids.append( f"{username}_{oid}" )
            mustmakenew = set( data['objectdata_ids'] )

            q = self.db.db.query( db.ScanScore ).filter( db.ScanScore.id.in_( ssids ) )
            scanscores = q.all()
            for onescore in scanscores:
                mustmakenew.remove( onescore.objectdata_id )
                onescore.goodbad = gbs[ onescore.objectdata_id ]
                res.append( { 'objectdata_id': onescore.objectdata_id, 'goodbad': gbs[ onescore.objectdata_id ] } )

            for oid in mustmakenew:
                ssid = f'{username}_{oid}'
                newscore = db.ScanScore( id=ssid, objectdata_id=oid, username=username, goodbad=gbs[oid] )
                self.db.db.add( newscore )
                res.append( { 'objectdata_id': oid, 'goodbad': gbs[ oid ] } )

            self.db.db.commit()

            return json.dumps( res )
        except Exception as e:
            return logerr( self.__class__, e )

# ======================================================================
# NOTE ABOUT VETTING : I haven't really thought about how this should
# interact with versiontags.  Right now, I just ignore the issue, and
# pretend that all objectdatas are independent.

class GetVetStats(HandlerBase):
    def do_the_things( self ):
        sys.stderr.write( "Starting GetVatStats\n" )
        conn = None
        try:
            self.jsontop()
            self.verifyauth()
            res = { 'yougal': 0,
                    'youexgal': 0,
                    'ngal': [],
                    'nexgal': []
                    }
            conn = db.DB._engine.raw_connection()
            cursor = conn.cursor( cursor_factory=psycopg2.extras.DictCursor )

            for field, cond in zip( [ 'yougal', 'youexgal' ],
                                    [ 'i.gallat<20. AND i.gallat>-20.', 'i.gallat>=20 OR i.gallat<=-20.' ] ):
                q = ( f"SELECT COUNT(s.id) AS n FROM scanscore s "
                                f"INNER JOIN objectdatas od ON s.objectdata_id=od.id "
                                f"INNER JOIN objects o ON od.object_id=o.id "
                                f"INNER JOIN images i ON o.image_id=i.id "
                                f"WHERE s.username=%(username)s "
                                f"AND ( {cond} ) " )
                subs = { "username": web.ctx.session.username }
                sys.stderr.write( f"Running query: {cursor.mogrify(q,subs)}\n" )
                cursor.execute( q, subs )
                rows = cursor.fetchall()
                sys.stderr.write( f"Got {len(rows)} results\n" )
                if len(rows) == 0:
                    res[field ] = 0
                else:
                    res[field] = rows[0]['n']

            for field, cond in zip( [ 'ngal', 'nexgal' ],
                                    [ 'i.gallat<20. AND i.gallat>-20.', 'i.gallat>=20 OR i.gallat<=-20.' ] ):
                query = ( f"SELECT nvets,COUNT(odid) AS nobjs FROM "
                          f" ( SELECT ss.objectdata_id AS odid,COUNT(ss.id) AS nvets "
                          f"   FROM scanscore ss "
                          f"   INNER JOIN objectdatas od ON ss.objectdata_id=od.id "
                          f"   INNER JOIN objects o ON o.id=od.object_id "
                          f"   INNER JOIN images i ON i.id=o.image_id "
                          f"   WHERE ( {cond} ) "
                          f"   GROUP BY ss.objectdata_id ) subq "
                          f" GROUP BY nvets ORDER BY nvets desc " )
                sys.stderr.write( f"Running query: {query}\n" )
                cursor.execute( query )
                rows = cursor.fetchall()
                sys.stderr.write( f"Got {len(rows)} results\n" )
                for row in rows:
                    res[field].append( [ row['nvets'], row['nobjs'] ] )

            return json.dumps( res )
        except Exception as e:
            return logerr( self.__class__, e )
        finally:
            if conn is not None:
                conn.close()

# ======================================================================

urls = (
    '/', "FrontPage",
    '/cand/(.+)', "ShowCand",
    '/getcamerainfo/(.+)/(.+)', "GetCameraInfo",
    '/getcamerachips/(.+)', "GetCameraChips",
    '/getrbtypes', "GetRBTypes",
    '/getversiontags', "GetVersionTags",
    '/findexposures', "FindExposures",
    '/checkpointdefs', "CheckpointDefs",
    '/exposurelog/(.+)', "ExposureLog",
    '/cutoutsforexp/(.+)', "CutoutsForExposure",
    '/cutoutsforcand/(.+)', "CutoutsForCandidate",
    '/searchcands', "SearchCandidates",
    '/getobjstovet', "GetObjectsToVet",
    '/setgoodbad', "SetGoodBad",
    '/getvetstats', "GetVetStats",
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
