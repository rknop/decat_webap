import sys
import os
import io
import pathlib
import web
import json
import traceback

import sqlalchemy as sa

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
        self.db = db.DB.get()
        
    def GET( self, *args, **kwargs ):
        return self.do_the_things( *args, **kwargs )

    def POST( self, *args, **kwargs ):
        return self.do_the_things( *args, **kwargs )

    def verifyauth( self ):
        if ( not hasattr( web.ctx.session, "authenticated" ) ) or ( not web.ctx.session.authenticated ):
            raise RuntimeError( "User not authenticated" )
        
    def jsontop( self ):
        web.header('Content-Type', 'application/json')

    def jsontop_verifyauth( self ):
        self.jsontop()
        self.verifyauth()
    
    def htmltop( self, title=None, h1=None, statusdiv=False, includejsstart=False ):
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
        self.response += f"<script src=\"{webapdirurl}decatview.js\" type=\"module\"></script>\n"
        if includejsstart:
            self.response += f"<script src=\"{webapdirurl}decatview_start.js\" type=\"module\"></script>\n"
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
    def __init__( self ):
        super().__init__()

    def do_the_things( self ):
        self.htmltop( includejsstart=True, statusdiv=True )
        self.response += "<div id=\"main-div\"></div>\n"
        self.htmlbottom()
        return self.response
        

# ======================================================================

class GetRBTypes(HandlerBase):
    def __init__( self ):
        super().__init__()

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
    
    def __init__( self ):
        super().__init__()

    def do_the_things( self ):
        try:
            self.jsontop()
            data = json.loads( web.data() )
            t0 = util.asDateTime( data['t0'] )
            t1 = util.asDateTime( data['t1'] )
            errorchk = sa.orm.aliased( db.ProcessCheckpoint )
            donechk = sa.orm.aliased( db.ProcessCheckpoint )
            q = self.db.db.query( db.Exposure, sa.func.count(sa.distinct(db.Subtraction.id)),
                                  sa.func.count(sa.distinct(errorchk.id)),
                                  sa.func.count(sa.distinct(db.Object.id)),
                                  sa.func.count(sa.distinct(db.ObjectRB.id)),
                                  sa.func.count(sa.distinct(donechk.id)) )
            q = q.join( db.Subtraction, db.Exposure.id==db.Subtraction.exposure_id, isouter=True )
            q = q.join( errorchk, sa.and_( db.Exposure.id==errorchk.exposure_id,
                                           errorchk.event_id==self.checkpointerrorvalue ),
                        isouter=True )
            q = q.join( donechk, sa.and_( db.Exposure.id==donechk.exposure_id,
                                          donechk.event_id==self.checkpointdonevalue ),
                        isouter=True )
            q = q.join( db.Object, db.Subtraction.id==db.Object.subtraction_id, isouter=True )
            q = q.join( db.ObjectRB, sa.and_( db.ObjectRB.object_id==db.Object.id,
                                              db.ObjectRB.rbtype_id==data['rbtype'],
                                              db.ObjectRB.rb>=data['rbcut'] ), isouter=True )
            if t0 is not None:
                q = q.filter( db.Exposure.mjd >= util.mjd( t0.year, t0.month, t0.day, t0.hour, t0.minute, t0.second ) )
            if t1 is not None:
                q = q.filter( db.Exposure.mjd <= util.mjd( t1.year, t1.month, t1.day, t1.hour, t1.minute, t1.second ) )
            if data['allorsomeprops'] != 'all':
                q = q.filter( db.Exposure.proposalid.in_( data['props'] ) )
            q = q.group_by( db.Exposure )
            q = q.order_by( db.Exposure.mjd )
            sys.stderr.write( f"Sending query: {str(q)}\n" )
            res = q.all()
            sys.stderr.write( f"Got {len(res)} responses\n" )
            exps = []
            for row in res:
                exposure = row[0]
                exps.append( { 'id': exposure.id,
                               'filename': exposure.filename,
                               'filter': exposure.filter,
                               'proposalid': exposure.proposalid,
                               'exptime': exposure.header['EXPTIME'],
                               'ra': exposure.ra,
                               'dec': exposure.dec,
                               'gallat': exposure.gallat,
                               'gallong': exposure.gallong,
                               'numsubs': row[1],
                               'numerrors': row[2],
                               'numobjs': row[3],
                               'numhighrbobjs': row[4],
                               'numdone': row[5] } )
            return json.dumps( { "status": "ok",
                                 "exposures": exps } )
        except Exception as e:
            return logerr( self.__class__, e )

# ======================================================================

class CheckpointDefs(HandlerBase):
    def __init__( self ):
        super().__init__()

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
    def __init__( self ):
        super().__init__()

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

urls = (
    '/', "FrontPage",
    '/getrbtypes', "GetRBTypes",
    '/findexposures', "FindExposures",
    '/checkpointdefs', "CheckpointDefs",
    '/exposurelog/(.+)', "ExposureLog",
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
def session_hook(): web.ctx_session = session
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
