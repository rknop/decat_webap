import sys
import pathlib
from datetime import datetime, timedelta
import dateutil.parser
import pytz
import uuid
import sqlalchemy as sa
import sqlalchemy.orm
import sqlalchemy.ext.automap
from sqlalchemy.ext.declarative import declarative_base, declared_attr, DeferredReflection
import sqlalchemy.inspection
import sqlalchemy.pool
from sqlalchemy.dialects.postgresql import UUID as sqlUUID

scriptdir = str( pathlib.Path( __file__ ).parent )
if scriptdir not in sys.path:
    sys.path.insert(0, scriptdir )

from decatview_config import DBdata, DBname

# class BaseClass(object):
#     @declared_attr
#     def __table__(cls):
#         global Base
#         sys.stderr.write( f"Making a Table with engine={DB._engine}" )
#         return sa.Table( cls.__name__.lower() + "s", Base.metadata, autoload_with=DB._engine )

    # @declared_attr
    # def __tablename__(cls):
    #     return cls.__name__.lower() + "s"
    
# Base = sqlalchemy.ext.automap.automap_base()
Base = declarative_base()


# ======================================================================

NULLUUID = uuid.UUID( '00000000-0000-0000-0000-000000000000' )

def asUUID( val, canbenone=True ):
    if val is None:
        if canbenone:
            return None
        else:
            return NULLUUID
    if isinstance( val, uuid.UUID ):
        return val
    else:
        return uuid.UUID( val )

# ======================================================================

class DB(object):
    _engine = None
    _sessionfac = None
    _dbparamsset = False
    
    @classmethod
    def setdbparams( cls, user=None, password=None, host=None, port=None, database=None ):
        global DBdata, DBname
        if user is None:
            with open( f'{DBdata}/dbuser' ) as ifp:
                cls._user = ifp.readline().strip()
        else:
            cls._user = user
        if password is None:
            with open( f'{DBdata}/dbpasswd' ) as ifp:
                cls._password = ifp.readline().strip()
        else:
            cls._password = password
        if host is None:
            with open( f'{DBdata}/dbhost' ) as ifp:
                cls._host = ifp.readline().strip()
        else:
            cls._host = host
        if port is None:
            with open( f'{DBdata}/dbport' ) as ifp:
                cls._port = ifp.readline().strip()
        else:
            cls._port = port
        if database is None:
            with open( f'{DBdata}/{DBname}' ) as ifp:
                cls._database = ifp.readline().strip()
        else:
            cls._database = database
        cls._dbparamsset = True
    
    @classmethod
    def DBinit( cls ):
        if cls._engine is None:
            if not cls._dbparamsset:
                cls.setdbparams()
            cls._engine = sa.create_engine(f'postgresql://{cls._user}:{cls._password}@{cls._host}:{cls._port}'
                                           f'/{cls._database}', poolclass=sqlalchemy.pool.NullPool )
            # sys.stderr.write( f"engine is {cls._engine}\n" )
            DeferredReflection.prepare( DB._engine )
            # sys.stderr.write( "kaglorky\n" )
            cls._sessionfac = sa.orm.sessionmaker( bind=cls._engine, expire_on_commit=False )
            # sys.stderr.write( f"sessionfac is {cls._sessionfac}\n" )

    @staticmethod
    def collectionname( base, local_cls, referred_cls, constraint ):
        return constraint.name

    @staticmethod
    def get( db=None ):
        if db is None:
            return DB()
        else:
            return DB( db.db )

    def __init__( self, db=None ):
        self.mustclose = False
        if db is None:
            if DB._engine is None:
                # sys.stderr.write( "Initializing database.\n" )
                DB.DBinit()
            # else:
            #     sys.stderr.write( "Don't need to initialize database\n" )
            # sys.stderr.write( f"Doing _sessionfac; _sessionfac is {DB._sessionfac}\n" )
            self.db = DB._sessionfac()
            self.mustclose = True
        else:
            self.db = db

    def __enter__( self ):
        return self

    def __exit__( self, exc_type, exc_val, exc_tb ):
        self.close()
            
    def __del__( self ):
        self.close()

    def close( self ):
        if self.mustclose and self.db is not None:
            self.db.close()
            self.db = None

# ======================================================================

class HasPrimaryID(object):
    @classmethod
    def get( cls, id, curdb=None ):
        with DB.get(curdb) as db:
            q = db.db.query(cls).filter( cls.id==id )
            if q.count() > 1:
                raise ErrorMsg( f'Error, {cls.__name__} {id} multiply defined!  This shouldn\'t happen.' )
            if q.count() == 0:
                return None
            return q[0]
    
class HasPrimaryUUID(HasPrimaryID):
    id = sa.Column( sqlUUID(as_uuid=True), primary_key=True, default=uuid.uuid4 )
    @classmethod
    def get( cls, id, curdb=None ):
        id = id if isinstance( id, uuid.UUID) else uuid.UUID( id )
        return super().get( id, curdb=curdb )

# ======================================================================
# Gotta keep this synced with db.py in the main lensgrinder code base!

class CameraChip(DeferredReflection,Base,HasPrimaryID):
    __tablename__ = "camerachips"

class Camera(DeferredReflection,Base,HasPrimaryID):
    __tablename__ = "cameras"
    
class Candidate(DeferredReflection,Base,HasPrimaryID):
    __tablename__ = "candidates"

class CheckpointEventDef(DeferredReflection,Base,HasPrimaryID):
    __tablename__ = "checkpointeventdefs"

class Cutout(DeferredReflection,Base,HasPrimaryID):
    __tablename__ = "cutouts"

class Exposure(DeferredReflection,Base,HasPrimaryID):
    __tablename__ = "exposures"

class Image(DeferredReflection,Base,HasPrimaryID):
    __tablename__ = "images"

class ObjectRB(DeferredReflection,Base,HasPrimaryID):
    __tablename__ = "objectrbs"

# class Object(DeferredReflection,Base,HasPrimaryID):
#     __tablename__ = "objects"


class ObjectData(DeferredReflection,Base,HasPrimaryID):
    __tablename__ = "objectdatas"

# SQLAlchemy craps out because this table doesn't
#   have a primary key.
# class ObjectData_VersionTag(DeferredReflection,Base):
#     __tablename__ = "objectdata_versiontag"

class ProcessCheckpoint(DeferredReflection,Base,HasPrimaryID):
    __tablename__ = "processcheckpoints"

class RBType(DeferredReflection,Base,HasPrimaryID):
    __tablename__ = "rbtypes"

class ScanScore(DeferredReflection,Base,HasPrimaryID):
    __tablename__ = "scanscore"

class Subtraction(DeferredReflection,Base,HasPrimaryID):
    __tablename__ = "subtractions"

class VersionTag(DeferredReflection,Base,HasPrimaryID):
    __tablename__ = "versiontags"
    
# ======================================================================

class User(DeferredReflection,Base,HasPrimaryUUID):
    __tablename__ = "webapusers"

    @classmethod
    def new( cls, displayname=None, email=None, username=None, pubkey="", privkey="", lastlogin=None, curdb=None ):
        if ( displayname is None ) or ( email is None ) or ( username is None ):
            raise ErrorMsg( f'Error, must have all of displayname, email, username to make a new User' )
        with DB.get(curdb) as db:
            person = User( displayname=displayname, email=email, username=username,
                           pubkey=pubkey, privkey=privkey, lastlogin=asDateTime(lastlogin) )
            db.db.add(person)
            db.db.commit()
            return person
    
    @classmethod
    def getbyusername( cls, name, curdb=None ):
        with DB.get(curdb) as db:
            q = db.db.query(User).filter( User.username==name )
            return q.all()

    @classmethod
    def getusernamelike( cls, namematch, curdb=None ):
        with DB.get(curdb) as DB:
            q = db.db.query(User).filter( User.username.like( f'%{namematch}%' ) )
            return q.all()

    @classmethod
    def getbyemail( cls, email, curdb=None ):
        with DB.get(curdb) as db:
            q = db.db.query(User).filter( User.email==email )
            return q.all()

    @classmethod
    def getemaillike( cls, emailmatch, curdb=None ):
        q = db.db.query(User).filter( User.email.like( f'%{emailmatch}%' ) )
        return q.all()

    @classmethod
    def getdisplaynamelike( cls, dnamematch, curdb=False ):
        q = db.db.query(User).filter( User.displayname.like( f'%{dnamematch}%' ) )
        return q.all()

# ======================================================================

class PasswordLink(DeferredReflection,Base,HasPrimaryUUID):
    __tablename__ = "passwordlink"
    userid = sa.Column( sqlUUID(as_uuid=True), default=None )
    
    @classmethod
    def new( cls, userid, expires=None, curdb=None ):
        if expires is None:
            expires = datetime.now(pytz.utc) + timedelta(hours=1)
        else:
            expires = asDateTime( expires )
        with DB.get(curdb) as db:
            link = PasswordLink( userid = asUUID(userid),
                                 expires = expires )
            db.db.add( link )
            db.db.commit()
            return link
    
