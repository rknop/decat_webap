#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import math
import pathlib
import logging
import numpy
import pandas
import json
import psycopg2
import psycopg2.extras
import psycopg2.extensions
import base64
from io import BytesIO

scriptdir = str( pathlib.Path( __file__ ).parent )
if scriptdir not in sys.path:
    sys.path.insert(0, scriptdir )

from util import dtohms, dtodms, radectolb, mjd, dateofmjd, parsedms, sanitizeHTML
from webapconfig import DBdata, DBname

def adapt_numpy_int64( val ):
        return psycopg2.extensions.AsIs( val )
psycopg2.extensions.register_adapter( numpy.int64, adapt_numpy_int64 )

def cursor_to_df( cursor, index=None ):
    rows = cursor.fetchall()
    if len(rows) == 0:
        columns = [ c.name for c in cursor.description ]
        df = pandas.DataFrame( columns=columns, dtype=int )
    else:
        df = pandas.DataFrame( rows )
    if index is not None:
        df.set_index( index, inplace=True )
    return df
    

# ======================================================================

class DB:
    _dbparamsset = False
    
    @classmethod
    def setdbparams( cls, user=None, password=None, host=None, port=None, database=None ):
        if user is None:
            with open( f'{DBdata}/dbuser' ) as ifp:
                cls.user = ifp.readline().strip()
        else:
            cls.user = user
        if password is None:
            with open( f'{DBdata}/dbpasswd' ) as ifp:
                cls.password = ifp.readline().strip()
        else:
            cls.password = password
        if host is None:
            with open( f'{DBdata}/dbhost' ) as ifp:
                cls.host = ifp.readline().strip()
        else:
            cls.host = host
        if port is None:
            with open( f'{DBdata}/dbport' ) as ifp:
                cls.port = ifp.readline().strip()
        else:
            cls.port = port
        if database is None:
            with open( f'{DBdata}/{DBname}' ) as ifp:
                cls.database = ifp.readline().strip()
        else:
            cls.database = database
        cls._dbparamsset = True
        
    
    @staticmethod
    def get( curdb=None ):
        if curdb is None:
            return DB()
        else:
            return DB( curdb )
    
    def __init__( self, curdb=None ):
        if curdb is not None:
            self.db = curdb.db
            self._mustclose = False
        else:
            if not self._dbparamsset:
                DB.setdbparams()
            self.db = psycopg2.connect( dbname=self.database, user=self.user, host=self.host,
                                        password=self.password, port=self.port,
                                        cursor_factory=psycopg2.extras.RealDictCursor )
            self._mustclose = True

    def close( self ):
        if self._mustclose and self.db is not None:
            self.db.close()
        self.db = None

    def __enter__( self ):
        return self

    def __exit__( self, exc_type, exc_val, exc_tb ):
        self.close()

    def __del( self ):
        self.close()

# ======================================================================

class HasDF:
    def __init__( self, df=None ):
        self._df = df
    
    @staticmethod
    def getlogger( logger=None ):
        if logger is None:
            logger = logging.getLogger( "main" )
            logerr = logging.StreamHandler( sys.stderr )
            logger.addHandler( logerr )
            logerr.setFormatter( logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s' ) )
            logger.setLevel( logging.DEBUG )
        return logger

    @property
    def df( self ):
        return self._df

    def __len__( self ):
        return len(self._df)
    
    def subset( self, start, end ):
        if start < 0:
            start = 0
        if start >= len(self._df):
            start = len(self._df) - 1
        if end < start:
            end = start+1
        if end > len(self._df):
            end = len(self._df)
        self._df = self._df.iloc[start:end]

# ======================================================================

class ObjectTable(HasDF):

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )

    baseobjquery = (
        "SELECT o.id AS oid,"
        "    o.created_at AS created_at,o.modified AS modified,"
        "    o.ra AS ra,o.dec AS dec,"
        "    o.candidate_id AS cid,o.subtraction_id AS subtraction_id,"
        "    o.name AS name,"
        "    o.mag AS mag,o.magerr AS magerr,"
        "    o.flux AS flux,o.fluxerr AS fluxerr,"
        "    o.ignore AS ignore,"
        "    s.ccdnum AS ccdnum,"
        "    e.filename AS exposurename,"
        "    e.mjd AS mjd,"
        "    e.filter AS filter "
        "  FROM exposures e "
        "  INNER JOIN subtractions s ON s.exposure_id=e.id "
        "  INNER JOIN objects o ON o.subtraction_id=s.id "
    )        

    # ======================================================================

    @classmethod
    def load_random_set( cls, rbtype=None, loadjpg=False, loadvetting=False, number=100,
                         mingallat=None, maxgallat=None, curdb=None, logger=None ):
        logger = cls.getlogger( logger )

        cond = ""
        subvars = {}
        if mingallat is not None:
            cond += " ( e.gallat<=-%(mingallat)s OR e.gallat>=%(mingallat)s ) "
            subvars['mingallat'] = mingallat
        if maxgallat is not None:
            cond += " AND " if len(cond)>0 else ""
            cond += " ( e.gallat>=-%(maxgallat)s AND e.gallat<=%(maxgallat)s ) "
            subvars['maxgallat'] = maxgallat
        if len(cond)>0:
            cond = f'WHERE {cond}'
        query = f'{cls.baseobjquery} {cond} ORDER BY RANDOM()'
        if number is not None:
            query += " LIMIT %(number)s"
            subvars['number'] = psycopg2.extensions.AsIs(number)

        with DB.get( curdb ) as db:
            cursor = db.db.cursor()
            cursor.execute( query, subvars )
            df = cursor_to_df(cursor, "oid")
            objtab = cls( df )

            if rbtype is not None:
                objtab.loadrb( rbtype=rbtype, curdb=db )
            if loadjpg:
                objtab.loadjpg( curdb=db )
            if loadvetting:
                objtab.loadvetting( curdb=db )

        return objtab

    # ======================================================================
    
    @classmethod
    def load_for_exposure( cls, exposure_id=None, exposure_name=None,
                           ccdnums=None, rbtype=None, loadjpg=False,
                           loadvetting=False, curdb=None, logger=None ):
        """Get all exposures for a given exposure id"""

        logger = cls.getlogger( logger )

        query = cls.baseobjquery
        subvars = {}
        cond = ""
        if exposure_id is not None:
            cond = "e.id=%(exposure_id)s"
            subvars[ 'exposure_id' ] = exposure_id
        elif exposure_name is not None:
            cond = "e.filename=%(exposure_name)s"
            subvars[ 'exposure_name' ] = exposure_name
        else:
            raise ValueError( "Must specify either exposure_id or exposure_name" )
        if ccdnums is not None:
            if len(cond) > 0:
                cond += " AND "
            cond += "s.sccdnum IN %(ccdnums)s"
            subvars[ 'ccdnums' ] = tuple( ccdnums )
        if len(cond) > 0:
            query += f" WHERE {cond}"

        with DB.get( curdb ) as db:
            cursor = db.db.cursor()
            # logger.debug( cursor.mogrify( query, subvars ) )
            cursor.execute( query, subvars )
            df = cursor_to_df(cursor, "oid")
            objtab = cls( df )

            if rbtype is not None:
                objtab.loadrb( rbtype=rbtype, curdb=db )
            if loadjpg:
                objtab.loadjpg( curdb=db )
            if loadvetting:
                objtab.loadvetting( curdb=db )

        return objtab

    # ======================================================================

    @classmethod
    def load_for_candidate( cls, candidate_id=None, rbtype=None, loadjpg=False,
                            loadvetting=False, curdb=None, logger=None ):
        logger = cls.getlogger( logger )
        with DB.get( curdb ) as db:
            cursor = db.db.cursor()
            query = cls.baseobjquery
            query += "WHERE o.candidate_id=%s ORDER BY e.mjd,e.filter"
            cursor.execute( query, ( candidate_id, ) )
            df = cursor_to_df( cursor, "oid" )
            objtab = cls( df )

            if rbtype is not None:
                objtab.loadrb( rbtype=rbtype, curdb=db )
            if loadjpg:
                objtab.loadjpg( curdb=db )
            if loadvetting:
                objtab.loadvetting( curdb=db )

        return objtab
    
    # ======================================================================
    
    @classmethod
    def load_by_rated( cls, conditions=[], rbtype=None, loadjpg=False, random=True, number=100, logger=None,
                       mingallat=None, maxgallat=None ):
        """Get candidates that have been rated.

        If conditions is not None, it's a list of dicts.  Each dict:
           type : "user", "majority", "unanimous"
           goodbad : 'good', 'bad', or 'either' for majority or unanimous
           user : username of user (user only)
           userrate : 'good', 'bad', 'notgood', 'notbad' (user only)
           minvets : minimum number of times it's been vetted (majority, unanimous only)
           mindiff : minimum ngood-nbad or nbad-ngood (majority only)
        majority and unanimous can't be used together.

        Table will be indexed by oid (object_id) and will have columns (maybe not in this order):
           created_at : timestamp
           modified : timestamp
           ra : float64
           dec : float64
           cid : str (candidate id)
           subtraction_id : int64
           name : None
           mag : float64
           magerr : float64
           flux : float64
           fluxerr : float64
           ignore : bool
           exposurename : str
           ccdnum : int
           ngoods : int64 ( hardcore overkill )
           nbads : int64
           users : list
           goodbads : list
         
        If rbtype is not None, there will also be a float column "rb"

        If loadjpg is True, there will be three base64 encoded columns
           scijpg, refjpg, diffjpg
        If loadfits is True, it will be ignored for now
           
        """

        logger = cls.getlogger( logger )
        
        userlist = []
        userrate = []
        notuserlist = []
        notuserrate = []
        majority = False
        unanimous = False
        minvets = 0
        mindiff = 1
        for cond in conditions:
            if cond['type'] == 'majority':
                if unanimous:
                    raise ValueError( f'majority and unanimous rated candidates don\'t make sense together' )
                majority = True
                if 'minvets' in cond: minvets = cond['minvets']
                if 'mindiff' in cond: mindiff = cond['mindiff']
                goodbad = cond['goodbad'] if 'goodbad' in cond else 'good'
            elif cond['type'] == 'unanimous':
                if majority:
                    raise ValueError( f'majority and unanimous rated candidates don\'t make sense together' )
                unanimous = True
                if 'minvets' in cond: minvets = cond['minvets']
                goodbad = cond['goodbad'] if 'goodbad' in cond else 'good'
            elif cond['type'] == 'user':
                if cond['userrate'][0:4] == "not ":
                    notuserlist.append( cond['user'] )
                    notuserrate.append( cond['userrate'][4:] )
                else:
                    userlist.append( cond['user'] )
                    userrate.append( cond['userrate'] )

        # Something that makes me unhappy:
        # If I have any user conditions, I'm going to pull down more
        # of the table than I really need to I do the filtering later
        # with pandas stuff.  If I could figure out a really fancy way
        # to do it all in SQL, I could just add a LIMIT clause.  As
        # is, I'm going to be working on WAY more data than I really
        # need to.

        subvals = {}
        usern = 0
        query = (
            "SELECT oid,created_at,modified,ra,dec,cid,subtraction_id,name,"
            "    mag,magerr,flux,fluxerr,ignore,exposurename,mjd,ccdnum,users,goodbads,ngoods,nbads "
            "FROM "
            "  ( SELECT o.id AS oid,"
            "        o.created_at AS created_at,o.modified AS modified,"
            "        o.ra AS ra,o.dec AS dec,"
            "        o.candidate_id AS cid,o.subtraction_id AS subtraction_id,"
            "        o.name AS name,"
            "        o.mag AS mag,o.magerr AS magerr,"
            "        o.flux AS flux,o.fluxerr AS fluxerr,"
            "        o.ignore AS ignore,"
            "        s.ccdnum AS ccdnum,"
            "        e.mjd AS mjd,"
            "        e.filename AS exposurename,"
            "        array_agg(ss.username) AS users,"
            "        array_agg(ss.goodbad) AS goodbads,"
            "        COUNT(ss.username) FILTER (WHERE goodbad='good') AS ngoods,"
            "        COUNT(ss.username) FILTER (WHERE goodbad='bad') AS nbads"
            "      FROM scanscore ss "
            "      INNER JOIN objects o ON ss.object_id=o.id "
            "      INNER JOIN subtractions s ON o.subtraction_id=s.id "
            "      INNER JOIN exposures e ON s.exposure_id=e.id " )

        cond = ""
        if mingallat is not None:
            cond += " ( e.gallat<=-%(mingallat)s OR e.gallat>=%(mingallat)s ) "
            subvals['mingallat'] = mingallat
        if maxgallat is not None:
            cond += " AND " if len(cond)>0 else ""
            cond += " ( e.gallat>=-%(maxgallat)s AND e.gallat<=%(maxgallat)s ) "
            subvals['maxgallat'] = maxgallat
        if len(cond) > 0:
            query += f"WHERE {cond} "
        cond = ""
            
        query += (
            "      GROUP BY o.id,o.created_at,o.modified,o.ra,o.dec,"
            "        o.candidate_id,o.subtraction_id,o.name,o.mag,o.magerr,"
            "        o.flux,o.fluxerr,o.ignore,e.filename,e.mjd,s.ccdnum ) AS x" )
        if unanimous:
            cond += " AND " if len(cond)>0 else ""
            if goodbad == "either":
                cond += " ( ( ngoods>=%(minvets)s AND nbads=0 ) OR ( nbads>=%(minvets)s AND ngoods=0 ) )"
            elif goodbad == "good":
                cond += " ( ngoods>=%(minvets)s AND nbads=0 ) "
            elif goodbad == "bad":
                cond += " ( nbads>=%(minvets)s AND ngoods=0 ) "
            else:
                raise ValueError( f'Unknown value of goodbad: {goodbad}' )
            subvals['minvets'] = minvets
        if majority:
            cond += " AND " if len(cond)>0 else ""
            if goodbad == "either":
                cond += " ( ( ngoods-nbads>=%(mindiff)s OR nbads-ngoods>=%(mindiff)s ) "
            elif goodbad == "good":
                cond += " ( ngoods-nbads>=%(mindiff)s "
            elif goodbad == "bad":
                cond += " ( nbads-ngoods>=%(mindiff)s "
            else:
                raise ValueError( f'Unknown value of goodbad: {goodbad}' )
            cond += " AND ngoods+nbads>=%(minvets)s ) "
            subvals['minvets'] = minvets
            subvals['mindiff'] = mindiff
        for user in userlist:
            cond += " AND " if len(cond)>0 else ""
            cond += f" ( %(user{usern})s=ANY(users) ) "
            subvals[f'user{usern}'] = user
            usern += 1
        if len(cond) > 0:
            query += f" WHERE {cond}"
        if random:
            query += " ORDER BY RANDOM()"
        if ( len(userlist) == 0) and ( len(notuserlist) == 0 ) and ( number is not None ):
            query += " LIMIT %(number)s"
            subvals['number'] = number
        
            
        with DB.get() as db:
            cursor = db.db.cursor()
            # logger.debug( cursor.mogrify( query, subvals ) )
            cursor.execute( query, subvals )
            df = cursor_to_df(cursor, "oid")

            if ( len(userlist) > 0 ) or ( len(notuserlist) > 0 ):
                # In order to apply the list conditions, unpack the arrays
                users = df["users"].explode()
                goodbads = df["goodbads"].explode()
                tmpdf = pandas.DataFrame( { "user": users, "goodbad": goodbads } )
                df = df.drop( ["users", "goodbads"], axis=1 ).merge( tmpdf, left_index=True, right_index=True )
                df = df.reset_index().set_index( ['oid', 'user'] )
                
                for user, rating in zip( userlist, userrate ):
                    if rating == "either":
                        tmp = df.loc[ df.index.isin( [user], level=1 ) ]
                        oids = tmp.index.unique(level=0).values
                        df = df.loc[oids]
                    else:
                        tmp = df['goodbad'].loc[ pandas.IndexSlice[ :, user ] ] == rating
                        oids = tmp[tmp].index.values
                        df = df.loc[oids]

                for user, rating in zip( notuserlist, notuserrate ):
                    if rating == "either":
                        tmp = df.loc[ df.index.isin( [user], level=1 ) ]
                        oids = tmp.index.unique(level=0).values
                        df = df.loc[ ~df.index.isin( oids, level=0 ) ]
                    else:
                        tmp = df['goodbad'].loc[ pandas.IndexSlice[ :, user ] ] == rating
                        oids = tmp[tmp].index.values
                        df = df.loc[ ~df.index.isin( oids, level=0 ) ]

                # Repack the users and goodbads into lists
                df.reset_index(inplace=True)
                tmp = df.groupby( 'oid' )[ ['user','goodbad'] ].agg(list)
                df = df.groupby( 'oid' ).agg('first')
                df.drop( ['user', 'goodbad'], axis=1, inplace=True )
                df = df.merge( tmp, left_index=True, right_index=True )
                df.rename( columns={ 'user':'users', 'goodbad':'goodbads' }, inplace=True )

            if not random:
                df.sort_values( ['mjd', 'ccdnum', 'oid' ], inplace=True )
                
            objtab = cls( df )
            if rbtype is not None:
                objtab.loadrb( rbtype=rbtype, curdb=db )
            if loadjpg:
                objtab.loadjpg( curdb=db )

        return objtab

    # ======================================================================
        
    def loadrb( self, rbtype=None, curdb=None ):
        if rbtype is None:
            return
        if len(self._df) == 0:
            self._df["rb"] = pandas.Series( [], dtype="float" )
            return
        oids = self._df.index.unique().values
        with DB.get( curdb ) as db:
            cursor = db.db.cursor()
            query = ( f"SELECT object_id as oid,rb FROM objectrbs "
                      f"WHERE object_id IN %(oids)s AND rbtype_id=%(rbtype)s" )
            # sys.stderr.write( str(cursor.mogrify( query, { 'oids': tuple(oids), 'rbtype': rbtype } )) + "\n" )
            cursor.execute( query, { 'oids': tuple(oids), 'rbtype': rbtype } )
            rbframe = cursor_to_df( cursor, "oid" )
            cursor.close()
        self._df = self._df.merge( rbframe, left_index=True, right_index=True, how='left' )

    # ======================================================================
        
    def loadjpg( self, curdb=None ):
        if len(self._df) == 0:
            self._df["scijpg" ] = pandas.Series( [], dtype="object" )
            self._df["refjpg" ] = pandas.Series( [], dtype="object" )
            self._df["diffjpg" ] = pandas.Series( [], dtype="object" )
            return
        oids = self._df.index.unique().values
        with DB.get( curdb ) as db:
            cursor = db.db.cursor()
            query = ( "SELECT object_id as oid, "
                      "  ENCODE(sci_jpeg, 'base64') as scijpg, "
                      "  ENCODE(ref_jpeg, 'base64') as refjpg, "
                      "  ENCODE(diff_jpeg, 'base64') as diffjpg "
                      "FROM cutouts WHERE object_id IN %s" )
            cursor.execute( query, ( tuple(oids), ) )
            cuframe = cursor_to_df( cursor, "oid" )
            cursor.close()
        self._df = self._df.merge( cuframe, left_index=True, right_index=True, how='left' )

    # ======================================================================

    def loadvetting( self, curdb=None ):
        if 'users' in self._df.columns:
            return
        if len(self._df) == 0:
            self._df["ngoods" ] = pandas.Series( [], dtype="int" )
            self._df["nbadss" ] = pandas.Series( [], dtype="int" )
            self._df["users" ] = pandas.Series( [], dtype="object" )
            self._df["goodbads" ] = pandas.Series( [], dtype="object" )
            return
        logger = self.getlogger()
        oids = self._df.index.unique().values
        query = ( "SELECT object_id as oid,"
                  "  COUNT(username) FILTER (WHERE goodbad='good') AS ngoods,"
                  "  COUNT(username) FILTER (WHERE goodbad='bad') AS nbads,"
                  "  array_agg(username) AS users,"
                  "  array_agg(goodbad) AS goodbads "
                  "FROM scanscore "
                  "WHERE object_id IN %(oids)s "
                  "GROUP BY object_id" )
        with DB.get( curdb ) as db:
            cursor = db.db.cursor()
            cursor.execute( query, { 'oids': tuple(oids) } )
            vetdf = cursor_to_df(cursor, "oid")
            cursor.close()
        self._df = self._df.merge( vetdf, left_index=True, right_index=True, how='left' )
        self._df.fillna( { 'ngoods': 0, 'nbads': 0 }, downcast='infer', inplace=True )
        # Sadly, fillna won't work with a list
        self._df['users'] = self._df['users'].apply( lambda x : x if isinstance( x, list ) else [] )
        self._df['goodbads'] = self._df['goodbads'].apply( lambda x : x if isinstance( x, list ) else [] )
        
    # ======================================================================

    def get_goodbad_for_user( self, user, curdb=None ):
        goodbads = {}
        with DB.get(curdb) as db:
            cursor = db.db.cursor()
            query = ( "SELECT object_id AS oid,goodbad FROM scanscore WHERE username=%(user)s "
                      "AND object_id IN %(oids)s" )
            oids = self._df.index.unique().values
            cursor.execute( query, { 'user': user, 'oids': tuple(oids) } )
            rows = cursor.fetchall()
            cursor.close()
            
        for row in rows:
            goodbads[row['oid']] = row['goodbad']
        return goodbads

    # ======================================================================
        
    def sortrb( self, ascending=False ):
        self._df = self._df.sort_values( ['rb', 'oid'], ascending=[ascending, True] )

    def sortccdnum( self ):
        self._df = self._df.sort_values( ['ccdnum', 'oid'] )

    def sortoid( self ):
        self._df = self._df.sort_values( ['oid'] )

        
# ======================================================================

class ExposureTable(HasDF):

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )

    # ======================================================================

    @classmethod
    def load( cls, t0, t1, propids=None, includestack=True, onlystack=False,
              loadnobjs=True, rbtype=None, mingallat=None, maxgallat=None ):
        logger = cls.getlogger()
        
        mjd0 = mjd( t0.year, t0.month, t0.day, t0.hour, t0.minute, t0.second )
        mjd1 = mjd( t1.year, t1.month, t1.day, t1.hour, t1.minute, t1.second )
        
        with DB.get() as db:
            cursor = db.db.cursor()
            query = ( "SELECT e.id AS eid,e.filename,e.filter,e.proposalid,e.ra,e.dec,"
                      "  COUNT(s.id) AS nsubs,"
                      "  e.header->'EXPTIME' AS exptime "
                      "FROM exposures e "
                      "LEFT JOIN subtractions s ON s.exposure_id=e.id "
                      "WHERE e.mjd>=%(mjd0)s AND e.mjd<=%(mjd1)s " )
            subvars = { 'mjd0': mjd0, 'mjd1': mjd1 }
            if propids is not None:
                query += "AND proposalid IN %(propids)s "
                subvars['propids'] = tuple(propids)
            if not includestack:
                query += "AND NOT e.is_stack "
            elif onlystack:
                query += "AND e.is_stack "
            if ( mingallat is not None ) and ( mingallat > 0 ):
                query += "AND ( e.gallat<=-%(mingallat)s OR e.gallat>=%(mingallat)s ) "
                subvars['mingallat'] = mingallat
            if ( maxgallat is not None ) and ( maxgallat < 90 ):
                query += "AND ( e.gallat>=-%(maxgallat)s AND e.gallat<=%(maxgallat)s ) "
                subvars['maxgallat'] = maxgallat
            query += "GROUP BY e.id,e.filename,e.filter,e.proposalid,e.mjd,e.gallat ORDER BY e.mjd,e.filter"
            # logger.debug( cursor.mogrify( query, subvars ) )
            cursor.execute( query, subvars )
            df = cursor_to_df(cursor, "eid")
            cursor.close()
            
            exptab = cls( df )

            if loadnobjs:
                exptab.loadnobjs( curdb=db )
            if rbtype is not None:
                exptab.loadrbcount( rbtype=rbtype, curdb=db )

            return exptab

    # ======================================================================

    def loadnobjs( self, curdb=None ):
        if "nobjs" in self._df.columns:
            return
        if len(self._df) == 0:
            self._df["nobjs"] = pandas.Series( [], dtype="int" )
            return

        logger = self.getlogger()
        
        eids = self._df.index.unique().values

        with DB.get( curdb ) as db:
            cursor = db.db.cursor()
            query = ( "SELECT e.id AS eid,COUNT(o.id) AS nobjs "
                      "FROM exposures e "
                      "LEFT JOIN subtractions s ON e.id=s.exposure_id "
                      "LEFT JOIN objects o ON o.subtraction_id=s.id "
                      "WHERE e.id IN %(eids)s "
                      "GROUP BY e.id " )
            cursor.execute( query, { 'eids': tuple(eids) } )
            nobjtab = pandas.DataFrame( cursor.fetchall() )
            if len(nobjtab) == 0:
                nobjtab = self.columns_to_empty_df(cursor)
            nobjtab.set_index( "eid", inplace=True )
            cursor.close()
            self._df = self._df.merge( nobjtab, left_index=True, right_index=True, how='left' )
            self._df.fillna( { 'nobjs': 0 }, downcast='infer', inplace=True )
            
    # ======================================================================

    def loadrbcount( self, rbtype=1, curdb=None ):
        if "nbigrbs" in self._df.columns:
            return
        if len(self._df) == 0:
            self._df["nbigrbs"] = pandas.Series( [], dtype="int" )
            return
        with DB.get( curdb ) as db:
            cursor = db.db.cursor()
            query = ( "SELECT id,rbcut,cliplow,cliphigh,fits,subsignorm,clip,model,description "
                      "FROM rbtypes "
                      "WHERE id=%(rbtype)s" )
            cursor.execute( query, { 'rbtype': rbtype } )
            rows = cursor.fetchall()
            if len(rows) == 0:
                raise ValueError( f'Unknown rbtype {rbtype}' )
            self.rbinfo = rows[0]

            eids = self._df.index.unique().values
            
            query = ( "SELECT e.id AS eid,COUNT(r.id) FILTER (WHERE rb>%(rbcut)s) AS nbigrbs "
                      "FROM exposures e "
                      "INNER JOIN subtractions s ON e.id=s.exposure_id "
                      "INNER JOIN objects o ON o.subtraction_id=s.id "
                      "INNER JOIN objectrbs r ON r.object_id=o.id "
                      "WHERE e.id IN %(eids)s AND r.rb>%(rbcut)s AND rbtype_id=%(rbtype)s"
                      "GROUP BY e.id" )
            cursor.execute( query, { 'rbtype': rbtype, 'rbcut': self.rbinfo['rbcut'], 'eids': tuple(eids) } )
            rbtab = cursor_to_df(cursor, "eid")
            cursor.close()
            self._df = self._df.merge( rbtab, left_index=True, right_index=True, how='left' )
            self._df.fillna( { 'nbigrbs': 0 }, downcast='infer', inplace=True )
            
    # ======================================================================

    def load_nchips_checkpoint( self, checkpoint=27, column="nfinished", curdb=None ):
        logger = self.getlogger()
        if column in self._df.columns:
            return
        if len(self._df) == 0:
            self._df[column] = pandas.Series( [], dtype="int" )
            return
        eids = self._df.index.unique().values
        with DB.get(curdb) as db:
            cursor = db.db.cursor()
            query = ( "SELECT e.id AS eid,COUNT(p.id) AS %(column)s "
                      "FROM exposures e "
                      "INNER JOIN processcheckpoints p ON e.id=p.exposure_id "
                      "WHERE e.id IN %(eids)s AND p.event_id=%(checkpoint)s "
                      "GROUP BY e.id" )
            cursor.execute( query, { 'column': psycopg2.extensions.AsIs(column),
                                     'eids': tuple(eids), 'checkpoint': checkpoint } )
            checktab = cursor_to_df(cursor, "eid")
            cursor.close()
            self._df = self._df.merge( checktab, left_index=True, right_index=True, how='left' )
            self._df.fillna( { column: 0 }, downcast='infer', inplace=True )
        

    # ======================================================================

    def load_nerrors( self, curdb=None ):
        if "nerrors" in self._df.columns:
            return
        if len(self._df) == 0:
            self._df["nerrors"] = pandas.Series( [], dtype="int" )
            return
        eids = self._df.index.unique().values
        with DB.get(curdb) as db:
            cursor = db.db.cursor()
            query = ( "SELECT e.id AS eid,COUNT(p.id) AS nerrors "
                      "FROM exposures e "
                      "INNER JOIN processcheckpoints p ON e.id=p.exposure_id "
                      "WHERE e.id IN %(eids)s AND p.event_id=999 "
                      "GROUP BY e.id" )
            cursor.execute( query, { 'eids': tuple(eids) } )
            errtab = cursor_to_df( cursor, "eid" )
            cursor.close()
            self._df = self._df.merge( errtab, left_index=True, right_index=True, how='left' )
            self._df.fillna( { 'nerrors': 0 }, downcast='infer', inplace=True )
    
            
# ======================================================================

class RBTypeTable(HasDF):
    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )

    @classmethod
    def get( cls, curdb=None, logger=None ):
        logger = cls.getlogger( logger )

        with DB.get(curdb) as db:
            cursor = db.db.cursor()
            cursor.execute( "SELECT * FROM rbtypes" )
            df = cursor_to_df( cursor, "id" )
        return cls( df )

# ======================================================================

class Exposure():
    def __init__( self, filename, *args, **kwargs ):
        super().__init__( *args, **kwargs )
        self.filename = filename
        self.nccds = 64
        self.ccdlist = range( 1, 63 )
        
    def get_log( self, curdb=None ):
        with DB.get(curdb) as db:
            cursor = db.db.cursor()
            query = ( "SELECT p.created_at,p.ccdnum,p.running_node,p.mpi_rank,p.notes,c.description,p.event_id "
                      "FROM processcheckpoints p "
                      "INNER JOIN exposures e ON p.exposure_id=e.id "
                      "LEFT JOIN checkpointeventdefs c ON p.event_id=c.id "
                      "WHERE e.filename=%s ORDER BY p.created_at" )
            cursor.execute( query, ( self.filename, ) )
            df = cursor_to_df( cursor )
            return df

# ======================================================================

class Candidate():
    fields = [ 'created_at', 'modified', 'ra', 'dec', 'id' ]

    @classmethod
    def load( self, candid, curdb=None ):
        with DB.get( curdb ) as db:
            cursor = db.db.cursor()
            query = "SELECT id,created_at,modified,ra,dec FROM candidates WHERE id=%s"
            cursor.execute( query, ( candid, ) )
            rows = cursor.fetchall()
            cursor.close()
        if len(rows) == 0:
            return None
        if len(rows) > 1:
            raise ValueError( f'Candidate {candid} is mulitply defined; this shouldn\'t happen!' )
        row = rows[0]
        return Candidate( id=row['id'], created_at=row['created_at'], modified=row['modified'],
                          ra=row['ra'], dec=row['dec'] )
    
    def __init__( self, *args, **kwargs ):
        for field in self.fields:
            if field in kwargs:
                setattr( self, field, kwargs[field] )

# ======================================================================

def check_scan_user( data, curdb=None ):
    if "user" not in data:
        rval = { "error": "No user specified" }
        return json.dumps(rval)
    if "password" not in data:
        rval = { "error": "No password given" }
        return json.dumps(rval)
    with DB.get(curdb) as db:
        cursor = db.db.cursor()
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
    

def set_user_object_scanscore( user, oids, statuses, curdb=None ):
    with DB.get(curdb) as db:
        cursor = db.db.cursor()
        for status, strobjid in zip(statuses, oids):
            objid = int(strobjid)
            if status == "good": goodbad = True
            elif status == "bad": goodbad = False
            else:
                rval = { "error": f"{status} is neither good nor bad" }
                return json.dumps(rval)

            pkey = f"{objid}_{user}"
            query = ( "INSERT INTO scanscore(id,object_id,username,goodbad) VALUES(%s,%s,%s,%s) "
                      "ON CONFLICT ON CONSTRAINT scanscore_pkey DO UPDATE SET goodbad=%s" )
            cursor.execute( query, ( pkey, objid, user, status, status ) )
        db.db.commit()
        cursor.close()
        
        # This is hopefully reudndant, but read the status back
        cursor = db.db.cursor()
        query = ( "SELECT object_id,username,goodbad FROM scanscore "
                  "WHERE username=%(user)s AND object_id IN %(objids)s" )
        cursor.execute( query, { 'user': user, 'objids': tuple(oids) } )
        rows = cursor.fetchall()
        if len(rows) != len(oids):
            rval = { "error": f"Read back {len(rows)} statuses when expected {len(oids)}; "
                     f"the database might be OK, but this page is mucked up now.  Go back." }
            return json.dumps(rval)
        rval = { "objid": [ row['object_id'] for row in rows ],
                 "status": [ row['goodbad'] for row in rows ] }
        cursor.close()
        return json.dumps(rval)

        
# ======================================================================

def main():
    print( "Don't" )

if __name__ == "__main__":
    main()
