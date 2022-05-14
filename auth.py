#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import uuid
import pathlib
import binascii
import traceback
import json
from datetime import datetime
import pytz
import web
from web import form
import Cryptodome.PublicKey.RSA
import Cryptodome.Cipher.PKCS1_v1_5

scriptpath = pathlib.Path( __file__ )
sys.path.insert( 0, str(scriptpath.parent) )
import db

#### HOW TO USE
#
# This isn't as modular as I'd like.  Right now, you need to have
#   a module "db" that uses SQLAlchemy and implements the following classes:
#
# User
#   properties: id UUID,
#               displayname TEXT
#               email TEXT
#               pubkey TEXT
#               privkey TEXT
#               lastlogin TEXT
# PasswordLink
#   properties: id UUID
#               userid UUID with foreign key link to AuthUser.id
#               expires TIMESTAMP WITH TIME ZONE
#
# Add the module as a submodule to your main webap; the code there needs:
#
# import auth
# ...
# urls = ( ...
#          "/auth", auth.app
#        )
# ...
# app = web.application( urls, locals() )
# ...
# initializer = { ... }
# initializer.update( auth.initializer )
# session = web.session.Session( app, web.session.DiskStore(...), initalizer=initializer )
# def session_hook(): web.ctx.session = session 
# app.add_processor( web.loadhook( session_hook ) ) 
#
# web.smtp also needs to be fully configured
#
# Access the session via web.ctx.session
# Variables in the session:
#    authenticated : True or False
#    useruuid
#    username
#    userdisplayname
#    useremail
#    ( there's also authuuid, a temporary throwaway value )
#
# (Won't work with web.py templates, see https://webpy.org/cookbook/sessions_with_subapp )
#
# CLIENT SIDE: use rkauth.js and resetpasswd_start.js

# ======================================================================

class HandlerBase:
    def GET( self ):
        return self.do_the_things()

    def POST( self ):
        return self.do_the_things()

    def jsontop( self ):
        web.header('Content-Type', 'application/json')
    
# ======================================================================

class GetAuthChallenge(HandlerBase):
    def __init__( self ):
        super().__init__()

    def do_the_things( self ):
        self.jsontop()
        try:
            web.ctx.session.authenticated = False
            inputdata = json.loads( web.data().decode(encoding="utf-8") )
            if not 'username' in inputdata:
                return json.dumps( { 'error': 'No username sent to server' } )
            with db.DB.get() as dbsess:
                users = db.User.getbyusername( inputdata['username'] )
                if len(users) > 1:
                    return json.dumps( { 'error': f'User {inputdata["username"]} is multiply defined; this is bad!' } )
                if len(users) == 0:
                    return json.dumps( { 'error': f'No such user {inputdata["username"]}' } )
                user = users[0]
            tmpuuid = str( uuid.uuid4() )
            pubkey = Cryptodome.PublicKey.RSA.importKey( user.pubkey )
            cipher = Cryptodome.Cipher.PKCS1_v1_5.new( pubkey )
            challenge = binascii.b2a_base64( cipher.encrypt( tmpuuid.encode("UTF-8") ) ).decode( "UTF-8" )
            web.ctx.session.username = user.username
            web.ctx.session.useruuid = user.id
            web.ctx.session.userdisplayname = user.displayname
            web.ctx.session.useremail = user.email
            web.ctx.session.authuuid = tmpuuid
            retdata = { 'username': user.username,
                        'privkey': user.privkey,
                        'challenge': challenge }
            return json.dumps( retdata )
        except Exception as e:
            sys.stderr.write( f'{traceback.format_exc()}\n' )
            return json.dumps( { 'error': f'Exception in GetAuthChallenge: {str(e)}' } )

class RespondAuthChallenge(HandlerBase):
    def __init__( self ):
        super().__init__()

    def do_the_things( self ):
        self.jsontop()
        try:
            inputdata = json.loads( web.data().decode(encoding="utf-8") )
            if ( ( not 'username' in inputdata ) or
                 ( not 'response' in inputdata ) ):
                return json.dumps( { 'error': ( 'Login error: username or challenge response missing; '
                                                ' (you probably can\'t fix this, contact code maintainer) '
                                               ) } )
            if inputdata['username'] != web.ctx.session.username:
                return json.dumps( { 'error': ( f'username {username} didn\'t match session username '
                                                '{web.ctx.session.username}; try logging out and logging back in '
                                               ) } )
            if web.ctx.session.authuuid != inputdata['response']:
                return json.dumps( { 'error': 'Authentication failure.' } )
            web.ctx.session.authenticated = True
            return json.dumps(
                { 'status': 'ok',
                  'message': f'User {web.ctx.session.username} logged in.',
                  'username': web.ctx.session.username,
                  'useruuid': str( web.ctx.session.useruuid ),
                  'useremail': web.ctx.session.useremail,
                  'userdisplayname': web.ctx.session.userdisplayname,
                 } )
        except Exception as e:
            sys.stderr.write( f'{traceback.format_exc()}\n' )
            return json.dumps( { 'error': f'Exception in RespondAuthChallenge: {str(e)}' } )
        

# ======================================================================

class GetPasswordResetLink(HandlerBase):
    def __init__( self ):
        super().__init__()

    def do_the_things( self ):
        self.jsontop()
        try:
            inputdata = json.loads( web.data().decode(encoding="utf-8") )
            if 'username' in inputdata:
                username = inputdata['username']
                them = db.User.getbyusername( username );
                if len( them ) == 0:
                    return json.dumps( { "error": f"username {username} not known" } )
                if len( them ) > 1:
                    return json.dumps( { "error": ( f"username {username} is multiply defined! "
                                                    "This is bad.  This is very bad." ) } )
            elif 'email' in inputdata:
                email = inputdata['email']
                them = db.User.getbyemail( email )
                if len( them ) == 0:
                    return json.dumps( { "error": f"email {email} not known" } )
            sentto = ""
            for user in them:
                link = db.PasswordLink.new( user.id )
                web.sendmail( "DECat Webap <raknop@lbl.gov>", user.email, "DECat webap password reset",
                              f'Somebody requested a password reset for {user.username}\n' +
                              f'for the LBL DECat Webap.  This link will expire in 1 hour.\n'
                              f'If you did not request this, you may ignore this message.\n' +
                              f'Here is the link; cut and paste it into your browser:\n\n' +
                              f'{web.ctx.home}/resetpassword?uuid={str(link.id)}' )
                if len(sentto) > 0:
                    sentto += " "
                sentto += user.username
            return json.dumps( { 'status': f'Password reset link(s) sent for {sentto}.' } )
        except Exception as e:
            sys.stderr.write( f'{traceback.format_exc()}\n' )
            return json.dumps( { 'error': f'Exception in GetPasswordResetLink: {str(e)}' } )

        
# ======================================================================

class ResetPassword(HandlerBase):
    def __init__( self ):
        super().__init__()

    def do_the_things( self ):
        web.header('Content-Type', 'text/html; charset="UTF-8"')
        response = "<!DOCTYPE html>\n"
        response += "<html>\n<head>\n<meta charset=\"UTF-8\">\n"
        response += f"<title>Password Reset</title>\n"

        # webapdirurl = str( pathlib.Path( web.ctx.env['SCRIPT_NAME'] ).parent )
        webapdirurl = str( pathlib.Path( web.ctx.homepath ).parent.parent )
        if webapdirurl[-1] != '/':
            webapdirurl += "/"
        sys.stderr.write( f"In ResetPassword, webapdirurl is {webapdirurl}\n" )
        # response += "<link href=\"" + webapdirurl
        # response += "photodb.css\" rel=\"stylesheet\" type=\"text/css\">\n"
        response += "<script src=\"" + webapdirurl + "aes.js\"></script>\n"
        response += "<script src=\"" + webapdirurl + "jsencrypt.min.js\"></script>\n"
        response += "<script src=\"" + webapdirurl + "resetpasswd_start.js\" type=\"module\"></script>\n"
        response += "</head>\n<body>\n"
        response += f"<h1>Reset Password</h1>\n<p><b>ROB Todo: make this header better</b></p>\n";

        try:
            inputdata = web.input()
            if not hasattr( inputdata, "uuid" ):
                response += "<p>Malformed password reset URL.</p>"
                return response
            pwlink = db.PasswordLink.get( inputdata.uuid )
            if pwlink is None:
                response += "<p>Invalid password reset URL.</p>"
                return response
            if pwlink.expires < datetime.now(pytz.utc):
                response += "<p>Password reset link has expired.</p>"
                return response
            user = db.User.get( pwlink.userid )

            response += f"<h2>Reset password for {user.username}</h2>\n"
            response += "<div id=\"authdiv\">"
            response += "<table>\n"
            response += "<tr><td>New Password:</td><td>"
            response += form.Input( name="newpassword", id="reset_password",
                                         type="password", size=20 ).render()
            response += "</td></tr>\n"
            response += "<tr><td>Confirm:</td><td>"
            response += form.Input( name="confirmpassword", id="reset_confirm_password",
                                         type="password", size=20 ).render()
            response += "</td></tr>\n"
            response += "<tr><td colspan=\"2\">"
            response += form.Button( name="getnewpassword", id="setnewpassword_button",
                                          html="Set New Password" ).render()
            response += "</td></tr>\n</table>\n"
            response += "</div>\n"
            response += form.Input( name="linkuuid", id="resetpasswd_linkid", type="hidden",
                                         value=str(pwlink.id) ).render()
            
            response += "</body>\n</html>\n"
            return response
        except Exception as e:
            sys.stderr.write( f'{traceback.format_exc()}\n' )
            response += f'Exception in ResetPassword: {str(e)}'
            return response
                
            
# ======================================================================

class GetKeys(HandlerBase):
    def __init__( self ):
        super().__init__()

    def do_the_things( self ):
        self.jsontop()
        try:
            inputdata = json.loads( web.data().decode(encoding="utf-8") )
            if 'passwordlinkid' not in inputdata:
                return json.dumps( { "error": "No password link id specified" } )
            link = db.PasswordLink.get( inputdata['passwordlinkid'] )
            if link is None:
                return json.dumps( { "error": "Invalid password link id" } )
            if link.expires < datetime.now(pytz.utc):
                return json.dumps( { "error": "Password reset link has expired" } )
            keys = Cryptodome.PublicKey.RSA.generate( 2048 )
            return json.dumps( { "privatekey": keys.exportKey().decode("UTF-8"),
                                 "publickey": keys.publickey().exportKey().decode("UTF-8") } )
        except Exception as e:
            sys.stderr.write( f'{traceback.format_exc()}\n' )
            return json.dumps( { "error": f"Exception in GetKeys: {str(e)}" } )
            
# ======================================================================

class ChangePassword(HandlerBase):
    def __init__( self ):
        super().__init__()

    def do_the_things( self ):
        self.jsontop()
        try:
            inputdata = json.loads( web.data().decode(encoding="utf-8") )
            if not "passwordlinkid" in inputdata:
                return json.dumps( { "error": "Call to changepassword without passwordlinkid" } )
            if not "publickey" in inputdata:
                return json.dumps( { "error": "Call to changepassword without publickey" } )
            if not "privatekey" in inputdata:
                return json.dumps( { "error": "Call to changepassword without privatekey" } )

            with db.DB.get() as dbsess:
                pwlink = db.PasswordLink.get( inputdata['passwordlinkid'], curdb=dbsess )
                user = db.User.get( pwlink.userid, curdb=dbsess )
                user.pubkey = inputdata['publickey']
                user.privkey = inputdata['privatekey']
                dbsess.db.delete( pwlink )
                dbsess.db.commit()
                return json.dumps( { "status": "Password changed" } )
        except Exception as e:
            sys.stderr.write( f'{traceback.format_exc()}\n' )
            return json.dumps( { 'error': f'Exception in ChangePassword: {str(e)}' } )
            
            
# ======================================================================

class CheckIfAuth(HandlerBase):
    def __init__( self ):
        super().__init__()

    def do_the_things( self ):
        self.jsontop()
        # sys.stderr.write( f'In CheckIfAuth; web.ctx.session.authenticated={web.ctx.session.authenticated}\n' )
        if ( hasattr( web.ctx, 'session' ) and
             hasattr( web.ctx.session, 'authenticated' ) and
             web.ctx.session.authenticated ):
            return json.dumps( { 'status': True,
                                 'username': web.ctx.session.username,
                                 'useruuid': str( web.ctx.session.useruuid ),
                                 'useremail': web.ctx.session.useremail,
                                 'userdisplayname': web.ctx.session.userdisplayname,
                                } )
        return json.dumps( { 'status': False } );

# ======================================================================

class Logout(HandlerBase):
    def __init__( self ):
        super().__init__()

    def do_the_things( self ):
        self.jsontop()
        web.ctx.session.kill()
        return json.dumps( { 'status': 'Logged out' } )

# ======================================================================

initializer = { 'username': None,
                'useruuid': None,
                'userdisplayname': None,
                'useremail': None,
                'authenticated': False,
                'authuuid': None }
urls = ( "/getchallenge", "GetAuthChallenge",
         "/respondchallenge", "RespondAuthChallenge",
         "/getpasswordresetlink", "GetPasswordResetLink",
         "/resetpassword", "ResetPassword",
         "/getkeys", "GetKeys",
         "/changepassword", "ChangePassword",
         "/isauth", "CheckIfAuth",
         "/logout", "Logout"
)

app = web.application( urls, locals() )

