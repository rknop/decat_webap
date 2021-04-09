webap = "https://c3.lbl.gov/raknop/viewexps.py/"

waitforresponse = function( request )
{
    var type;
    
    if (request.readyState === 4 && request.status === 200) {
        type = request.getResponseHeader("Content-Type");
        if (type === "application/json")
        {
            return true;
        }
        else
        {
            window.alert("Request didn't return JSON.  Everything is broken.  Panic.");
            return null;
        }
    }
    else if (request.readyState == 4) {
        window.alert("Woah, got back status " + request.status + ".  Everything is broken.  Panic.");
        return null;
    }
    else {
        return false;
    }
}


catchHttpResponse = function( req, handler, errorhandler )
{
    if ( ! waitforresponse( req ) ) return;
    var statedata = JSON.parse( req.responseText );
    if ( statedata.hasOwnProperty( "error" ) ) {
        window.alert( statedata.error );
        if ( errorhandler != null ) {
            errorhandler( statedata );
        }
        return;
    }
    handler( statedata );
}

sendgoodbad = function( user, password, obj, goodbad )
{
    var id = obj + "status";
    let elem = document.getElementById(id);
    elem = id.innerHTML = "...sending...";
    var req = new XMLHttpRequest();
    req.open( "POST", webap + "setgoodbad");
    req.onreadystatechange = function() {
        catchHttpResponse( req, function( statedata ) {
            echogoodbad( statedata );
        }, null )
    };
    req.setRequestHeader( "Content-Type", "application/json" );
    req.send( JSON.stringify( { "obj": obj, "password": password, "user": user, "status": goodbad } ) );
}

echogoodbad = function( statedata ) {
    var id = statedata["objid"] + "status";
    console.log( "Looking for element " + id );
    elem = document.getElementById(id);
    elem.innerHTML = statedata["status"];
}
