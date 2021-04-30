webap = "https://c3.lbl.gov/raknop/decat/view/decatview.py/"

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

get_show_exp_data = function( exposure )
{
    var data = {};
    data.ccds = document.getElementById("ccds").value;
    data.orderby = document.getElementById("real/bogus").value;
    data.showrb = document.getElementById("showrb").value;
    data.offset = document.getElementById("offset").value;
    data.date0 = document.getElementById("date0").value;
    data.date1 = document.getElementById("date1").value;
    data.exposure = exposure;
    return data;
}

send_show_exp_request = function( reqdata )
{
    var form = document.createElement( "form" );
    form.setAttribute( "method", "post" );
    form.setAttribute( "action", webap + "showexp" );
    for ( let i in reqdata ) {
        let input = document.createElement( "input" );
        input.setAttribute( "name", i );
        input.setAttribute( "type", "hidden" );
        input.setAttribute( "value", reqdata[i] );
        form.appendChild( input );
    }
    console.log( "Submitting form to " + form.getAttribute("action") );
    document.body.appendChild( form );
    form.submit()
}

showobjects = function( exposure )
{
    var reqdata = get_show_exp_data( exposure )
    reqdata.whattodo = "Show Objects";
    send_show_exp_request( reqdata );
}

showlog = function( exposure )
{
    var reqdata = get_show_exp_data( exposure )
    reqdata.whattodo = "Show Log";
    send_show_exp_request( reqdata );
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
