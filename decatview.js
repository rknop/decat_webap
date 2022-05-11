import { webapconfig } from "./decatview_config.js"
import { rkAuth } from "./rkauth.js"
import { rkWebUtil } from "./rkwebutil.js"


// Namespace

var decatview = {}

// **********************************************************************
// **********************************************************************
// **********************************************************************
// Overall context of webap

decatview.Context = function() {}

// **********************************************************************
// Initialize the webap context

decatview.Context.prototype.init = function() {
    var self = this;

    // Some hardcoded defaults
    this.mingallat = 0;
    this.maxgallat = 90;
    this.selectedrbtype = webapconfig.defaultrbtype;
    this.allproposalsorsome = "all";
    this.proposals = webapconfig.proposals;
    this.selectedproposals = webapconfig.defaultproposals;
    this.knownrbtypes = [];
    this.chosenrbtype = webapconfig.defaultrbtype;
    
    // Make the things
    this.connector = new rkWebUtil.Connector( webapconfig.webapurl );
    
    this.statusdiv = document.getElementById( "status-div" );
    this.maindiv = document.getElementById( "main-div" );
    if ( this.statusdiv == null ) {
        window.alert( "Coudn't find status div!  This should never happen." )
        return;
    }
    this.auth = new rkAuth( this.statusdiv, webapconfig.webapurl, function() { self.render_userstatus() } );
    this.auth.checkAuth( null, function() { self.promptlogin() } );
    this.render();
}

// **********************************************************************
// Prompt for login

decatview.Context.prototype.promptlogin = function() {
    var self = this;
    
    rkWebUtil.wipeDiv( this.statusdiv );
    // ROB!!! Check to see if logged in before saying not logged in!
    // (Or will this only be called in that case?)
    let p = rkWebUtil.elemaker( "p", this.statusdiv, { "classes": [ "smaller", "italic" ] } );
    p.appendChild( document.createTextNode( "Not logged in — " ) );
    rkWebUtil.elemaker( "span", p,
                        { "classes": [ "link" ],
                          "text": "Log In",
                          "click": function() { self.auth.showLoginUI() } } );
}

// **********************************************************************
// Render login information

decatview.Context.prototype.render_userstatus = function() {
    var self = this;

    rkWebUtil.wipeDiv( this.statusdiv );
    let p = rkWebUtil.elemaker( "p", this.statusdivl, { "classes": [ "smaller", "italic" ] } );
    p.appendChild( document.createTextNode( "Logged in as " + this.auth.username +
                                            " (" + this.auth.displayname + ") — " ) );
    rkWebUtil.elemaker( "span", p,
                        { "classes": [ "link" ],
                          "text": "Log OUt",
                          "click": function() {
                              self.connector.sendHttpRequest( "logout", function() { self.promptlogin() } )
                          }
                        } );
}

// **********************************************************************
// Return a "back to home" thingy

decatview.Context.prototype.backToHome = function( div ) {
    var self = this;
    var p = rkWebUtil.elemaker( "p", div, { "text": "Back to Home",
                                            "classes": [ "link" ],
                                            "click": function() { self.render(); } } );
}
    

// **********************************************************************
// Render the main page

decatview.Context.prototype.render = function() {
    let self = this;
    let p, h2, div, hr, option;

    rkWebUtil.wipeDiv( this.maindiv );
    
    // Exposure Search

    rkWebUtil.elemaker( "hr", this.maindiv );
    h2 = rkWebUtil.elemaker( "h2", this.maindiv, { "text": "Exposure Search" } )
    p = rkWebUtil.elemaker( "p", this.maindiv,
                            { "text": "Enter dates as \"yyyy-mm-dd\" or " +
                                      "\"yyyy-mm-dd hh:mm:ss\" or " +
                                      "\"yyyy-mm-dd hh:mm:ss-05:00\" (the last one indicating a time zone " +
                                      "that is 5 hours before UTC)" } )
    p = rkWebUtil.elemaker( "p", this.maindiv, { "text": "List exposures from " } );
    this.startdatewid = rkWebUtil.elemaker( "input", p,
                                         { "attributes": { "type": "text",
                                                           "size": 24 } } );
    if ( this.startdate != null ) {
        this.startdatewid.value = this.startdate;
    }
    p.appendChild( document.createTextNode( " to " ) );
    this.enddatewid = rkWebUtil.elemaker( "input", p,
                                       { "attributes": { "type": "text",
                                                         "size": 24 } } );
    if ( this.enddate != null ) {
        this.enddatewid.value = this.enddate;
    }
    
    p = rkWebUtil.elemaker( "p", this.maindiv, { "text": "Galactic latitudes between ± " } );
    this.mingallatwid = rkWebUtil.elemaker( "input", p,
                                            { "attributes": { "type": "numeric",
                                                              "size": 2,
                                                              "min": 0,
                                                              "max": 90,
                                                              "value": this.mingallat } } );
    p.appendChild( document.createTextNode( "° and " ) );
    this.maxgallatwid = rkWebUtil.elemaker( "input", p,
                                            { "attributes": { "type": "numeric",
                                                              "size": 2,
                                                              "min": 0,
                                                              "max": 90,
                                                              "value": this.maxgallat } } );
    p.appendChild( document.createTextNode( "°" ) );
    

    p = rkWebUtil.elemaker( "p", this.maindiv, { "text": "Use real/bogus type: " } );
    this.rbtypewid = rkWebUtil.elemaker( "select", p, );
    this.connector.sendHttpRequest( "/getrbtypes", {}, function( data ) { self.populateRbTypeWid( data ) } );
                                         
    p = rkWebUtil.elemaker( "p", this.maindiv );
    this.allorsomepropswid = rkWebUtil.elemaker( "select", p );
    option = rkWebUtil.elemaker( "option", this.allorsomepropswid,
                                 { "attributes": { "value": "all" },
                                   "text": "Include all propsal IDs" } );
    if ( this.allproposalsorsome == "all" ) {
        option.setAttribute( "selected", "selected" );
    }
    option = rkWebUtil.elemaker( "option", this.allorsomepropswid,
                                 { "attributes": { "value": "some" },
                                   "text": "Include only propsal IDs:" } );
    if ( this.allproposalsorsome == "some" ) {
        option.setAttribute( "selected", "selected" );
    }
    this.proplistdiv = rkWebUtil.elemaker( "div", this.maindiv );
    this.proplistdiv.style.display = "none";
    this.allorsomepropswid.addEventListener(
        "change",
        function() {
            this.allproposalsorsome = self.allorsomepropswid.value;
            rkWebUtil.hideOrShow( self.proplistdiv, self.allorsomepropswid.value, ["all"], ["some"] );
        } );
    this.propcheckboxes = [];
    for ( let prop in this.proposals ) {
        let checkbox = rkWebUtil.elemaker( "input", this.proplistdiv,
                                           { "attributes": { "type": "checkbox",
                                                             "value": prop,
                                                             "id": "propcheckbox-" + prop } } )
        rkWebUtil.elemaker( "label", this.proplistdiv,
                            { "attributes": { "for": "propcheckbox-" + prop },
                              "text": this.proposals[prop] });
        rkWebUtil.elemaker( "br", this.proplistdiv );
        if ( this.selectedproposals.includes( prop ) ) {
            checkbox.setAttribute( "checked", "checked" );
        }
        checkbox.addEventListener( "change", function() { self.updateSelectedProposals(); } );
        this.propcheckboxes.push( checkbox );
    }

    rkWebUtil.elemaker( "br", this.maindiv );
    rkWebUtil.button( this.maindiv, "List Exposures",
                      function() {
                          self.startdate = self.startdatewid.value;
                          self.enddate = self.enddatewid.value;
                          self.listExposures();
                      } );

    // Candidate Lookup

    rkWebUtil.elemaker( "hr", this.maindiv );
    h2 = rkWebUtil.elemaker( "h2", this.maindiv, { "text": "Candidate Lookup" } );
    p = rkWebUtil.elemaker( "p", this.maindiv, { "text": "Not implemented yet.",
                                                 "classes": [ "italic" ] } );
    
    // Candidate search
    
    rkWebUtil.elemaker( "hr", this.maindiv );
    h2 = rkWebUtil.elemaker( "h2", this.maindiv, { "text": "Candidate Search" } );
    p = rkWebUtil.elemaker( "p", this.maindiv, { "text": "Not implemented yet.",
                                                 "classes": [ "italic" ] } );

    // Candidate vetting

    // ROB! Check if authenticated
    rkWebUtil.elemaker( "hr", this.maindiv );
    h2 = rkWebUtil.elemaker( "h2", this.maindiv, { "text": "Candidate Vetting" } );
    p = rkWebUtil.elemaker( "p", this.maindiv, { "text": "Not implemented yet.",
                                                 "classes": [ "italic" ] } );
}

// **********************************************************************
// Populate r/b type widget

decatview.Context.prototype.populateRbTypeWid = function( data ) {
    var self = this;
    
    if ( data.hasOwnProperty( "error" ) ) {
        window.alert( data["error"] );
        return;
    }
    rkWebUtil.wipeDiv( this.rbtypewid );
    this.knownrbtypes = data["rbtypes"];
    for ( let rbinfo of this.knownrbtypes ) {
        let option = rkWebUtil.elemaker( "option", this.rbtypewid,
                                         { "attributes": { "value": rbinfo.id },
                                           "text": rbinfo.id + " — " + rbinfo.description } );
        if ( rbinfo.id == this.chosenrbtype ) {
            option.setAttribute( "selected", "selected" );
        }
    }
    this.rbtypewid.addEventListener( "change", function() { this.chosenrbtype = self.rbtypewid.value } );
}

// **********************************************************************

decatview.Context.prototype.updateSelectedProposals = function() {
    this.selectedproposals = [];
    for ( let checkbox of this.propcheckboxes ) {
        if ( checkbox.checked ) {
            this.selectedproposals.push( checkbox.value );
        }
    }
}

// **********************************************************************
// List exposures

decatview.Context.prototype.listExposures = function() {
    var self = this;

    rkWebUtil.wipeDiv( this.maindiv );
    this.backToHome( this.maindiv );

    let p = rkWebUtil.elemaker( "p", this.maindiv, { "text": "Loading exposures from " +
                                                 this.startdate + " to " + this.enddate } );
    if ( this.allproposalsorsome == "all" ) {
        p = rkWebUtil.elemaker( "p", this.maindiv, { "text": "Showing exposures from ALL proposals." } );
    }
    else {
        p = rkWebUtil.elemaker( "p", this.maindiv, { "text": "Showing exposures from proposals: " } );
        let first = true;
        let text = "";
        for ( let prop of this.selectedproposals ) {
            if ( first ) first=false;
            else text += ", ";
            text += prop;
        }
        p.appendChild( document.createTextNode( text ) );
    }

    p = rkWebUtil.elemaker( "p", this.maindiv, { "text": "r/b type is " + this.chosenrbtype } );
    for ( let rbinfo of this.knownrbtypes ) {
        if ( rbinfo.id == this.chosenrbtype ) {
            p.appendChild( document.createTextNode( "(" + rbinfo.description + ") ; cutoff is " + rbinfo.rbcut ) );
            this.rbcut = rbinfo.rbcut;
            break;
        }
    }

    this.exposuresdiv = rkWebUtil.elemaker( "div", this.maindiv );

    this.showExposures( this.startdatewid.value, this.enddatewid.value, this.chosenrbtype, this.rbcut,
                        this.selectedproposals );
}

// **********************************************************************
// Show tiles for up to 100 objects
//
// ROB : it's gratuitous to pass rbcut to the webap, since that
//  information is in the database that the server has.  I did it
//  because I was being lazy about constructing my queries server side.

decatview.Context.prototype.showExposures = function( t0text, t1text, rb, rbcut, props ) {
    var self = this;
    this.connector.sendHttpRequest( "findexposures",
                                    { "t0": t0text,
                                      "t1": t1text,
                                      "rbtype": rb,
                                      "rbcut": rbcut,
                                      "allorsomeprops": this.allproposalsorsome,
                                      "props": props },
                                    function(data) { self.actuallyShowExposures( data ) } );
}

decatview.Context.prototype.actuallyShowExposures = function( data ) {
    var self = this;
    
    if ( data.hasOwnProperty( "error" ) ) {
        window.alert( data["error"] );
        return;
    }

    var table = rkWebUtil.elemaker( "table", this.exposuresdiv, { "classes": [ "exposurelist" ] } );
    var tr = rkWebUtil.elemaker( "tr", table );
    rkWebUtil.elemaker( "th", tr, { "text": "Exposure" } );
    rkWebUtil.elemaker( "th", tr, { "text": "Band" } );
    rkWebUtil.elemaker( "th", tr, { "text": "propid" } );
    rkWebUtil.elemaker( "th", tr, { "text": "t_exp" } );
    rkWebUtil.elemaker( "th", tr, { "text": "ra" } );
    rkWebUtil.elemaker( "th", tr, { "text": "dec" } );
    rkWebUtil.elemaker( "th", tr, { "text": "l" } );
    rkWebUtil.elemaker( "th", tr, { "text": "b" } );
    rkWebUtil.elemaker( "th", tr, { "text": "#Subs" } );
    rkWebUtil.elemaker( "th", tr, { "text": "#Done" } );
    rkWebUtil.elemaker( "th", tr, { "text": "N.Srcs" } );
    rkWebUtil.elemaker( "th", tr, { "text": "rb≥cut" } );

    for ( let exposure of data["exposures"] ) {
        let td, button;
        let tr = rkWebUtil.elemaker( "tr", table );
        if ( exposure.is_stack ) {
            tr.classList.add( "stack" );
        } else {
            tr.classList.add( "notstack" );
        }
        rkWebUtil.elemaker( "td", tr, { "text": exposure.filename } );
        rkWebUtil.elemaker( "td", tr, { "text": exposure.filter } );
        rkWebUtil.elemaker( "td", tr, { "text": exposure.proposalid } );
        rkWebUtil.elemaker( "td", tr, { "text": exposure.exptime.toFixed(1) } );
        rkWebUtil.elemaker( "td", tr, { "text": exposure.ra.toFixed(4) } );
        rkWebUtil.elemaker( "td", tr, { "text": exposure.dec.toFixed(4) } );
        rkWebUtil.elemaker( "td", tr, { "text": exposure.gallong.toFixed(4) } );
        rkWebUtil.elemaker( "td", tr, { "text": exposure.gallat.toFixed(4) } );
        rkWebUtil.elemaker( "td", tr, { "text": exposure.numsubs } );
        rkWebUtil.elemaker( "td", tr, { "text": exposure.numdone } );
        rkWebUtil.elemaker( "td", tr, { "text": exposure.numobjs } );
        rkWebUtil.elemaker( "td", tr, { "text": exposure.numhighrbobjs } );
        td = rkWebUtil.elemaker( "td", tr );
        button = rkWebUtil.button( td, "Show Objects", function() { self.showExposureObjects( exposure.id ) } );
        td = rkWebUtil.elemaker( "td", tr );
        button = rkWebUtil.button( td, "Show Log", function() { self.showExposureLog( exposure.id ) } );
        if ( exposure.numerrors > 0 ) {
            rkWebUtil.elemaker( "td", tr, { "text": exposure.numerrors + " errors", "classes": [ "bad" ] } );
        }
    }
}


// **********************************************************************

export { decatview }
