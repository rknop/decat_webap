import { webapconfig } from "./decatview_config.js"
import { rkAuth } from "./rkauth.js"
import { rkWebUtil } from "./rkwebutil.js"
import { ExposureList } from "./exposurelist.js"
import { DecatVetting } from "./vetting.js"

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
    this.allproposalsorsome = "some";
    this.proposals = webapconfig.proposals;
    this.selectedproposals = webapconfig.defaultproposals;
    this.knownrbtypes = [];
    this.chosenrbtype_exgal = webapconfig.defaultrbtype_exgal;
    this.chosenrbtype_gal = webapconfig.defaultrbtype_gal;
    this.chosenrbtype_candsearch = webapconfig.defaultrbtype_exgal;

    this.limitallproposalsorsome = "some";
    this.limitproposals = webapconfig.proposals;
    this.limitselectedproposals = webapconfig.defaultproposals;
    this.limits = {
        "startdate": { "value": "",
                       "use": false,
                       "desc": "Search objects detected starting: ",
                       "note": "GMT; yyyy-mm-dd or yyyy-mm-dd hh:mm:ss" },
        "enddate" : { "value": "",
                      "use": false,
                      "desc": "Search objects detected ending: ",
                      "note": "GMT; yyyy-mm-dd or yyyy-mm-dd hh:mm:ss" },
        "sncut" : { "value": 5,
                    "use": false,
                    "type": "number",
                    "min": 0.,
                    "max": 50.,
                    "step": 0.5,
                    "desc": "S/N cut",
                    "note": "(Remember only S/N≥5ish get saved in the first place)" },
        "userbcut": { "value": null,
                      "use": true,
                      "desc": "Only high r/b objects in initial search" },
        "startcount": { "value": "",
                        "use": false,
                        "desc": "Filter detection counts starting: ",
                        "note": "GMT; yyyy-mm-dd or yyyy-mm-dd hh:mm:ss" },
        "endcount": { "value": "",
                      "use": false,
                      "desc": "Filter detection counts ending: ",
                      "note": "GMT; yyyy-mm-dd or yyyy-mm-dd hh:mm:ss" },
        "diffdays" : { "value": 3,
                       "use": false,
                       "type": "number",
                       "min": 2,
                       "max": 10,
                       "step": 1,
                       "desc": "Days bet. first and last detection ≥",
                       "note": "Only considering high r/b" },
        "brightest" : { "value": 16,
                        "use": false,
                        "type": "number",
                        "min": 16.,
                        "max": 25.,
                        "step": 0.5,
                        "desc": "Min (brightest) magnitude ≥",
                        "note": "Only considering high r/b" },
        "dimmest" : { "value": 25.,
                      "use": false,
                      "type": "number",
                      "min": 16.,
                      "max": 25.,
                      "step": 0.5,
                      "desc": "Max (dimmest) magnitude ≤",
                      "note": "Only considering high r/b" },
        "numfilters": { "value": 3,
                        "use": false,
                        "type": "number",
                        "min": 2,
                        "max": 20,
                        "step": 1,
                        "desc": "Seen in at least this many bands:" },
        "numdets" : { "value": 4,
                      "use": true,
                      "type": "number",
                      "min": 1,
                      "max": 20,
                      "step": 1,
                      "desc": "Min # detections: " },
        "highrbdets" : { "value": 2,
                         "use": true,
                         "type": "number",
                         "min": 1,
                         "max": 20,
                         "step": 1,
                         "desc": "Min high r/b detections: " },
        "outsidedets": { "value": 2,
                         "use": false,
                         "type": "number",
                         "min": 1,
                         "max": 20,
                         "step": 1,
                         "desc": "Max # detections outside date range: ",
                         "note": "Ignored if \"Filter detection counts...\" aren't used." },
        "outsidehighrbdets": { "value": 2,
                               "use": true,
                               "type": "number",
                               "min": 1,
                               "max": 20,
                               "step": 1,
                               "desc": "Max high r/b detections outside date range: ",
                               "note": "Ignored if \"Filter detection counts...\" aren't used." },
        "gallatmin" : { "value": 20,
                        "use": true,
                        "type": "number",
                        "min": 0,
                        "max": 90,
                        "step": 1,
                        "desc": "|Galactic Latitude| ≥" },
        "gallatmax" : { "value": 90,
                        "use": false,
                        "type": "number",
                        "min": 0,
                        "max": 90,
                        "step": 1,
                        "desc": "|Galatic Latitude| ≤" },
        "ra" : { "value": 0.,
                 "use": false,
                 "grouphead": true,
                 "note": "° ",
                 "desc": "Search around ra",
                 "size": 9 },
        "dec" : { "value": 0.,
                  "use": null,
                  "groupprev": true,
                  "note": "° ",
                  "desc": " dec",
                  "size": 9 },
        "radius" : { "value": 0.001,
                     "use": null,
                     "groupprev": true,
                     "note": "°",
                     "desc": " radius",
                     "size": 9 }
    }
    
    // Make the things
    this.connector = new rkWebUtil.Connector( webapconfig.webapurl );
    
    this.statusdiv = document.getElementById( "status-div" );
    this.maindiv = document.getElementById( "main-div" );
    if ( this.statusdiv == null ) {
        window.alert( "Coudn't find status div!  This should never happen." )
        return;
    }
    this.auth = new rkAuth( this.statusdiv, webapconfig.webapurl,
                            function() { self.render_userstatus() },
                            function() { self.promptlogin() } );
    this.auth.checkAuth();
    this.render();
}

// **********************************************************************
// Render the main page

decatview.Context.prototype.render = function() {
    let self = this;
    let p, h2, div, hr, option, table, tr, td;

    rkWebUtil.wipeDiv( this.maindiv );

    // Clean out some things that may have been created by
    // subpages.  (This is ugly.  Refactor so that I dont'
    // have to know here what other functions made.
    for ( let prop of [ "exposurelister", "candsearchdiv" ] ) {
        if ( this.hasOwnProperty( prop ) )
            delete this[prop];
    }
    
    rkWebUtil.elemaker( "hr", this.maindiv );
    this.renderExposureSearch( this.maindiv );
    
    // Candidate Lookup

    rkWebUtil.elemaker( "hr", this.maindiv );
    this.renderCandidateLookup( this.maindiv );

    // Candidate search
    
    rkWebUtil.elemaker( "hr", this.maindiv );
    this.renderCandidateSearch( this.maindiv );
        
    // Candidate vetting

    rkWebUtil.elemaker( "hr", this.maindiv );
    this.vettingstartdiv = rkWebUtil.elemaker( "div", this.maindiv );
    this.renderVettingStart( this.vettingstartdiv );

    // Populate the rb type widgets
    
    this.connector.sendHttpRequest( "getrbtypes", {}, function( data ) { self.populateRbTypeWids( data ) } );
}

// **********************************************************************
// A dropdown widget for selecting a set of proposals
//
// div is the parent element for the widgets
//
// widtext is text that goes before " all proposal IDs" or " only
//   proposal IDs" in widgets.  Make this osmething like "Include" or
//   "Search"
//
// propprefix is a prefix for properties that get read from and stuffed
//   into this.

decatview.Context.prototype.proposalwid = function( div, widtext, propprefix ) {
    var self = this;
    var p, option;

    var allorsome = this[propprefix + "allproposalsorsome"];
    var proposals = this[propprefix + "proposals"];
    var selectedproposals = this[propprefix + "selectedproposals"];
    
    p = rkWebUtil.elemaker( "p", div );
    var allorsomepropswid = rkWebUtil.elemaker( "select", p );
    option = rkWebUtil.elemaker( "option", allorsomepropswid,
                                 { "attributes": { "value": "all" },
                                   "text": widtext + " all propsal IDs" } );
    if ( allorsome == "all" ) {
        option.setAttribute( "selected", "selected" );
    }
    option = rkWebUtil.elemaker( "option", allorsomepropswid,
                                 { "attributes": { "value": "some" },
                                   "text": widtext + " only propsal IDs:" } );
    if ( allorsome == "some" ) {
        option.setAttribute( "selected", "selected" );
    }
    var proplistdiv = rkWebUtil.elemaker( "div", div );
    rkWebUtil.hideOrShow( proplistdiv, self[propprefix + "allproposalsorsome"], ["all"], ["some"] )
    allorsomepropswid.addEventListener(
        "change",
        function() {
            let val = self[propprefix + "allorsomepropswid"].value
            self[propprefix + "allproposalsorsome"] = val;
            rkWebUtil.hideOrShow( self[propprefix + "proplistdiv"], val, ["all"], ["some"] );
        } );
    var propcheckboxes = [];
    for ( let prop in proposals ) {
        let checkbox = rkWebUtil.elemaker( "input", proplistdiv,
                                           { "attributes": { "type": "checkbox",
                                                             "value": prop,
                                                             "id": "propcheckbox-" + prop } } )
        rkWebUtil.elemaker( "label", proplistdiv,
                            { "attributes": { "for": "propcheckbox-" + prop },
                              "text": proposals[prop] });
        rkWebUtil.elemaker( "br", proplistdiv );
        if ( selectedproposals.includes( prop ) ) {
            checkbox.setAttribute( "checked", "checked" );
        }
        checkbox.addEventListener( "change", function() { self.updateSelectedProposals( propprefix ); } );
        propcheckboxes.push( checkbox );
    }

    this[propprefix + "allorsomepropswid"] = allorsomepropswid;
    this[propprefix + "proplistdiv"] = proplistdiv;
    this[propprefix + "propcheckboxes"] = propcheckboxes;
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

    // ROB!  I should probably make hooks instead of hardcoding calls here
    this.renderVettingStart( this.vettingstartdiv );
}

// **********************************************************************
// Render login information

decatview.Context.prototype.render_userstatus = function() {
    var self = this;

    rkWebUtil.wipeDiv( this.statusdiv );
    let p = rkWebUtil.elemaker( "p", this.statusdiv, { "classes": [ "smaller", "italic" ] } );
    p.appendChild( document.createTextNode( "Logged in as " + this.auth.username +
                                            " (" + this.auth.userdisplayname + ") — " ) );
    rkWebUtil.elemaker( "span", p,
                        { "classes": [ "link" ],
                          "text": "Log Out",
                          "click": function() {
                              self.auth.logout( function() {
                                  self.promptlogin();
                                  self.render();
                              } ); } } );

    // ROB!  I should probably make hooks instead of hardcoding calls here
    this.renderVettingStart( this.vettingstartdiv );
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
//

decatview.Context.prototype.renderExposureSearch = function( div ) {
    var self = this;
    let p, h2;

    h2 = rkWebUtil.elemaker( "h2", div, { "text": "Exposure Search" } )
    p = rkWebUtil.elemaker( "p", div,
                            { "text": "Enter dates as \"yyyy-mm-dd\" or \"yyyy-mm-dd hh:mm:ss\"" } );
    p = rkWebUtil.elemaker( "p", div, { "text": "List exposures from " } );
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
    p.appendChild( document.createTextNode( " UTC" ) );
    
    p = rkWebUtil.elemaker( "p", div, { "text": "Galactic latitudes between ± " } );
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

    p = rkWebUtil.elemaker( "p", div, { "text": "Use real/bogus type: for b<20°: " } );
    this.rbtypewid_gal = rkWebUtil.elemaker( "select", p, );
    rkWebUtil.elemaker( "span", p, { "text": "; for b≥20°: " } );
    this.rbtypewid_exgal = rkWebUtil.elemaker( "select", p, );
                                         
    this.proposalwid( div, "Include", "" );

    rkWebUtil.elemaker( "br", div );
    rkWebUtil.button( div, "List Exposures",
                      function() {
                          self.startdate = self.startdatewid.value;
                          self.enddate = self.enddatewid.value;
                          rkWebUtil.wipeDiv( self.maindiv );
                          self.backToHome( self.maindiv );
                          let div = rkWebUtil.elemaker( "div", self.maindiv );
                          let rbinfo = { "gal": null, "exgal": null };
                          for ( let which of [ "gal", "exgal" ] ) {
                              self["chosenrbtype_"+which] = self["rbtypewid_"+which].value;
                              for ( let rb of self.knownrbtypes ) {
                                  if ( rb.id == self["chosenrbtype_"+which] ) {
                                      rbinfo[which] = rb;
                                  }
                              }
                          }
                          let proplist = null;
                          if ( self.allproposalsorsome == "some" ) {
                              proplist = self.selectedproposals;
                          }
                          self.mingallat = self.mingallatwid.value;
                          self.maxgallat = self.maxgallatwid.value;
                          self.exposurelister = new ExposureList( div, self.startdate, self.enddate, rbinfo,
                                                                  proplist, self.mingallat, self.maxgallat,
                                                                  self.connector );
                          self.exposurelister.render();
                      } );

}

// **********************************************************************

decatview.Context.prototype.renderCandidateLookup = function( div ) {
    var self = this;
    let h2, p;
    
    h2 = rkWebUtil.elemaker( "h2", div, { "text": "Candidate Lookup" } );
    p = rkWebUtil.elemaker( "p", div );
    this.candidatelookupname = rkWebUtil.elemaker( "input", p, { "attributes": { "type": "text", "size": 12 } } );
    rkWebUtil.button( p, "Show Candidate", function() { self.lookupCandidate() } );
}


// **********************************************************************

decatview.Context.prototype.renderCandidateSearch = function( div ) {
    var self = this;
    let h2, table, tr, td, p;
    
    h2 = rkWebUtil.elemaker( "h2", div, { "text": "Candidate Search" } );

    rkWebUtil.elemaker( "p", div,
                        { "text": "Searches for objects using the checked criteria.  " +
                          "First searches for objects between the " +
                          "\"Search objects detected\" time limits, using the r/b and SN cuts if selected.  " +
                          "After that, it looks at found objects in the \"Filter detection counts\" time limits " +
                          "(for things like counts of objects, date range object was seen, etc.)" } );

    p = rkWebUtil.elemaker( "p", div, { "text": "Use real/bogus type: " } );
    this.rbtypewid_candsearch = rkWebUtil.elemaker( "select", p );

    this.proposalwid( div, "Search", "limit" );
    rkWebUtil.elemaker( "br", div );
    
    table = rkWebUtil.elemaker( "table", div, { "classes": [ "candsearchparams" ] } );

    for ( let limitname in this.limits ) {
        let limit = this.limits[limitname];
        if ( limit.groupprev == undefined ) tr = rkWebUtil.elemaker( "tr", table );
        if ( limit.groupprev == undefined ) td = rkWebUtil.elemaker( "td", tr );
        if ( limit.use != null ) {
            limit.checkbox = rkWebUtil.elemaker( "input", td, { "attributes": { "type": "checkbox", } } );
            if ( limit.use ) limit.checkbox.setAttribute( "checked", "checked" );
            limit.checkbox.addEventListener( "change", function() { limit.use = limit.checkbox.checked; } );
        }
        if ( limit.grouphead ) {
            td = rkWebUtil.elemaker( "td", tr, { "attributes": { "colspan": 3 },
                                                 "classes": [ "left" ] } );
            // td.style.textAlign = 'left';
        }
        else if ( limit.groupprev == undefined ) td = rkWebUtil.elemaker( "td", tr );
        td.appendChild( document.createTextNode( limit.desc ) );
        if ( limit.groupprev == undefined && limit.grouphead == undefined ) {
            td = rkWebUtil.elemaker( "td", tr, { "classes": [ "left" ] } );
            // td.style.textAlign = "left";
        }
        if ( limit.value != null ) {
            limit.wid = rkWebUtil.elemaker( "input", td, { "attributes": { "value": limit.value } } );
            for ( let property of [ "type", "min", "max", "step", "size" ] ) {
                if ( limit[property] != undefined ) {
                    limit.wid.setAttribute( property, limit[property] );
                }
            }
            limit.wid.addEventListener( "change", function() { limit.value = limit.wid.value } );
        }
        if ( limit.note != undefined ) {
            if ( limit.groupprev == undefined && limit.grouphead == undefined ) {
                td = rkWebUtil.elemaker( "td", tr, { "classes": [ "left" ] } );
                // td.style.textAlign = "left";
            }
            td.appendChild( document.createTextNode( limit.note ) );
        }
    }

    p = rkWebUtil.elemaker( "p", div );
    rkWebUtil.button( p, "Seach for Candiates",
                      function() { self.searchForCandidates() } );
}

// **********************************************************************

decatview.Context.prototype.renderVettingStart = function( div ) {
    var self = this;
    let h2, table, tr, td, p;

    rkWebUtil.wipeDiv( div );
    h2 = rkWebUtil.elemaker( "h2", div, { "text": "Candidate Vetting" } );

    if ( ! this.auth.authenticated ) {
        rkWebUtil.elemaker( "p", this.vettingstartdiv,
                            { "text": "Log in to do candidate vetting.",
                              "classes": [ "italic" ] } );
        return;
    }

    p = rkWebUtil.elemaker( "p", div, { "text": "For a description of the vetting process, and examples of " +
                                        "\"good\" and \"bad\" candidates, please see " } );
    rkWebUtil.elemaker( "a", p, { "text": "\"Vetting detections on subtractions\"",
                                  "classes": [ "link" ],
                                  "attributes": { "href": "../vetting.html" } } );
    
    table = rkWebUtil.elemaker( "table", div, { "classes" : [ "candsearchparams" ] } );
    tr = rkWebUtil.elemaker( "tr", table );
    td = rkWebUtil.elemaker( "td", tr, { "text": "Type of field:" } );
    td = rkWebUtil.elemaker( "td", tr, { "classes": [ "left" ] } );
    this.vetgalorexgalwid = rkWebUtil.elemaker( "select", td );
    for ( let which of [ "Galactic", "Extragalactic" ] ) {
        let option = rkWebUtil.elemaker( "option", this.vetgalorexgalwid,
                                         { "attributes": { "value": which },
                                           "text": which } );
        if ( webapconfig.vetdefaultgalexgal == which ) {
            option.setAttribute( "selected", "selected" );
        }
    }
    tr = rkWebUtil.elemaker( "tr", table );
    td = rkWebUtil.elemaker( "td", tr, { "text": "Object vetted by others?" } );
    td = rkWebUtil.elemaker( "td", tr, { "classes": [ "left" ] } );
    this.vetbyotherswid = rkWebUtil.elemaker( "select", td );
    for ( let which of [ "Yes", "No" ] ) {
        let option = rkWebUtil.elemaker( "option", this.vetbyotherswid,
                                         { "attributes": { "value": which },
                                           "text": which } );
        if ( webapconfig.vetdefaultshowothers == which ) {
            option.setAttribute( "selected", "selected" );
        }
    }
    tr = rkWebUtil.elemaker( "tr", table );
    td = rkWebUtil.elemaker( "td", tr );
    rkWebUtil.button( td, "Show Objects",
                      function() {
                          rkWebUtil.wipeDiv( self.maindiv );
                          self.backToHome( self.maindiv );
                          let div = rkWebUtil.elemaker( "div", self.maindiv );
                          self.vetter = new DecatVetting( div, self.auth, self.connector,
                                                          self.vetgalorexgalwid.value,
                                                          self.vetbyotherswid.value );
                          self.vetter.render();
                      } );
    td = rkWebUtil.elemaker( "td", tr, { "attributes": { "colspan": 2 },
                                         "text": "(This may take several seconds, be patient.)" } );
}

// **********************************************************************
// Populate r/b type widget

decatview.Context.prototype.populateRbTypeWids = function( data ) {
    var self = this;
    
    if ( data.hasOwnProperty( "error" ) ) {
        window.alert( data["error"] );
        return;
    }
    this.knownrbtypes = data["rbtypes"];
    for ( let which of [ "gal", "exgal", "candsearch" ] ) {
        rkWebUtil.wipeDiv( this["rbtypewid_" + which] );
        for ( let rbinfo of this.knownrbtypes ) {
            let option = rkWebUtil.elemaker( "option", this["rbtypewid_" + which],
                                             { "attributes": { "value": rbinfo.id },
                                               "text": rbinfo.id + " — " + rbinfo.description } );
            if ( rbinfo.id == this["chosenrbtype_" + which] ) {
                option.setAttribute( "selected", "selected" );
            }
        }
        this["rbtypewid_"+which].addEventListener( "change",
                                                   function()
                                                   { self["chosenrbtype_"+which] = self["rbtypewid_"+which].value } );
    }
}

// **********************************************************************

decatview.Context.prototype.updateSelectedProposals = function( propprefix ) {
    this[propprefix + "selectedproposals"] = [];
    for ( let checkbox of this[propprefix + "propcheckboxes"] ) {
        if ( checkbox.checked ) {
            this[propprefix + "selectedproposals"].push( checkbox.value );
        }
    }
}


// **********************************************************************

decatview.Context.prototype.lookupCandidate = function() {
    var candid = this.candidatelookupname.value;
    var href = webapconfig.webapurl + "cand/" + this.candidatelookupname.value
    href += "?rbtype=" + this.chosenrbtype;
    window.open( href, "_self" );
}

// **********************************************************************

decatview.Context.prototype.searchForCandidates = function() {
    var self = this;
    var data = {};

    rkWebUtil.wipeDiv( this.maindiv );
    this.backToHome( this.maindiv );
    rkWebUtil.elemaker( "h2", this.maindiv, { "text": "Candidate Search" } );

    this.searchinfodiv = rkWebUtil.elemaker( "div", this.maindiv );
    
    data.rbtype = this.chosenrbtype_candsearch;
    if ( this.limitallproposalsorsome == "all" ) {
        data.allorsome = "all";
        rkWebUtil.elemaker( "p", this.searchinfodiv, { "text": "Searching all proposals" } );
    } else {
        data.allorsome = "some";
        data.proposals = this.limitselectedproposals;
        rkWebUtil.elemaker( "p", this.searchinfodiv, { "text": "Searching proposals: " + this.limitselectedproposals } );
    }

    var ul = rkWebUtil.elemaker( "ul", this.searchinfodiv, );
    for ( let limit in this.limits ) {
        data["use" + limit] = this.limits[limit].use;
        data[limit] = this.limits[limit].value;
        if ( this.limits[limit].use ) {
            rkWebUtil.elemaker( "li", ul, { "text": "Limit " + limit + " : " + this.limits[limit].value } );
        }
    }
    
    this.candsearchdiv = rkWebUtil.elemaker( "div", this.maindiv );
    rkWebUtil.elemaker( "span", this.candsearchdiv,
                        { "text": "Searching for candidates...", "classes": [ "warning" ] } );

    this.connector.sendHttpRequest( "searchcands", data, function( res ) { self.ingestAndShowCands( res ) } )
}

// **********************************************************************

decatview.Context.prototype.ingestAndShowCands = function( data ) {
    var p = rkWebUtil.elemaker( "p", this.searchinfodiv );
    p.appendChild( document.createTextNode( "Initially found: " + data['ninitiallyfound'] ) )
    rkWebUtil.elemaker( "br", p );
    p.appendChild( document.createTextNode( "After limiting initial search to high r/b: " + data['nhighrb'] ) );
    rkWebUtil.elemaker( "br", p );
    p.appendChild( document.createTextNode( "After object (maybe high/sn) count filter: " + data['nobjcount'] ) );
    rkWebUtil.elemaker( "br", p );
    p.appendChild( document.createTextNode( "After date/mag/rb/sn filter: " + data['ndatemagrbsnfiltered'] ) );
    rkWebUtil.elemaker( "br", p );
    p.appendChild( document.createTextNode( "After outside-date-range filter: " + data['noutsidedate'] ) );
    rkWebUtil.elemaker( "br", p );
    p.appendChild( document.createTextNode( "Final number of candidates found: " + data['n'] ) );

    this.showcandsrows = []
    for ( let row of data['candidates'] ) {
        // The "highsn" variables here are misnamed.  Really it's
        //    "in date range", and *may* include a s/n clut
        this.showcandsrows.push(
            { 'id': row.id,
              'numobjs': row.numhighsn,
              'numhighrb': row.numhighrb,
              'numhighrbfilt': row.highrbfiltcount,
              'numfilt': row.filtcount,
              'highrbminmjd': row.highrbminmjd,
              'highrbmaxmjd': row.highrbmaxmjd,
              'highrbdeltamjd': row.highrbmaxmjd - row.highrbminmjd,
              'highrbminmag': row.highrbminmag,
              'highrbmaxmag': row.highrbmaxmag,
              'minmjd': row.highsnminmjd,
              'maxmjd': row.highsnmaxmjd,
              'deltamjd': row.highsnmaxmjd - row.highsnminmjd,
              'minmag': row.highsnminmag,
              'maxmag': row.highsnmaxmag,
              'totnobjs': row.totnobjs,
              'totmaxmjd': row.totmaxmjd,
              'totminmjd': row.totminmjd,
              'totdeltamjd': row.totmaxmjd - row.totminmjd,
              'totmaxmag': row.totmaxmag,
              'totminmag': row.totminmag,
              'fracin': row.numhighsn / row.totnobjs,
            }
        )
    }

    this.showcandsortkey = "id";
    this.showcandsortascending = true;
    this.showCands();
}

decatview.Context.prototype.sortCandsAndShow = function( key, ascending=true ) {
    if ( ( this.showcandsortkey == key ) && ( this.showcandsortascending == ascending ) ) return;
    
    if ( ascending ) {
        this.showcandsrows.sort( function( a, b ) {
            if ( a[key] > b[key] ) return 1;
            else if ( a[key] < b[key] ) return -1;
            else return 0;
        } );
    } else {
        this.showcandsrows.sort( function( a, b ) {
            if ( a[key] < b[key] ) return 1;
            else if ( a[key] > b[key] ) return -1;
            else return 0;
        } );
    }
    this.showcandsortkey = key;
    this.showcandsortascending = ascending;
    this.showCands();
}

decatview.Context.prototype.showCands = function() {
    var table, th, tr, td, a, href, span;
    var self = this;
    
    rkWebUtil.wipeDiv( this.candsearchdiv );
    table = rkWebUtil.elemaker( "table", this.candsearchdiv, { "classes": [ "candlist" ] } );

    tr = rkWebUtil.elemaker( "tr", table );
    rkWebUtil.elemaker( "th", tr );
    rkWebUtil.elemaker( "th", tr, { "text": "Within selected dates",
                                    "attributes": { "colspan": 7 } } );
    rkWebUtil.elemaker( "th", tr, { "text": "Overall",
                                    "classes": [ "borderleft" ],
                                    "attributes": { "colspan": 4 } } );
    tr = rkWebUtil.elemaker( "tr", table );
    let first = true;
    let fields = [ [ "Candidate", "id" ],
                   [ "N.Objs", "numobjs" ],
                   [ "N.rb≥cut", "numhighrb" ],
                   [ "N.filters", "numfilt" ],
                   [ "Min MJD", "minmjd" ],
                   [ "Δt", "deltamjd" ],
                   [ "Min Mag", "minmag" ],
                   [ "Max Mag", "maxmag" ],
                   [ "N.Objs", "totnobjs" ],
                   [ "Δt", "totdeltamjd" ],
                   [ "Max Mag", "totmaxmag" ],
                   [ "Min Mag", "totminmag" ],
                   [ "Nin/Ntot", "fracin" ] ];
    for ( let field of fields ) {
        let hdr = field[0];
        let key = field[1];
        let stuff = { "text": hdr };
        if ( hdr == "N.Objs") {
            if ( first ) first = false;
            else stuff["classes"] = [ "borderleft" ];
        }
        if ( hdr == "Nin/Ntot" ) stuff['classes'] = [ 'borderleft' ];
        th = rkWebUtil.elemaker( "th", tr, stuff );
        span = rkWebUtil.elemaker( "span", th, { "text": "▲",
                                                 "click": function(e) { self.sortCandsAndShow( key, true ); } } );
        if ( ( this.showcandsortkey == key ) && ( this.showcandsortascending ) )
            span.classList.add( "good" );
        span.classList.add( "pointer" );
        span = rkWebUtil.elemaker( "span", th, { "text": "▼",
                                                 "click": function(e) { self.sortCandsAndShow( key, false ); } } );
        if ( ( this.showcandsortkey == key ) && ( ! this.showcandsortascending ) )
            span.classList.add( "good" );
        span.classList.add( "pointer" );
    }

    var decimals = { 'minmjd': 3,
                     'deltamjd': 3,
                     'minmag': 2,
                     'maxmag': 2,
                     'totdeltamjd': 3,
                     'totminmag': 2,
                     'totmaxmag': 2,
                     'fracin': 3,
                   }
    console.log( "this.showcandsrows.length = " + this.showcandsrows.length );
    for ( let row of this.showcandsrows ) {
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr );
        href = webapconfig.webapurl + "cand/" + row.id;
        if ( this.chosenrbtype_candsearch != null ) href += "?rbtype=" + this.chosenrbtype_candsearch;
        a = rkWebUtil.elemaker( "a", td, { "text": row["id"],
                                           "classes": [ "link" ],
                                           "attributes": { "href": href, "target": "_blank" } } );
        for ( let prop of [ "numobjs", "numhighrb", "numfilt",
                            "minmjd", "deltamjd", "minmag", "maxmag",
                            "totnobjs", "totdeltamjd", "totminmag", "totmaxmag", "fracin" ] ) {
            let stuff;
            if ( row.hasOwnProperty( prop ) ) {
                if ( row[prop] == undefined ) {
                    console.log( "row[" + prop + "] is undefined." );
                    stuff = { "text": "<undef>" };
                } else {
                    stuff = { "text": row[prop] };
                    if ( decimals.hasOwnProperty( prop ) ) {
                        stuff = { "text": row[prop].toFixed( decimals[prop] ) };
                    }
                }
            } else {
                stuff = { "text": "<missing>" };
            }
            if ( prop == "totnobjs" || prop == "fracin" ) stuff["classes"] = [ "borderleft" ];
            td = rkWebUtil.elemaker( "td", tr, stuff );
        }
    }

}
    

// **********************************************************************

export { decatview }
