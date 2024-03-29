import { webapconfig } from "./decatview_config.js"
import { rkAuth } from "./rkauth.js"
import { rkWebUtil } from "./rkwebutil.js"
import { ExposureList } from "./exposurelist.js"
import { DecatVetting } from "./vetting.js"
import { SVGPlot } from "./svgplot.js"

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
    this.versiontags = null;
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
                       "note": "GMT; yyyy-mm-dd or yyyy-mm-dd hh:mm:ss or mjd" },
        "enddate" : { "value": "",
                      "use": false,
                      "desc": "Search objects detected ending: ",
                      "note": "GMT; yyyy-mm-dd or yyyy-mm-dd hh:mm:ss or mjd" },
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
                        "note": "GMT; yyyy-mm-dd or yyyy-mm-dd hh:mm:ss or mjd" },
        "endcount": { "value": "",
                      "use": false,
                      "desc": "Filter detection counts ending: ",
                      "note": "GMT; yyyy-mm-dd or yyyy-mm-dd hh:mm:ss or mjd" },
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
    for ( let prop of [ "exposurelister", "candsearchobj" ] ) {
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
// A dropdown widget for version tags

decatview.Context.prototype.versiontagwid = function( parentelem ) {
    var self=this;

    var vtwid = rkWebUtil.elemaker( "select", parentelem );
    if ( this.versiontags == null ) {
        this.connector.sendHttpRequest( "getversiontags", {},
                                        function( data ) { self.fillversiontagwid( data, vtwid ); } );
    }
    else {
        this.fillversiontagwid( null, vtwid );
    }
    return vtwid;
}

decatview.Context.prototype.fillversiontagwid = function( data, vtwid ) {
    if ( data != null ) {
        if ( data.hasOwnProperty( "error") ) {
            window.alert( "Error getting version tags: " + data["error"] + "; things are broken." );
            return;
        }
        else if ( data["status"] != "ok" ) {
            window.alert( "Unexpected response getting version tags; things are broken." );
            return;
        }
        this.versiontags = data["versiontags"];
    }
    let first = true;
    for ( let vt of this.versiontags ) {
        console.log( "Adding version tag " + vt );
        let option = rkWebUtil.elemaker( "option", vtwid,
                                         { "attributes": { "value": vt.id },
                                           "text": vt.tag + " — " + vt.description } );
        if ( first ) {
            option.setAttribute( "selected", "selected" );
            first = false;
        }
    }
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

    p = rkWebUtil.elemaker( "p", div, { "text": "Version tag for counting subtractions, objects: " } );
    this.exposuresearch_versiontagwid = this.versiontagwid( p );
    
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
                          let vtid = self.exposuresearch_versiontagwid.value;
                          let vtdesc = null;
                          for ( let i in self.versiontags ) {
                              if ( vtid == self.versiontags[i].id ) {
                                  vtdesc = self.versiontags[i].tag + " — " + self.versiontags[i].description;
                                  break;
                              }
                          }
                          self.exposuresearch_versiontag = vtid;
                          self.exposurelister = new ExposureList( div, self.startdate, self.enddate, rbinfo,
                                                                  proplist, self.mingallat, self.maxgallat,
                                                                  vtid, vtdesc, self.connector );
                          self.exposurelister.render();
                      } );

}

// **********************************************************************

decatview.Context.prototype.renderCandidateLookup = function( div ) {
    var self = this;
    let h2, p;
    
    h2 = rkWebUtil.elemaker( "h2", div, { "text": "Candidate Lookup" } );

    p = rkWebUtil.elemaker( "p", div, { "text": "Version tag for object data: " } );
    this.candidatelookup_versiontagwid = this.versiontagwid( p );

    p.appendChild( document.createTextNode( " (Currently ignored, just uses 'latest')" ) );
    
    p = rkWebUtil.elemaker( "p", div );
    this.candidatelookupname = rkWebUtil.elemaker( "input", p, { "attributes": { "type": "text", "size": 12 } } );
    rkWebUtil.button( p, "Show Candidate", function() { self.lookupCandidate() } );
}


// **********************************************************************

decatview.Context.prototype.renderCandidateSearch = function( div ) {
    var self = this;
    let h2, table, tr, td, p;
    
    h2 = rkWebUtil.elemaker( "h2", div, { "text": "Candidate Search" } );

    p = rkWebUtil.elemaker( "div", div );
    p.style.max_width = "80ex";
    p.innerHTML = `
<p>Search for objects using the checked criteria.</p>
<ul style="max-width: 80ex">
<li><p>First searches for objects between the "Search objects detected"
    times.  The first four criteria apply to this search.  All objects
    seen in the date range (if checked) above the S/N cut (with only
    high r/b objects included if that option is checked) will be
    found.</p></li>
<li><p>Filters that search based on the rest of the criteria (using those
    that are checked.)  That filter is based on data in the "Filter
    detection counts" range, which can be different from the initial
    search range.  You can use this to reject candidates that have too
    many detections outside the filter range, for instance.</p></li>
</ul>
`;
    p = rkWebUtil.elemaker( "p", div, { "text": "Use real/bogus type: " } );
    this.rbtypewid_candsearch = rkWebUtil.elemaker( "select", p );

    p = rkWebUtil.elemaker( "p", div, { "text": "Use Version tag: " } );
    this.candidatesearch_versiontagwid = this.versiontagwid( p );
    
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
    td = rkWebUtil.elemaker( "td", tr, { "text": "Version tag:" } );
    td = rkWebUtil.elemaker( "td", tr, { "classes": [ "left" ] } );
    this.vet_versiontagwid = this.versiontagwid( td );
    td.appendChild( document.createTextNode( " (Use \"latest\" if unsure.)" ) );
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
                                         "classes": [ "left" ],
                                         "text": "(This may take several seconds, be patient.)" } );

    rkWebUtil.elemaker( "h4", div, { "text": "Vetting Stats" } );
    this.vettingtable = rkWebUtil.elemaker( "table", div, { "classes": [ "candsearchparams" ] } );
    tr = rkWebUtil.elemaker( "tr", this.vettingtable );
    td = rkWebUtil.elemaker( "td", tr, { "text": "Your exgal:" } )
    this.exgalyouvevetted = rkWebUtil.elemaker( "td", tr, { "classes": [ "warning" ], "text": "...loading..." } );
    tr = rkWebUtil.elemaker( "tr", this.vettingtable );
    td = rkWebUtil.elemaker( "td", tr, { "text": "Your gal:" } )
    this.galyouvevetted = rkWebUtil.elemaker( "td", tr, { "classes": [ "warning" ], "text": "...loading..." } );

    this.connector.sendHttpRequest( "getvetstats", {}, function( data ) { self.renderVetStats( data ) } );
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

decatview.Context.prototype.renderVetStats = function( data ) {
    var tr;
    
    rkWebUtil.wipeDiv( this.exgalyouvevetted );
    rkWebUtil.wipeDiv( this.galyouvevetted );
    this.exgalyouvevetted.classList.remove( "warning" );
    this.galyouvevetted.classList.remove( "warning" );
    this.exgalyouvevetted.appendChild( document.createTextNode( data.youexgal ) );
    this.galyouvevetted.appendChild( document.createTextNode( data.yougal ) );

    tr = rkWebUtil.elemaker( "tr", this.vettingtable );
    rkWebUtil.elemaker( "th", tr, { "text": "Extragalactic Counts", "attributes": { "colspan": 2 } } );
    tr = rkWebUtil.elemaker( "tr", this.vettingtable );
    rkWebUtil.elemaker( "th", tr, { "text": "N. Vets" } );
    rkWebUtil.elemaker( "th", tr, { "text": "N. Objects" } );

    for ( let nexgal of data['nexgal'] ) {
        tr = rkWebUtil.elemaker( "tr", this.vettingtable );
        rkWebUtil.elemaker( "td", tr, { "text": nexgal[0] } );
        rkWebUtil.elemaker( "td", tr, { "text": nexgal[1] } );
    }
        
    tr = rkWebUtil.elemaker( "tr", this.vettingtable );
    rkWebUtil.elemaker( "th", tr, { "text": "Galactic Counts", "attributes": { "colspan": 2 } } );
    tr = rkWebUtil.elemaker( "tr", this.vettingtable );
    rkWebUtil.elemaker( "th", tr, { "text": "N. Vets" } );
    rkWebUtil.elemaker( "th", tr, { "text": "N. Objects" } );
    
    for ( let ngal of data['ngal'] ) {
        tr = rkWebUtil.elemaker( "tr", this.vettingtable );
        rkWebUtil.elemaker( "td", tr, { "text": ngal[0] } );
        rkWebUtil.elemaker( "td", tr, { "text": ngal[1] } );
    }
}

// **********************************************************************

decatview.Context.prototype.lookupCandidate = function() {
    var candid = this.candidatelookupname.value;
    var href = webapconfig.webapurl + "cand/" + this.candidatelookupname.value
    href += "?rbtype=" + this.chosenrbtype;
    window.open( href, "_self" );
}

decatview.Context.prototype.searchForCandidates = function() {
    var searchdata = {};

    this.chosenrbtype_candsearch = this.rbtypewid_candsearch.value;
    searchdata.rbtype = this.chosenrbtype_candsearch;
    searchdata.vtag = this.candidatesearch_versiontagwid.value;
    if ( this.limitallproposalsorsome == "all" ) {
        searchdata.allorsome = "all";
    } else {
        searchdata.allorsome = "some";
        searchdata.proposals = this.limitselectedproposals;
    }

    for ( let limit in this.limits ) {
        searchdata["use" + limit] = this.limits[limit].use;
        searchdata[limit] = this.limits[limit].value;
    }

    this.candsearchobj = new decatview.CandSearch();
    this.candsearchobj.searchForCandidates( searchdata, this.limits, this.maindiv, this );
}

// **********************************************************************
// **********************************************************************
// **********************************************************************

decatview.CandSearch = function() {}

// These variables are redundant with things in decatview_showcand.js.
// So is a bunch of the code below.
// Suggests refactoring needed.
decatview.CandSearch.filtercolors = { 'g': '#008800',
                                   'r': '#880000',
                                   'i': '#884400',
                                   'z': '#444400' };
decatview.CandSearch.othercolors = [ '#000088', '#880088', '#008888', '#448800' ];
decatview.CandSearch.filterorder = [ 'g', 'r', 'i', 'z' ];

decatview.CandSearch.prototype.searchForCandidates = function( searchdata, limits, maindiv, parent ) {
    var self = this;
    var hbox, subbox, vbox, div, ul, span, li;
    this.maindiv = maindiv;
    this.parent = parent;
    this.divsforltcvs = {};
    this.plotters = {};
    this.datasets = {};

    this.rbtype = searchdata.rbtype;
    
    rkWebUtil.wipeDiv( this.maindiv );
    parent.backToHome( this.maindiv );
    rkWebUtil.elemaker( "h2", this.maindiv, { "text": "Candidate Search" } );

    hbox = rkWebUtil.elemaker( "div", this.maindiv, { "classes": ["hbox"] } );
    vbox = rkWebUtil.elemaker( "div", hbox, { "classes": ["vbox"] } );
    
    this.searchinfodiv = rkWebUtil.elemaker( "div", vbox );

    if ( searchdata.allorsome == "all" ) {
        rkWebUtil.elemaker( "p", this.searchinfodiv, { "text": "Searching all proposals" } );
    } else {
        rkWebUtil.elemaker( "p", this.searchinfodiv, { "text": "Searching proposals: " + searchdata.proposals } );
    }
    rkWebUtil.elemaker( "p", this.searchinfodiv, { "text": "r/b type: " + this.rbtype } );
    
    ul = rkWebUtil.elemaker( "ul", this.searchinfodiv, );
    for ( let limit in limits ) {
        if ( limits[limit].use ) {
            rkWebUtil.elemaker( "li", ul, { "text": "Limit " + limit + " : " + limits[limit].value } );
        }
    }
    
    this.candsearchdiv = rkWebUtil.elemaker( "div", vbox );
    rkWebUtil.elemaker( "span", this.candsearchdiv,
                        { "text": "Searching for candidates...", "classes": [ "warning" ] } );

    vbox = rkWebUtil.elemaker( "div", hbox, { "classes": ["vbox", "emmarginleft"] }  );
    let pointdiv = rkWebUtil.elemaker( "div", vbox, { "classes": ["hbox"] } );

    div = rkWebUtil.elemaker( "div", pointdiv, { "classes": ["oneclipmeta"] } );
    rkWebUtil.elemaker( "h3", div, { "text": "New" } );
    div = rkWebUtil.elemaker( "div", div, { "classes": ["oneclip"] } );
    this.newclip = rkWebUtil.elemaker( "div", div, { "classes": [ "oneclipwrapper" ] } );
    
    div = rkWebUtil.elemaker( "div", pointdiv, { "classes": ["oneclipmeta"] } );
    rkWebUtil.elemaker( "h3", div, { "text": "Ref" } );
    div = rkWebUtil.elemaker( "div", div, { "classes": ["oneclip"] } );
    this.refclip = rkWebUtil.elemaker( "div", div, { "classes": [ "oneclipwrapper" ] } );
    
    div = rkWebUtil.elemaker( "div", pointdiv, { "classes": ["oneclipmeta"] } );
    rkWebUtil.elemaker( "h3", div, { "text": "Sub" } );
    div = rkWebUtil.elemaker( "div", div, { "classes": ["oneclip"] } );
    this.subclip = rkWebUtil.elemaker( "div", div, { "classes": [ "oneclipwrapper" ] } );

    hbox = rkWebUtil.elemaker( "div", vbox, { "classes": ["hbox"] } );
    subbox = rkWebUtil.elemaker( "div", hbox );
    ul = rkWebUtil.elemaker( "ul", subbox );
    li = rkWebUtil.elemaker( "li", ul, { "text": "rb: " } );
    this.rbtext = rkWebUtil.elemaker( "span", li );
    li = rkWebUtil.elemaker( "li", ul, { "text": "Proposal: " } );
    this.proptext = rkWebUtil.elemaker( "span", li );
    li = rkWebUtil.elemaker( "li", ul, { "text": "Mag: " } );
    this.magtext = rkWebUtil.elemaker( "span", li );
    li = rkWebUtil.elemaker( "li", ul, { "text": "File: " } );
    this.filetext = rkWebUtil.elemaker( "span", li );
    subbox = rkWebUtil.elemaker( "div", hbox );
    ul = rkWebUtil.elemaker( "ul", subbox );
    li = rkWebUtil.elemaker( "li", ul, { "text": "Band: " } );
    this.bandtext = rkWebUtil.elemaker( "span", li );
    li = rkWebUtil.elemaker( "li", ul, { "text": "CCD: " } );
    this.ccdtext = rkWebUtil.elemaker( "span", li );
    li = rkWebUtil.elemaker( "li", ul, { "text": "obj id: " } )
    this.objidtext = rkWebUtil.elemaker( "span", li );
    li = rkWebUtil.elemaker( "li", ul, { "text": "obj data id: " } )
    this.objdataidtext = rkWebUtil.elemaker( "span", li );
    
    hbox = rkWebUtil.elemaker( "div", vbox, { "classes": ["hbox"] } );
    this.showcandlink = rkWebUtil.elemaker( "a", vbox, { "text": "Show Candidate in New Tab" } );
    this.showcandlink.style.display = "none";
    
    this.ltcvplotdiv = rkWebUtil.elemaker( "div", vbox, { "classes": ["vbox"] } );
    
    parent.connector.sendHttpRequest( "searchcands", searchdata,
                                      function( res ) { self.ingestAndShowCands( res ) } );
}

// **********************************************************************

decatview.CandSearch.prototype.ingestAndShowCands = function( data ) {
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

decatview.CandSearch.prototype.sortCandsAndShow = function( key, ascending=true ) {
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

decatview.CandSearch.prototype.showCands = function() {
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
        a = rkWebUtil.elemaker( "a", td, { "text": row["id"], "classes": [ "link" ] } );
        a.addEventListener( "click", function() { self.showLtcv( row["id"] ); } );
                                
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
    
decatview.CandSearch.prototype.showLtcv = function( candid ) {
    var self = this;
    var data = {};
    if ( this.rbtype != null ) data['rbtype'] = this.rbtype;

    this.parent.connector.sendHttpRequest( "cutoutsforcand/" + candid, data,
                                           function( res ) { self.actuallyShowLtcv( res ) } );
}

decatview.CandSearch.prototype.actuallyShowLtcv = function( data ) {
    var self=this;

    this.showcands_ltcvdata = data;
    
    rkWebUtil.wipeDiv( this.ltcvplotdiv );
    rkWebUtil.wipeDiv( this.newclip );
    rkWebUtil.wipeDiv( this.refclip );
    rkWebUtil.wipeDiv( this.subclip );

    rkWebUtil.elemaker( "h3", this.ltcvplotdiv, { "text": data.candid } );

    this.showcandlink.style.display = "inline";
    let href = webapconfig.webapurl + "cand/" + data.candid;
    if ( this.rbtype != null ) href += "?rbtype=" + this.rbtype;
    this.showcandlink.setAttribute( "href", href );
    this.showcandlink.setAttribute( "target", "_blank" );
    
    this.bands = new Set();
    for ( let obj of data.objs ) this.bands.add( obj.filter );

    var didplot = new Set();
    var coloroff = 0;
    var ymins = {};
    var ymaxs = {};
    var xmin = 1e32;
    var xmax = -1e32;
    for ( let band of decatview.CandSearch.filterorder ) {
        if ( this.bands.has( band ) ) {
            let limits = this.plotltcv( data.objs, band, decatview.CandSearch.filtercolors[band] );
            console.log( "Got limits: " + limits );
            ymins[band] = limits[2];
            ymaxs[band] = limits[3];
            if ( limits[0] < xmin ) xmin = limits[0];
            if ( limits[1] > xmax ) xmax = limits[1];
            didplot.add( band );
        }
    }
    for ( let band of this.bands ) {
        if ( ! didplot.has(band) ) {
            this.plotltcv( data.objs, band, decatview.CandSearch.othercolors[ coloroff ] );
            coloroff += 1;
            if ( coloroff >= decatview.CandSearch.othercolors.length ) coloroff = 0;
            ymins[band] = limits[2];
            ymaxs[band] = limits[3];
            if ( limits[0] < xmin ) xmin = limits[0];
            if ( limits[1] > xmax ) xmax = limits[1];
        }
    }

    for ( let band of this.bands ) {
        this.plotters[band].defaultlimits = [ xmin, xmax, ymins[band], ymaxs[band] ];
        this.plotters[band].zoomToDefault();
    }
        
}

decatview.CandSearch.prototype.plotltcv = function( objs, band, color ) {
    var self = this;
    
    let x = [];
    let y = [];
    let dy = [];

    for ( let obj of objs ) {
        if ( obj.filter == band ) {
            x.push( obj.meanmjd );
            let flux = 10**( (obj.mag - 29.)/-2.5 );
            let dflux = obj.magerr * Math.log(10)/2.5 * flux;
            y.push( flux )
            dy.push( dflux );
        }
    }
    let div = rkWebUtil.elemaker( "div", this.ltcvplotdiv, { "classes": [ "vbox", "ltcvdiv" ] } );
    this.divsforltcvs[ band ] = div;

    let buttons = [ rkWebUtil.button( div, "Share X Range", function(e) { self.ltcvShareXRange( band ) } ) ];
    
    this.plotters[ band ] = new SVGPlot.Plot( { "divid": "svgplotdiv-" + band,
                                                "svgid": "svgplotsvg-" + band,
                                                "title": band + "-band",
                                                "xtitle": "mjd",
                                                "ytitle": "flux (arb.)",
                                                "buttons": buttons
                                              } );
    div.appendChild( this.plotters[ band ].topdiv );
    this.datasets[ band ] = new SVGPlot.Dataset( { "name": band,
                                                   "x": x,
                                                   "y": y,
                                                   "dy": dy,
                                                   "color": color,
                                                   "linewid": 0 } );
    this.plotters[ band ].addDataset( this.datasets[ band ] );
    this.plotters[ band ].addClickListener( function( info ) {
        self.clickedOnPoint( info, band );
    } );
                                            
    this.plotters[ band ].redraw();
    return [ this.plotters[band].xmin, this.plotters[band].xmax,
             this.plotters[band].ymin, this.plotters[band].ymax  ];
}

// **********************************************************************

decatview.CandSearch.prototype.ltcvShareXRange = function( band ) {
    let xmin = this.plotters[band].xmin;
    let xmax = this.plotters[band].xmax;
    for ( let b of this.bands ) {
        this.plotters[b].xmin = xmin;
        this.plotters[b].xmax = xmax;
    }
}

// **********************************************************************

decatview.CandSearch.prototype.clickedOnPoint = function( info, band ) {
    rkWebUtil.wipeDiv( this.newclip );
    rkWebUtil.wipeDiv( this.refclip );
    rkWebUtil.wipeDiv( this.subclip );
    for ( let pltband in this.plotters ) {
        if ( pltband != band ) {
            this.plotters[pltband].removehighlight()
        }
    }
    
    for ( let val of [ 'rbtext', 'proptext', 'magtext', 'filetext',
                       'bandtext', 'ccdtext', 'objidtext', 'objdataidtext' ] ) {
        this[val].innerHTML = '';
    }

    // Try to find the object that corresponds to the click
    var dex = -1;
    var clickobj = null;
    for ( let obj of this.showcands_ltcvdata.objs ) {
        if ( obj.filter == band ) {
            dex += 1;
            if ( dex == info.pointdex ) {
                clickobj = obj;
                break;
            }
        }
    }
    if ( clickobj == null ) {
        window.alert( "Failed to find cutouts for clicked point; this shouldn't happend." );
        return;
    }
    this.rbtext.innerHTML = clickobj.rb.toFixed(2);
    this.proptext.innerHTML = clickobj.proposalid;
    this.magtext.innerHTML = clickobj.mag.toFixed(2) + " ± " + clickobj.magerr.toFixed(2);
    this.filetext.innerHTML = clickobj.filename;
    this.bandtext.innerHTML = clickobj.filter;
    this.ccdtext.innerHTML = clickobj.ccdnum;
    this.objidtext.innerHTML = clickobj.object_id;
    this.objdataidtext.innerHTML = clickobj.objectdata_id;

    var divmap = { "sci": this.newclip, "ref": this.refclip, "diff": this.subclip };
    for ( let img of [ "sci", "ref", "diff" ] ) { 
        if ( clickobj[img+"_jpeg"] == null ) {
            rkWebUtil.elemaker( "span", divmap[img], { "text": "cutout missing" } );
        } else {
            rkWebUtil.elemaker( "img", divmap[img],
                                { "attributes": { "src": "data:image/jpeg;base64," + clickobj[img+"_jpeg"],
                                                  "width": 153,
                                                  "height": 153,
                                                  "alt": img } } );
        }
    }
}

// **********************************************************************

export { decatview }
