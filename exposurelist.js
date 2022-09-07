import { rkWebUtil } from "./rkwebutil.js"
import { CutoutList } from "./cutoutlist.js"

// **********************************************************************
// ROB!  Down in showing hte exposure log, you've hardcoded
// DECam

var ExposureList = function( div, startdate, enddate, rbinfo, proplist, mingallat, maxgallat,
                             versiontagid, versiontagdesc, connector ) {
    this.topdiv = div;
    this.div = null;
    this.startdate = startdate;
    this.enddate = enddate
    this.rbinfo = rbinfo;
    this.proplist = proplist;
    this.mingallat = mingallat;
    this.maxgallat = maxgallat;
    this.versiontagid = versiontagid
    this.versiontagdesc = versiontagdesc;
    this.connector = connector;
    this.checkpointdefs = null;
    this.camerachips = {};
}

ExposureList.prototype.render = function( rerender=false ) {
    var self = this;

    if ( rerender ) {
        if ( this.div != null ) this.div.remove();
        this.div = null;
    }

    if ( this.div != null ) {
        this.div.remove();
        rkWebUtil.wipeDiv( this.topdiv );
        this.topdiv.appendChild( this.div );
        this.div.style.display = "block";
        return;
    }

    this.div = rkWebUtil.elemaker( "div", this.topdiv );
    
    let start = this.startdate.trim() == "" ? "(no limit)" : this.startdate;
    let end = this.enddate.trim() == "" ? "(no limit)" : this.enddate;
    let p = rkWebUtil.elemaker( "p", this.div, { "text": "Showing exposures starting: " + start +
                                                             ", ending: " + end } );
    if ( this.proplist == null ) {
        p = rkWebUtil.elemaker( "p", this.div, { "text": "Showing exposures from ALL proposals." } );
    }
    else {
        p = rkWebUtil.elemaker( "p", this.div, { "text": "Showing exposures from proposals: " } );
        let first = true;
        let text = "";
        for ( let prop of this.proplist ) {
            if ( first ) first=false;
            else text += ", ";
            text += prop;
        }
        p.appendChild( document.createTextNode( text ) );
    }

    p = rkWebUtil.elemaker( "p", this.div, { "text": "Version tag for subtractions & object data: " +
                                             this.versiontagdesc } );

    p = rkWebUtil.elemaker( "p", this.div );
    rkWebUtil.elemaker( "span", p, { "text": "For galactic fields, r/b type is " + this.rbinfo.gal.id +
                                     " (" + this.rbinfo.gal.description + ") ; " +
                                     "cutoff is " + this.rbinfo.gal.rbcut } );
    rkWebUtil.elemaker( "br", p );
    rkWebUtil.elemaker( "span", p, { "text": "For extragalactic fields, r/b type is " + this.rbinfo.exgal.id +
                                     " (" + this.rbinfo.exgal.description + ") ; " +
                                     "cutoff is " + this.rbinfo.exgal.rbcut } );

    p = rkWebUtil.elemaker( "p", this.div, { "text": "Reload list",
                                             "classes": [ "link" ] } );
    p.addEventListener( "click", function() { self.render( true ) } );
                                             
    
    this.exposuresdiv = rkWebUtil.elemaker( "div", this.div );
    rkWebUtil.elemaker( "p", this.exposuresdiv, { "text": "Loading...", "classes": [ "warning" ] } );

    this.showExposures( this.startdate, this.enddate, this.proplist );
}

// **********************************************************************
// Show tiles for up to 100 objects
//
// ROB : it's gratuitous to pass rbcut to the webap, since that
//  information is in the database that the server has.  I did it
//  because I was being lazy about constructing my queries server side.

ExposureList.prototype.showExposures = function( t0text, t1text, props ) {
    var self = this;
    this.connector.sendHttpRequest( "findexposures",
                                    { "t0": t0text,
                                      "t1": t1text,
                                      "versiontagid": this.versiontagid,
                                      "rbtypes": [ this.rbinfo.gal.id, this.rbinfo.exgal.id ],
                                      "rbcuts": [ this.rbinfo.gal.rbcut, this.rbinfo.exgal.rbcut ],
                                      "mingallat": this.mingallat,
                                      "maxgallat": this.maxgallat,
                                      "allorsomeprops": this.proplist == null ? "all" : "some",
                                      "props": props },
                                    function(data) { self.actuallyShowExposures( data ) } );
}

ExposureList.prototype.actuallyShowExposures = function( data ) {
    var self = this;

    rkWebUtil.wipeDiv( this.exposuresdiv );
    
    if ( data.hasOwnProperty( "error" ) ) {
        window.alert( data["error"] );
        rkWebUtil.Elemaker( "p", this.exposuresdiv, { "text": "Loading failed", "classes": [ "bad" ] } );
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
    rkWebUtil.elemaker( "th", tr, { "text": "#Copy" } );
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
        rkWebUtil.elemaker( "td", tr, { "text": exposure.numcopyout } );
        rkWebUtil.elemaker( "td", tr, { "text": exposure.numobjs } );
        let rbid = ( Math.abs(exposure.gallat)>=20 ? this.rbinfo.exgal.id : this.rbinfo.gal.id );
        let rbtag = ( Math.abs(exposure.gallat)>=20 ? "exgal" : "gal" );
        let rbprop = "numhighrb" + rbid;
        let rbcount = ( exposure.hasOwnProperty(rbprop) ? exposure[rbprop] : 0 )
        rkWebUtil.elemaker( "td", tr, { "text": rbcount } );
        td = rkWebUtil.elemaker( "td", tr );
        button = rkWebUtil.button( td, "Show Objects", function() { self.showExposureObjects( exposure.id,
                                                                                              exposure.filename,
                                                                                              rbtag ) } );
        td = rkWebUtil.elemaker( "td", tr );
        button = rkWebUtil.button( td, "Show Log", function() { self.showExposureLog( exposure.id,
                                                                                      exposure.filename ) } );
        if ( exposure.numerrors > 0 ) {
            rkWebUtil.elemaker( "td", tr, { "text": exposure.numerrors + " errors", "classes": [ "bad" ] } );
        }
    }
}

// **********************************************************************

ExposureList.prototype.showExposureLog = function( exposureid, exposurename ) {
    // **** WARNING : harcoding DECam here
    var camid = 1;
    // ****
    var self = this;
    this.div.style.display = "none";
    rkWebUtil.elemaker( "p", this.topdiv, { "text": "Back to exposure list",
                                            "classes": [ "link" ],
                                            "click": function() { self.render() } } );
    rkWebUtil.elemaker( "h3", this.topdiv, { "text": "Exposure: " + exposurename } );
    let logdiv = rkWebUtil.elemaker( "div", this.topdiv );
    rkWebUtil.elemaker( "p", logdiv, { "text": "Loading event log...", "classes": [ "warning" ] } );
    if ( this.checkpointdefs == null ) {
        this.connector.sendHttpRequest( "checkpointdefs", {},
                                        function( data ) {
                                            self.checkpointdefs = data;
                                            self.okNowShowExposureLog( exposureid, logdiv, camid );
                                        } );
    }
    else {
        this.okNowShowExposureLog( exposureid, logdiv, camid );
    }
}

ExposureList.prototype.okNowShowExposureLog = function( exposureid, logdiv, camid ) {
    var self = this;
    if ( ! this.camerachips.hasOwnProperty( camid ) ) {
        this.connector.sendHttpRequest( "getcamerachips/" + camid, {},
                                        function( data ) {
                                            self.okNoReallyNowShowExposureLog( exposureid, logdiv, camid,
                                                                               data["camerachips"] );
                                        } );
    }
    else {
        this.okNoReallyNowShowExposureLog( exposureid, logdiv, camid, this.camerachips[camid] );
    }
}

ExposureList.prototype.okNoReallyNowShowExposureLog = function( exposureid, logdiv, camid, camerachips ) {
    var self = this;
    this.camerachips[camid] = camerachips;
    this.connector.sendHttpRequest( "exposurelog/" + exposureid, {},
                                    function( data ) {
                                        self.actuallyShowExposureLog( data, logdiv, camid ) } );
}

// ROB!  Built in assumption (from DECam) that CCD number starts at 1
// ROB!  Should also pull down camera info and get the number of CCDs
ExposureList.prototype.actuallyShowExposureLog = function( data, logdiv, camid ) {
    var self = this;
    var errorid=-1, infoid=-1, subid=-1, doneid=-1;
    var p, table, tr;
    
    for ( let defid in this.checkpointdefs ) {
        if ( this.checkpointdefs[defid] == "error" ) {
            errorid = defid;
        }
        else if ( this.checkpointdefs[defid] == "info" ) {
            infoid = defid;
        }
        else if ( this.checkpointdefs[defid] == "image subtraction done" ) {
            subid = defid;
        }
        else if ( this.checkpointdefs[defid].startsWith( "detected objects" ) ) {
            doneid = defid;
        }
    }
    var hassubs = new Set();
    var hasobjs = new Set();
    var haserrors = new Set();
    var hasinfos = new Set();
    var nodes = new Set();
    
    for ( let event of data["checkpoints"] ) {
        if ( ( event.ccdnum == null ) && ( event.subccdnum != null ) ) event.ccdnum = event.subccdnum;
        if ( event.ccdnum != null ) {
            if ( event.event_id == errorid ) haserrors.add( event.ccdnum );
            if ( event.event_id == infoid  ) hasinfos.add( event.ccdnum );
            if ( event.event_id == subid ) hassubs.add( event.ccdnum );
            if ( event.event_id == doneid ) hasobjs.add( event.ccdnum );
        }
        nodes.add( event.running_node );
    }
    haserrors = Array.from( haserrors ).sort( (a,b)=>a-b );
    hasinfos = Array.from( hasinfos ).sort( (a,b)=>a-b );
    var nosubs = [];
    var noobjs = [];
    for ( let chip of this.camerachips[camid] ) {
        if ( chip.isgood ) {
            if ( !hassubs.has( chip.chipnum ) ) nosubs.push( chip.chipnum );
            if ( !hasobjs.has( chip.chipnum ) ) noobjs.push( chip.chipnum );
        }
    }

    rkWebUtil.wipeDiv( logdiv );
    
    var ccdlink = function( p, ccd ) {
        p.appendChild( document.createTextNode( " " ) )
        rkWebUtil.elemaker( "a", p, { "text": ccd, "classes": [ "link" ],
                                      "attributes": { "href": "#ccd-" + ccd } } );
    }
    p = rkWebUtil.elemaker( "p", logdiv, { "text": "Ran on: " + Array.from( nodes ).sort() } );
    p = rkWebUtil.elemaker( "p", logdiv, { "text": "CCDs without subtraction: " } )
    for ( let ccd of nosubs ) ccdlink( p, ccd );
    p = rkWebUtil.elemaker( "p", logdiv, { "text": "CCDs without object detection: " } );
    for ( let ccd of noobjs ) ccdlink( p, ccd );
    p = rkWebUtil.elemaker( "p", logdiv, { "text": "CCDs with errors logged: " } );
    for ( let ccd of haserrors ) ccdlink( p, ccd );
    p = rkWebUtil.elemaker( "p", logdiv, { "text": "CCDs with info logged: " } );
    for ( let ccd of hasinfos ) ccdlink( p, ccd );
    p = rkWebUtil.elemaker( "p", logdiv, { "text": "Jump to CCD: " } );
    for ( let chip of this.camerachips[camid] ) if ( chip.isgood ) ccdlink( p, chip.chipnum );

    rkWebUtil.elemaker( "h3", logdiv, { "text": "Exposure" } )
    table = rkWebUtil.elemaker( "table", logdiv, { "classes": [ "logtable" ] } );
    tr = rkWebUtil.elemaker( "tr", table );
    for ( let title of [ "Rank", "Time", "Event", "Notes" ] ) {
        rkWebUtil.elemaker( "th", tr, { "text": title } );
    }
    for ( let chkpt of data["checkpoints"] ) {
        if ( chkpt.ccdnum == null ) {
            tr = rkWebUtil.elemaker( "tr", table );
            if ( chkpt.event_id == errorid ) {
                tr.classList.add( "bad" );
            }
            else if ( chkpt.event_id == infoid ) {
                tr.classList.add( "info" );
            }
            rkWebUtil.elemaker( "td", tr, { "text": chkpt.mpi_rank } );
            rkWebUtil.elemaker( "td", tr, { "text": chkpt.created_at } );
            rkWebUtil.elemaker( "td", tr, { "text": this.checkpointdefs[chkpt.event_id] } );
            rkWebUtil.elemaker( "td", tr, { "text": chkpt.notes } );
        }
    }
            
    
    for ( let chip of this.camerachips[camid] ) {
        if ( ! chip.isgood ) continue;
        let ccd = chip.chipnum;

        rkWebUtil.elemaker( "h3", logdiv, { "text": "CCD " + ccd,
                                            "attributes": { "id": "ccd-" + ccd } } );
        table = rkWebUtil.elemaker( "table", logdiv, { "classes": [ "logtable" ] } );
        tr = rkWebUtil.elemaker( "tr", table );
        for ( let title of [ "CCD", "Rank", "Time", "Event", "Notes" ] ) {
            rkWebUtil.elemaker( "th", tr, { "text": title } );
        }
        for ( let chkpt of data["checkpoints"] ) {
            if ( chkpt.ccdnum == ccd ) {
                tr = rkWebUtil.elemaker( "tr", table );
                if ( chkpt.event_id == errorid ) {
                    tr.classList.add( "bad" );
                }
                else if ( chkpt.event_id == infoid ) {
                    tr.classList.add( "info" );
                }
                rkWebUtil.elemaker( "td", tr, { "text": chkpt.ccdnum } );
                rkWebUtil.elemaker( "td", tr, { "text": chkpt.mpi_rank } );
                rkWebUtil.elemaker( "td", tr, { "text": chkpt.created_at } );
                rkWebUtil.elemaker( "td", tr, { "text": this.checkpointdefs[chkpt.event_id] } );
                rkWebUtil.elemaker( "td", tr, { "text": chkpt.notes } );
            }
        }
    }
}


// **********************************************************************
// I should probably make this into its own class

ExposureList.prototype.showExposureObjects = function( expid, filename, rbtag ) {
    var self = this;
    var rbinfo = this.rbinfo[rbtag];
    this.shown_objects_expid = expid;
    this.div.style.display = "none";
    rkWebUtil.elemaker( "p", this.topdiv, { "text": "Back to exposure list",
                                            "classes": [ "link" ],
                                            "click": function() {
                                                delete( self.cutouts ),
                                                self.render() } } );
    rkWebUtil.elemaker( "h3", this.topdiv, { "text": "Objects for " + filename } );
    var p = rkWebUtil.elemaker( "p", this.topdiv, { "text": "r/b type is " + rbinfo.id +
                                                    " (" + rbinfo.description + ") ; " +
                                                    "cutoff is " + rbinfo.rbcut } );
    this.abovecutoutdiv = rkWebUtil.elemaker( "div", this.topdiv );
    this.cutoutdiv = rkWebUtil.elemaker( "div", this.topdiv );
    this.belowcutoutdiv = rkWebUtil.elemaker( "div", this.topdiv );
    rkWebUtil.elemaker( "p", this.cutoutdiv, { "text": "Loading object cutouts...",
                                               "classes": [ "warning" ] } );
    this.cutouts = new CutoutList( this.cutoutdiv, { "rbinfo": rbinfo } );
    this.connector.sendHttpRequest( "cutoutsforexp/" + this.shown_objects_expid,
                                    { "rbtype": rbinfo.id,
                                      "offset": 0,
                                      "limit": 100 },
                                    function( data ) {
                                        self.renderExposureObjects( data, rbinfo )
                                    } );
}

// **********************************************************************

ExposureList.prototype.renderExposureObjects = function( data, rbinfo ) {
    var self = this;
    rkWebUtil.wipeDiv( this.abovecutoutdiv );
    rkWebUtil.wipeDiv( this.belowcutoutdiv );
    rkWebUtil.elemaker( "p", this.abovecutoutdiv, { "text": "versiontag=" + data.versiontag } );
    rkWebUtil.elemaker( "p", this.abovecutoutdiv,
                        { "text": "Starting at object " + data.offset + " of " +
                          data.totnobjs + " objects.",
                          "classes": [ "bold" ] } );

    var handlereq = function( data ) {
        rkWebUtil.wipeDiv( self.abovecutoutdiv );
        rkWebUtil.wipeDiv( self.belowcutoutdiv );
        rkWebUtil.wipeDiv( self.cutoutdiv );
        rkWebUtil.elemaker( "p", self.cutoutdiv, { "text": "Loading object cutouts...",
                                                   "classes": [ "warning" ] } );
        self.renderExposureObjects( data, rbinfo )
    }
    
    for ( let div of [ this.abovecutoutdiv, this.belowcutoutdiv ] ) {
        if ( data.offset + 100 < data.totnobjs ) {
            rkWebUtil.button( div, "Next 100",
                              function() {
                                  self.connector.sendHttpRequest(
                                      "cutoutsforexp/" + self.shown_objects_expid,
                                      { "rbtype": rbinfo.id,
                                        "offset": data.offset + 100,
                                        "limit": 100 },
                                      handlereq ) } );
            div.appendChild( document.createTextNode( " " ) )
        }
        if ( data.offset > 0 ) {
            let prevoff = data.offset - 100;
            if ( data.offset < 100 ) prevoff = 0;
            rkWebUtil.button( div, "Previous 100",
                              function() {
                                  self.connector.sendHttpRequest(
                                      "cutoutsforexp/" + self.shown_objects_expid,
                                      { "rbtype": rbinfo.id,
                                        "offset": prevoff,
                                        "limit": 100 },
                                      handlereq ) } );
            div.appendChild( document.createTextNode( " " ) )
            if ( prevoff > 0 ) {
                rkWebUtil.button( div, "First 100",
                                  function() {
                                      self.connector.sendHttpRequest(
                                          "cutoutsforexp/" + self.shown_objects_expid,
                                          { "rbtype": rbinfo.id,
                                            "offset": 0,
                                            "limit": 100 },
                                          handlereq ) } );
            }
        }
    }
    this.cutouts.render( data.objs );
}

// **********************************************************************

export { ExposureList }
