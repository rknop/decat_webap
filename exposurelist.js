import { rkWebUtil } from "./rkwebutil.js"
import { CutoutList } from "./cutoutlist.js"

// **********************************************************************

var ExposureList = function( div, startdate, enddate, rbinfo, proplist, connector ) {
    this.div = div;
    this.startdate = startdate;
    this.enddate = enddate
    this.rbinfo = rbinfo;
    this.proplist = proplist;
    this.connector = connector;
    this.checkpointdefs = null;
}

ExposureList.prototype.render = function() {
    rkWebUtil.wipeDiv( this.div );

    let p = rkWebUtil.elemaker( "p", this.div, { "text": "Loading exposures from " +
                                                 this.startdate + " to " + this.enddate } );
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

    p = rkWebUtil.elemaker( "p", this.div, { "text": "r/b type is " + this.rbinfo.id +
                                             " (" + this.rbinfo.description + ") ; " +
                                             "cutoff is " + this.rbinfo.rbcut } );

    this.exposuresdiv = rkWebUtil.elemaker( "div", this.div );
    rkWebUtil.elemaker( "p", this.exposuresdiv, { "text": "Loading...", "classes": [ "warning" ] } );
    
    this.showExposures( this.startdate, this.enddate, this.rbinfo.id, this.rbinfo.rbcut, this.proplist );
}

// **********************************************************************
// Show tiles for up to 100 objects
//
// ROB : it's gratuitous to pass rbcut to the webap, since that
//  information is in the database that the server has.  I did it
//  because I was being lazy about constructing my queries server side.

ExposureList.prototype.showExposures = function( t0text, t1text, rb, rbcut, props ) {
    var self = this;
    this.connector.sendHttpRequest( "findexposures",
                                    { "t0": t0text,
                                      "t1": t1text,
                                      "rbtype": rb,
                                      "rbcut": rbcut,
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
        button = rkWebUtil.button( td, "Show Objects", function() { self.showExposureObjects( exposure.id,
                                                                                              exposure.filename ) } );
        td = rkWebUtil.elemaker( "td", tr );
        button = rkWebUtil.button( td, "Show Log", function() { self.showExposureLog( exposure.id ) } );
        if ( exposure.numerrors > 0 ) {
            rkWebUtil.elemaker( "td", tr, { "text": exposure.numerrors + " errors", "classes": [ "bad" ] } );
        }
    }
}

// **********************************************************************

ExposureList.prototype.showExposureLog = function( exposureid ) {
    var self = this;
    rkWebUtil.wipeDiv( this.div );
    rkWebUtil.elemaker( "p", this.div, { "text": "Back to exposure list",
                                         "classes": [ "link" ],
                                         "click": function() { self.render() } } );
    let logdiv = rkWebUtil.elemaker( "div", this.div );
    rkWebUtil.elemaker( "p", logdiv, { "text": "Loading event log...", "classes": [ "warning" ] } );
    if ( this.checkpointdefs == null ) {
        this.connector.sendHttpRequest( "checkpointdefs", {},
                                        function( data ) {
                                            self.checkpointdefs = data;
                                            self.okNowShowExposureLog( exposureid, logdiv );
                                        } );
    }
    else {
        this.okNowShowExposureLog( exposureid, logdiv );
    }
}

ExposureList.prototype.okNowShowExposureLog = function( exposureid, logdiv ) {
    var self = this;
    this.connector.sendHttpRequest( "exposurelog/" + exposureid, {},
                                    function( data ) {
                                        self.actuallyShowExposureLog( data, logdiv ) } );
}

// ROB!  Built in assumption (from DECam) that CCD number starts at 1
// ROB!  Should also pull down camera info and get the number of CCDs
ExposureList.prototype.actuallyShowExposureLog = function( data, logdiv ) {
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
    var minccdnum = 1;          // ROB!  Get this from camera database
    var maxccdnum = -1;
    
    for ( let event of data["checkpoints"] ) {
        if ( event.ccdnum > maxccdnum ) maxccdnum = event.ccdnum;
        if ( event.event_id == errorid ) haserrors.add( event.ccdnum );
        if ( event.event_id == infoid  ) hasinfos.add( event.ccdnum );
        if ( event.event_id == subid ) hassubs.add( event.ccdnum );
        if ( event.event_id == doneid ) hasobjs.add( event.ccdnum );
        nodes.add( event.running_node );
    }
    haserrors = Array.from( haserrors ).sort();
    hasinfos = Array.from( hasinfos ).sort();
    var nosubs = [];
    var noobjs = [];
    for ( let i = minccdnum ; i <= maxccdnum ; i+=1 )  {
        if ( !hassubs.has( i ) ) nosubs.push( i );
        if ( !hasobjs.has( i ) ) noobjs.push( i );
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
    for ( let ccd = minccdnum ; ccd <= maxccdnum ; ccd+=1 ) ccdlink( p, ccd );
    
    for ( let ccd = minccdnum ; ccd <= maxccdnum ; ccd+=1 ) {
        rkWebUtil.elemaker( "h3", logdiv, { "text": "CCD " + ccd,
                                            "attributes": { "id": "ccd-" + ccd } } );
        table = rkWebUtil.elemaker( "table", logdiv, { "classes": [ "logtable" ] } );
        tr = rkWebUtil.elemaker( "tr", table );
        for ( let title of [ "CCD", "Rank", "Time", "Event", "Notes" ] ) {
            rkWebUtil.elemaker( "th", tr, { "text": title } );
        }
        for ( let chkpt of data["checkpoints"] ) {
            if ( ( chkpt.ccdnum < 0 ) || ( chkpt.ccdnum == ccd ) ) {
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

ExposureList.prototype.showExposureObjects = function( expid, filename ) {
    var self = this;
    this.shown_objects_expid = expid;
    rkWebUtil.wipeDiv( this.div );
    rkWebUtil.elemaker( "p", this.div, { "text": "Back to exposure list",
                                         "classes": [ "link" ],
                                         "click": function() {
                                             delete( self.cutouts ),
                                             self.render() } } );
    rkWebUtil.elemaker( "h3", this.div, { "text": "Objects for " + filename } );
    var p = rkWebUtil.elemaker( "p", this.div, { "text": "r/b type is " + this.rbinfo.id +
                                                 " (" + this.rbinfo.description + ") ; " +
                                                 "cutoff is " + this.rbinfo.rbcut } );
    this.abovecutoutdiv = rkWebUtil.elemaker( "div", this.div );
    this.cutoutdiv = rkWebUtil.elemaker( "div", this.div );
    this.belowcutoutdiv = rkWebUtil.elemaker( "div", this.div );
    rkWebUtil.elemaker( "p", this.cutoutdiv, { "text": "Loading object cutouts...",
                                               "classes": [ "warning" ] } );
    this.cutouts = new CutoutList( this.cutoutdiv, { "rbinfo": this.rbinfo } );
    this.connector.sendHttpRequest( "cutoutsforexp/" + this.shown_objects_expid,
                                    { "rbtype": this.rbinfo.id,
                                      "offset": 0,
                                      "limit": 100 },
                                    function( data ) {
                                        self.renderExposureObjects( data )
                                    } );
}

// **********************************************************************

ExposureList.prototype.renderExposureObjects = function( data ) {
    var self = this;
    rkWebUtil.wipeDiv( this.abovecutoutdiv );
    rkWebUtil.wipeDiv( this.belowcutoutdiv );
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
        self.renderExposureObjects( data )
    }
    
    for ( let div of [ this.abovecutoutdiv, this.belowcutoutdiv ] ) {
        if ( data.offset + 100 < data.totnobjs ) {
            rkWebUtil.button( div, "Next 100",
                              function() {
                                  self.connector.sendHttpRequest(
                                      "cutoutsforexp/" + self.shown_objects_expid,
                                      { "rbtype": self.rbinfo.id,
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
                                      { "rbtype": self.rbinfo.id,
                                        "offset": prevoff,
                                        "limit": 100 },
                                      handlereq ) } );
            div.appendChild( document.createTextNode( " " ) )
            if ( prevoff > 0 ) {
                rkWebUtil.button( div, "First 100",
                                  function() {
                                      self.connector.sendHttpRequest(
                                          "cutoutsforexp/" + self.shown_objects_expid,
                                          { "rbtype": self.rbinfo.id,
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
