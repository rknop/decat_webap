import { webapconfig } from "./decatview_config.js"
import { rkAuth } from "./rkauth.js"
import { rkWebUtil } from "./rkwebutil.js"
import { CutoutList } from "./cutoutlist.js"
import { SVGPlot } from "./svgplot.js"

// **********************************************************************

var ShowCandidate = function( div, candid, rbtype ) {
    this.superdiv = div;
    this.candid = candid;
    this.rbtype = rbtype;
    this.rbinfo = null;
    this.connector = new rkWebUtil.Connector( webapconfig.webapurl );
}

// **********************************************************************

ShowCandidate.filtercolors = { 'g': '#008800',
                               'r': '#880000',
                               'i': '#884400',
                               'z': '#444400' };
ShowCandidate.othercolors = [ '#000088', '#880088', '#008888', '#448800' ];
ShowCandidate.filterorder = [ 'g', 'r', 'i', 'z' ];

// **********************************************************************

ShowCandidate.prototype.render = function() {
    rkWebUtil.wipeDiv( this.superdiv );
    this.infodiv = rkWebUtil.elemaker( "div", this.superdiv );
    this.rbinfodiv = rkWebUtil.elemaker( "div", this.superdiv );
    this.maindiv = rkWebUtil.elemaker( "div", this.superdiv, { "classes": [ "candidateshow" ] } );
    this.tilesdiv = rkWebUtil.elemaker( "div", this.maindiv, { "classes": [ "candidatecutouts" ] } );
    this.ltcvsdiv = rkWebUtil.elemaker( "div", this.maindiv, { "classes": [ "candidateltcvs" ] } );
    this.divsforltcvs = {};
    this.plotters = {};
    this.datasets = {};
    
    if ( this.rbtype != null ) {
        this.populateRBInfoDiv();
    }
    else {
        this.showTheThings();
    }
}

// **********************************************************************

ShowCandidate.prototype.populateRBInfoDiv = function() {
    var self = this;
    this.connector.sendHttpRequest( "getrbtypes", {}, function( data ) { self.actuallyPopulateRBInfoDiv( data ) } );
}

// **********************************************************************

ShowCandidate.prototype.actuallyPopulateRBInfoDiv = function( data ) {
    for ( let rbinfo of data["rbtypes"] ) {
        if ( rbinfo.id == this.rbtype ) {
            this.rbinfo = rbinfo;
        }
    }
    if ( this.rbinfo == null ) {
        rkWebUtil.elemaker( "p", this.rbinfodiv, { "text": "(Unknown r/b type " + this.rbtype + ")",
                                                   "classes": [ "warning" ] } );
    }
    else {
        rkWebUtil.elemaker( "p", this.rbinfodiv, { "text": "r/b type is " + this.rbinfo.id + " (" +
                                                   this.rbinfo.description + ") ; cutoff is " +
                                                   this.rbinfo.rbcut } );
    }
    this.showTheThings();
}

// **********************************************************************

ShowCandidate.prototype.showTheThings = function() {
    var self = this;
    var data = {};
    if ( this.rbinfo != null ) data.rbtype = this.rbinfo.id; 
    this.cutouts = new CutoutList( this.tilesdiv, { "rbinfo": this.rbinfo,
                                                    "showcandid": false,
                                                    "showfilename": true,
                                                    "showband": true,
                                                    "showpropid": true,
                                                    "imgsize": 153 } );
    rkWebUtil.elemaker( "p", this.tilesdiv, { "text": "Loading data for candidate...",
                                              "classes": [ "warning" ] } );
    this.connector.sendHttpRequest( "cutoutsforcand/" + this.candid, data,
                                    function( data ) { self.actuallyShowTheThings( data ); } );
}

// **********************************************************************

ShowCandidate.prototype.actuallyShowTheThings = function( data ) {
    rkWebUtil.elemaker( "h3", this.infodiv, { "text": "Candidate: " + data.objs[0].candid } );

    var p = rkWebUtil.elemaker( "p", this.infodiv, { "text": "Candidate RA: " +
                                                     data['candra'].toFixed(5) +
                                                     " ; Dec: " +
                                                     data['canddec'].toFixed(5) + " — " } );
    rkWebUtil.elemaker( "a", p, { "text": "DESI Viewer at this position",
                                  "classes": [ "link" ],
                                  "attributes": {
                                      "target": "_blank",
                                      "href": "https://www.legacysurvey.org/viewer-desi"
                                          + "?ra=" + data['candra'].toFixed(5)
                                          + "&dec=" + data['canddec'].toFixed(5)
                                          + "&zoom=16&layer=dr8&mark="
                                          + data['candra'].toFixed(5) + "," + data['canddec'].toFixed(5)
                                  }
                                } );
    rkWebUtil.elemaker( "p", this.infodiv, { "text": data.totnobjs + " sources for candidate." } );

    // Figure out what bands we have
    this.bands = new Set();
    for ( let obj of data.objs ) {
        this.bands.add( obj.filter );
    }

    var didplot = new Set();
    var coloroff = 0;
    var ymins = {};
    var ymaxs = {};
    var xmin = 1e32;
    var xmax = -1e32;
    for ( let band of ShowCandidate.filterorder ) {
        if ( this.bands.has( band ) ) {
            let limits = this.plotltcv( data.objs, band, ShowCandidate.filtercolors[band] );
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
            this.plotltcv( data.objs, band, ShowCandidate.othercolors[ coloroff ] );
            coloroff += 1;
            if ( coloroff >= ShowCandidate.othercolors.length ) coloroff = 0;
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
        
    this.cutouts.render( data.objs );
}

// **********************************************************************

ShowCandidate.prototype.plotltcv = function( objs, band, color ) {
    var self = this;
    
    let x = [];
    let y = [];
    let dy = [];

    for ( let obj of objs ) {
        if ( obj.filter == band ) {
            x.push( obj.mjd );
            y.push( obj.flux );
            dy.push( obj.fluxerr );
        }
    }
    let div = rkWebUtil.elemaker( "div", this.ltcvsdiv, { "classes": [ "vbox", "ltcvdiv" ] } );
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
    
    this.plotters[ band ].redraw();
    return [ this.plotters[band].xmin, this.plotters[band].xmax,
             this.plotters[band].ymin, this.plotters[band].ymax  ];
}

// **********************************************************************

ShowCandidate.prototype.ltcvShareXRange = function( band ) {
    let xmin = this.plotters[band].xmin;
    let xmax = this.plotters[band].xmax;
    for ( let b of this.bands ) {
        this.plotters[b].xmin = xmin;
        this.plotters[b].xmax = xmax;
    }
}

// **********************************************************************

export { ShowCandidate }

