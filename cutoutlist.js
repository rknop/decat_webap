import { rkWebUtil } from "./rkwebutil.js"

// **********************************************************************

var CutoutList = function( div, options={} ) {
    this.div = div;
    this.options = {
        "showltcvs": false,
        "showfilename": false,
        "showband": false,
        "showrbs": true,
        "goodbadinterface": false,
        "imgsize": 204,
        "imgclass": "img",
        "rbinfo": null,
    };
    Object.assign( this.options, options );
}

CutoutList.prototype.render = function( cutoutdata )  {
    var self = this;
    var table, tr, td, span;
    rkWebUtil.wipeDiv( this.div );
    table = rkWebUtil.elemaker( "table", this.div, { "classes": [ "maintable" ] } );
    tr = rkWebUtil.elemaker( "tr", table );
    rkWebUtil.elemaker( "th", tr, { "text": "Info" } );
    rkWebUtil.elemaker( "th", tr, { "text": "New" } );
    rkWebUtil.elemaker( "th", tr, { "text": "Ref" } );
    rkWebUtil.elemaker( "th", tr, { "text": "Sub" } );

    for ( let data of cutoutdata ) {
        tr = rkWebUtil.elemaker( "tr", table );
        td = rkWebUtil.elemaker( "td", tr );
        // ROB TODO : link on candid
        td.appendChild( document.createTextNode( "Candidate: " ) );
        rkWebUtil.elemaker( "span", td, { "text": data.candid } );
        if ( this.options.showrbs ) {
            rkWebUtil.elemaker( "br", td );
            span = rkWebUtil.elemaker( "span", td, { "text": "rb: " + data.rb.toFixed(2) } );
            if ( this.options.rbinfo != null ) {
                if ( data.rb >= this.options.rbinfo.rbcut )
                    span.classList.add( "good" );
                else
                    span.classList.add( "bad" );
            }
            else span.classList.add( "bold" );
        }
        rkWebUtil.elemaker( "br", td );
        rkWebUtil.elemaker( "span", td, { "text": "α: " + data.ra.toFixed(5) + "  δ: " + data.dec.toFixed(5) } );
        if ( this.options.showfilename ) {
            rkWebUtil.elemaker( "br", td );
            rkWebUtil.elemaker( "span", td, { "text": "File: " + data.filename } );
        }
        if ( this.options.showband ) {
            rkWebUtil.elemaker( "br", td );
            rkWebUtil.elemaker( "span", td, { "text": "Band: " + data.filter } );
        }
        rkWebUtil.elemaker( "br", td );
        rkWebUtil.elemaker( "span", td, { "text": "ccd: " + data.ccdnum } );
        rkWebUtil.elemaker( "br", td );
        rkWebUtil.elemaker( "span", td, { "text": "Obj ID: " + data.object_id } );

        for ( let img of [ "sci", "ref", "diff" ] ) {
            td = rkWebUtil.elemaker( "td", tr );
            if ( data[img+"_jpeg"] == null ) {
                rkWebUtil.elemaker( "span", td, { "text": "(" + img + " cutout missing)" } );
            }
            else {
                rkWebUtil.elemaker( "img", td,
                                    { "classes": [ this.options.imgclass ],
                                      "attributes": { "src": "data:image/jpeg;base64," + data[img+"_jpeg"],
                                                      "width": this.options.imgsize,
                                                      "height": this.options.imgsize,
                                                      "alt": img } } );
            }
        }
    }
}

// **********************************************************************

export { CutoutList }
