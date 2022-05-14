import { rkWebUtil } from "./rkwebutil.js"

// Not 100% happy to be importing this here.  Should think about refactoring.
import { webapconfig } from "./decatview_config.js"

// **********************************************************************

var CutoutList = function( div, options={} ) {
    this.div = div;
    this.options = {
        "showcandid": true,
        "showfilename": false,
        "showband": false,
        "showrbs": true,
        "showmag": true,
        "showpropid": false,
        "imgsize": 204,
        "imgclass": "img",
        "rbinfo": null,
        "rowcallback": null,
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
        if ( this.options.showcandid ) {
            td.appendChild( document.createTextNode( "Candidate: " ) );
            let href = webapconfig.webapurl + "cand/" + data.candid;
            if ( this.options.rbinfo != null ) href += "?rbtype=" + this.options.rbinfo.id
            rkWebUtil.elemaker( "a", td, { "text": data.candid,
                                           "classes": [ "link" ],
                                           "attributes": { "href": href, "target": "_blank" } } );
        }
        if ( this.options.showrbs ) {
            rkWebUtil.elemaker( "br", td );
            if ( data.rb == null ) {
                span = rkWebUtil.elemaker( "span", td, { "text": "rb: (unknown)"} );
                span.classList.add( "bold" );
                span.classList.add( "italic" );
            }
            else {
                span = rkWebUtil.elemaker( "span", td, { "text": "rb: " + data.rb.toFixed(2) } );
                if ( this.options.rbinfo != null ) {
                    if ( data.rb >= this.options.rbinfo.rbcut )
                        span.classList.add( "good" );
                    else
                        span.classList.add( "bad" );
                }
                else span.classList.add( "bold" );
            }
        }
        if ( this.options.showpropid ) {
            rkWebUtil.elemaker( "br", td );
            rkWebUtil.elemaker( "span", td, { "text": "Proposal: " + data.proposalid } );
        }
        rkWebUtil.elemaker( "br", td );
        rkWebUtil.elemaker( "span", td, { "text": "α: " + data.ra.toFixed(5) + "  δ: " + data.dec.toFixed(5) } );
        if ( this.options.showmag ) {
            rkWebUtil.elemaker( "br", td );
            rkWebUtil.elemaker( "span", td, { "text": "Mag: " + data.mag.toFixed(3) + 
                                              " ± " + data.magerr.toFixed(3) } );
        }
        if ( this.options.showfilename ) {
            rkWebUtil.elemaker( "br", td );
            rkWebUtil.elemaker( "span", td, { "text": "File: " + data.filename } );
        }
        rkWebUtil.elemaker( "br", td );
        if ( this.options.showband ) {
            rkWebUtil.elemaker( "span", td, { "text": "Band: " + data.filter + "  " } );
        }
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

        if ( this.options.rowcallback != null ) this.options.rowcallback( tr, data );
    }
}

// **********************************************************************

export { CutoutList }
