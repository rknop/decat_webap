import { rkWebUtil } from "./rkwebutil.js"
import { CutoutList } from "./cutoutlist.js"

var DecatVetting = function( div, auth, conn, galorexgal, vetbyothers ) {
    this.div = div;
    this.auth = auth;
    this.connector = conn;
    this.galorexgal = galorexgal;
    this.vetbyothers = vetbyothers;
}

// **********************************************************************

DecatVetting.prototype.render = function() {
    var self = this;
    
    if ( ! this.auth.authenticated ) {
        window.alert( "Error: not logged in, cannot vet objects." );
        return;
    }
    
    rkWebUtil.wipeDiv( this.div );
    rkWebUtil.elemaker( "p", this.div, { "text": "Getting objects to vet...", "classes": [ "warning" ] } );
    this.connector.sendHttpRequest( "getobjstovet",
                                    { "galorexgal": this.galorexgal,
                                      "alreadyvetted": this.vetbyothers },
                                    function( data ) {
                                        self.renderObjectsToVet( data );
                                    } );
}

// **********************************************************************

DecatVetting.prototype.renderObjectsToVet = function( data ) {
    var self = this;
    let button;

    this.rows = {};
    rkWebUtil.wipeDiv( this.div );

    button = rkWebUtil.button( this.div, "Give me more", function() { self.render() } );
    button.classList.add( "margex" );
    button = rkWebUtil.button( this.div, "Make All Unset Bad", function() { self.makeAllUnsetBad() } );
    button.classList.add( "margex" );
    let cutoutdiv = rkWebUtil.elemaker( "div", this.div );
    let cutoutlist = new CutoutList( cutoutdiv, { "showfilename": false,
                                                  "showrbs": false,
                                                  "rowcallback": function( tr, obj ) {
                                                      self.renderRow( tr, obj );
                                                  } } );
    cutoutlist.render( data.objs );
    button = rkWebUtil.button( this.div, "Give me more", function() { self.render() } );
    button.classList.add( "margex" );
    button = rkWebUtil.button( this.div, "Make All Unset Bad", function() { self.makeAllUnsetBad() } );
    button.classList.add( "margex" );
}

// **********************************************************************

DecatVetting.prototype.renderRow = function( tr, obj ) {
    let self = this;
    let td, span;

    let row = {};
    td = rkWebUtil.elemaker( "td", tr );
    row.good = rkWebUtil.elemaker( "input", td,
                                   { "attributes":
                                     { "type": "radio",
                                       "name": "goodbad-"+obj.objectdata_id,
                                       "id": "good-"+obj.objectdata_id,
                                       "value": "good"
                                     }
                                   } );
    rkWebUtil.elemaker( "label", td, { "text": "good", "attributes": { "for": "good-"+obj.objectdata_id } } );
    rkWebUtil.elemaker( "br", td );
    row.bad = rkWebUtil.elemaker( "input", td,
                                  { "attributes":
                                    { "type": "radio",
                                      "name": "goodbad-"+obj.objectdata_id,
                                      "id": "bad-"+obj.objectdata_id,
                                      "value": "bad" }
                                  }
                                               );
    rkWebUtil.elemaker( "label", td, { "text": "bad", "attributes": { "for": "bad-"+obj.objectdata_id } } );
    rkWebUtil.elemaker( "br", td );
    span = rkWebUtil.elemaker( "span", td, { "text": "Current status: " } );
    row.status = rkWebUtil.elemaker( "span", span );
    this.setRowGoodBadInterface( row, obj.goodbad );

    let gbfunc = function() {
        let gb;
        if ( row.good.checked ) {
            gb = "good";
        } else if ( row.bad.checked ) {
            gb = "bad";
        } else {
            gb = "unset";
        }
        rkWebUtil.wipeDiv( row.status );
        row.status.appendChild( document.createTextNode( "...sending..." ) );
        row.status.classList.remove( ...row.status.classList );
        row.status.classList.add( "warning" );
        self.sendManyGoodBads( [ obj.objectdata_id ], [ gb ] );
    }

    row.good.addEventListener( "change", gbfunc ) 
    row.bad.addEventListener( "change", gbfunc );

    this.rows[ obj.objectdata_id ] = row;
}

// **********************************************************************

DecatVetting.prototype.setRowGoodBadInterface = function( row, goodbad )
{
    if ( goodbad == "good" ) {
        row.bad.checked = false;
        row.good.checked = true;
        rkWebUtil.wipeDiv( row.status );
        row.status.appendChild( document.createTextNode( "good" ) );
        row.status.classList.remove( ...row.status.classList );
        row.status.classList.add( "good" );
    }
    else if ( goodbad == "bad" ) {
        row.good.checked = false;
        row.bad.checked = true;
        rkWebUtil.wipeDiv( row.status );
        row.status.appendChild( document.createTextNode( "bad" ) );
        row.status.classList.remove( ...row.status.classList );
        row.status.classList.add( "bad" );
    }
    else if ( goodbad == "unset" ) {
        row.good.checked = false;
        row.bad.checked = false;
        rkWebUtil.wipeDiv( row.status );
        row.status.appendChild( document.createTextNode( "(unset)" ) );
        row.status.classList.remove( ...row.status.classList );
        row.status.classList.add( "italic" );
    }
    else {
        row.good.checked = false;
        row.bad.checked = false;
        rkWebUtil.wipeDiv( row.status );
        row.status.appendChild( document.createTextNode( "Error: " + row.status + " unknown" ) );
        row.status.classList.remove( ...row.status.classList );
        row.status.classList.add( "warning" );
    }
}

// **********************************************************************

DecatVetting.prototype.sendManyGoodBads = function( objids, gbs ) {
    let self = this;
    this.connector.sendHttpRequest( "setgoodbad", { "objectdata_ids": objids, "goodbads": gbs },
                               function( data ) { self.updateGoodBadStatus( data ) } );
}

// **********************************************************************

DecatVetting.prototype.updateGoodBadStatus = function( data ) {
    for ( let row of data ) {
        this.setRowGoodBadInterface( this.rows[row.objectdata_id], row.goodbad );
    }
}

// **********************************************************************

DecatVetting.prototype.makeAllUnsetBad = function() {
    if ( ! confirm( "Really set all unset objects to bad?  Be careful!" ) )
        return;
    
    let objs = [];
    let gbs = [];
    for ( let objid in this.rows ) {
        let row = this.rows[objid];
        if ( ( ! row.good.checked ) & ( ! row.bad.checked ) ) {
            objs.push( objid );
            gbs.push( "bad" );
            rkWebUtil.wipeDiv( row.status );
            row.status.appendChild( document.createTextNode( "...sending..." ) );
            row.status.classList.remove( ...row.status.classList );
            row.status.classList.add( "warning" );
        }
    }
    this.sendManyGoodBads( objs, gbs );
}

// **********************************************************************

export { DecatVetting }
