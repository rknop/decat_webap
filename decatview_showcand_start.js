import { ShowCandidate } from "./decatview_showcand.js"

console.log( "In decatview_showcand_start.js" );

ShowCandidate.started = false;
ShowCandidate.init_interval = window.setInterval(
    function()
    {
        var requestdata, renderer;
        if ( document.readyState == "complete" ) {
            if ( !ShowCandidate.statrted ) {
                console.log( "Starting showcand" );
                ShowCandidate.started = true;
                window.clearInterval( ShowCandidate.init_interval );
                let candelem = document.getElementById( "showcand_candid" );
                let rbtypeelem = document.getElementById( "rbtype" );
                let rbtype;
                if ( rbtypeelem.value == "None" ) rbtype = null;
                else rbtype = parseInt( rbtypeelem.value );
                let div = document.getElementById( "main-div" );
                renderer = new ShowCandidate( div, candelem.value, rbtype );
                renderer.render();
            }
        }
    },
    100);
