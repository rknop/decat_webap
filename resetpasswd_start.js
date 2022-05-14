import { rkAuth } from "./rkauth.js"

// ****
// Set the variable webapurl
import { webapconfig } from "./decatview_config.js"
var webapurl = webapconfig.webapurl;
// ****

rkAuth.started = false;
rkAuth.init_interval = window.setInterval(
    function()
    {
        var requestdata, renderer;
        if ( document.readyState == "complete" ) {
            if ( !rkAuth.started ) {
                rkAuth.started = true;
                window.clearInterval( rkAuth.init_interval );
                let div = document.getElementById( "authdiv" );
                let auther = new rkAuth( div, webapurl, null );
                let button = document.getElementById( "setnewpassword_button" );
                button.addEventListener( "click", function() { auther.getPrivKey() } );
            }
        }
    },
    100);
            
    
