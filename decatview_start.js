import { decatview } from "./decatview.js"

decatview.started = false;
decatview.init_interval = window.setInterval(
    function()
    {
        var requestdata, renderer;
        if ( document.readyState == "complete" ) {
            if ( !decatview.statrted ) {
                decatview.started = true;
                window.clearInterval( decatview.init_interval );
                renderer = new decatview.Context();
                renderer.init();
            }
        }
    },
    100);
