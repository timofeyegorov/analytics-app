"use strict";


(($) => {


    $(() => {

        $("form").bind("submit", (event) => {
            let url = new URL(window.location);
            if (!event.currentTarget[0].value) event.currentTarget[0].disabled = true;
            if (event.currentTarget[1].value === "__all__") event.currentTarget[1].disabled = true;
        });

    });


})(jQuery);
