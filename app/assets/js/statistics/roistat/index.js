"use strict";


(($) => {


    $(() => {

        $("form").bind("submit", (event) => {
            if (!event.currentTarget[0].value) event.currentTarget[0].disabled = true;
            if (!event.currentTarget[1].value) event.currentTarget[1].disabled = true;
            if (event.currentTarget[2].value === "__all__") event.currentTarget[2].disabled = true;
            if (event.currentTarget[3].value === "__all__") event.currentTarget[3].disabled = true;
            if (event.currentTarget[4].value === "__all__") event.currentTarget[4].disabled = true;
        });

    });


})(jQuery);
