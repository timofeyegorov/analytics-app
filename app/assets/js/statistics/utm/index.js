"use strict";


(($) => {


    $(() => {

        $("form").bind("submit", (event) => {
            let url = new URL(window.location);
            $("input[name=details]").remove();
            if (!event.currentTarget[0].value) event.currentTarget[0].disabled = true;
            if (!event.currentTarget[1].value) event.currentTarget[1].disabled = true;
            if (event.currentTarget[2].value === "__all__") event.currentTarget[2].disabled = true;
            if (event.currentTarget[3].value === "__all__") event.currentTarget[3].disabled = true;
            if (event.currentTarget[4].value === "__all__") event.currentTarget[4].disabled = true;
            if (event.currentTarget[5].value === "__all__") event.currentTarget[5].disabled = true;
            if (event.currentTarget[6].value === "__all__") event.currentTarget[6].disabled = true;
            if (url.searchParams.has("details")) {
                event.currentTarget.append($(`<input type="hidden" name="details" value="${url.searchParams.get("details")}">`)[0]);
            }
        });

    });


})(jQuery);
