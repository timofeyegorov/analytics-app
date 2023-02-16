"use strict";


(($) => {


    $(() => {

        $("form").bind("submit", (event) => {
            let url = new URL(window.location);
            if (!event.currentTarget[0].value) event.currentTarget[0].disabled = true;
            if (event.currentTarget[1].value === "__all__") event.currentTarget[1].disabled = true;
        });

        $("#statistics > table td.switchable").bind("switch", (event, value_type) => {
            $(event.currentTarget).text(event.currentTarget.dataset[value_type]);
        });

        $("#field-switch").bind("change", (event) => {
            $("#statistics > table td.switchable").trigger("switch", event.currentTarget.checked ? "percent": "absolute");
        });

    });


})(jQuery);
