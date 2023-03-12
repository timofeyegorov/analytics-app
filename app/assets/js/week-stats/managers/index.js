"use strict";


(($) => {


    $(() => {

        $("#field-hide_inactive_managers").bind("change", (event) => {
            let inactive = $("table tr.inactive");
            event.preventDefault();
            event.currentTarget.checked ? inactive.addClass("hidden") : inactive.removeClass("hidden");
        });

    });


})(jQuery);
