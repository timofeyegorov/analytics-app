"use strict";

(($) => {


    $(() => {

        $("button.short-full-btn").bind("click", (event) => {
            let table = $(event.currentTarget).closest("table");
            table.toggleClass("short");
            $.cookie("table_short", table.hasClass("short"));
        });

    });


})(jQuery);
