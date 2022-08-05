"use strict";


(($) => {


    $(() => {

        $(".filter-actions > button.btn-reset").bind("click", (event) => {
            event.preventDefault();
            window.location = $(event.currentTarget).closest("form").attr("action");
        });

    });


})(jQuery);
