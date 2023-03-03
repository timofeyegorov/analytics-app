"use strict";


(($) => {


    $(() => {

        $("#search-leads-response > .item > .title").bind("click", (event) => {
            event.preventDefault();
            $(event.currentTarget).parent().children(".extra").toggleClass("visible");
        });

    });


})(jQuery);
