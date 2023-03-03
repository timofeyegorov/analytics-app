"use strict";


(($) => {


    $(() => {

        $("#search-leads-response > .item > .title").bind("click", (event) => {
            event.preventDefault();
            $(event.currentTarget).parent().children(".extra").toggleClass("visible");
        });

        new ClipboardJS("#search-leads-response > .item > * > .param > .value > i", {
            text: (trigger) => {
                let target = $(trigger).next("span");
                target.parent().addClass("copied");
                if (target[0].CopiedTimeout) clearTimeout(target[0].CopiedTimeout);
                target[0].CopiedTimeout = setTimeout(() => {
                    target.parent().removeClass("copied");
                }, 500);
                return target.text();
            },
        });

    });


})(jQuery);
