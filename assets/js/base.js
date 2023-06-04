"use strict";


(($) => {


    Array.prototype.in = function(value) {
        return this.indexOf(value) > -1;
    };


    $(() => {

        $("form.form-filter").bind("submit", (event) => {
            $.map(event.currentTarget, (field) => {
                if (field.nodeName === "INPUT" && ["text", "date", "hidden", "submit"].in(field.type)) {
                    field.disabled = ["submit"].in(field.type) ? true : field.value === "";
                } else if (field.nodeName === "SELECT" && ["select-one"].in(field.type)) {
                    field.disabled = field.value === "";
                }
            });
        });

        $("form.form-filter input[type=reset]").bind("click", (event) => {
            event.preventDefault();
            window.location = $(event.currentTarget).closest("form").attr("action");
        });

    });


})(jQuery);
