"use strict";


(($) => {


    $(() => {

        $("form.form-filter").bind("submit", (event) => {
            let form = event.currentTarget,
                form_data = new FormData(form);
            form_data.forEach((value, name) => {
                let field = $(form).find(`[name=${name}]`)[0];
                if (field.nodeName === "INPUT" && ["date"].indexOf(field.type) > -1) {
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
