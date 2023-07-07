"use strict";


(($) => {


    $(() => {

        let render_int = (value) => {
            let value_parse = parseFloat(value);
            if (!isNaN(value_parse) && `${value_parse}` === `${value}`)
                return Math.round(value_parse).toString().replace(/\B(?=(\d{3})+(?!\d))/g, " ");
            return "0";
        };

        let parse_value = (state, value, owner, full) => {
            if (state === "absolute") {
                return `${render_int(value)} ₽`;
            } else if (state === "percent_owner") {
                return `${render_int(owner === 0 ? 0 : value / owner * 100)} %`;
            } else if (state === "percent") {
                return `${render_int(full === 0 ? 0 : value / full * 100)} %`;
            }
            return "";
        };

        let recalc = () => {
            let state = "absolute";
            $(".field-checkbox > input").each((_, field) => {
                if (field.checked) state = field.name;
            });
            $("#statistics > table td.recalc").each((_, td) => {
                $(td).text(parse_value(state, parseFloat(td.dataset.value), parseFloat(td.dataset.owner), parseFloat(td.dataset.full)));
            });
        };

        $("form").bind("submit", (event) => {
            if (!event.currentTarget[0].value) event.currentTarget[0].disabled = true;
            if (!event.currentTarget[1].value) event.currentTarget[1].disabled = true;
        });

        $(".field-checkbox > input").bind("change", (event) => {
            let fields = $(".field-checkbox > input");
            event.preventDefault();
            fields.each((_, field) => {
                if (field !== event.currentTarget) field.checked = false;
            });
            recalc();
        });

        recalc();

    });


})(jQuery);
