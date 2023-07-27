"use strict";


(($) => {


    $(() => {

        let render_int = (value) => {
            let value_parse = parseFloat(value);
            if (!isNaN(value_parse) && `${value_parse}` === `${value}`)
                return Math.round(value_parse).toString().replace(/\B(?=(\d{3})+(?!\d))/g, " ");
            return "0";
        };

        let render_percent = (value, decimal) => {
            let value_parse = parseFloat(value);
            decimal = decimal === undefined ? 0 : parseInt(decimal);
            let decimal_div = Math.pow(10, decimal);
            if (isNaN(value_parse) || `${value_parse}` !== `${value}`) value = 0;
            let values = `${Math.round(value_parse * decimal_div) / decimal_div}`.split(".");
            values[0] = Math.round(values[0]).toString().replace(/\B(?=(\d{3})+(?!\d))/g, " ");
            if (values[1] === undefined) values[1] = '';
            if (decimal > 0) values[1] = `${values[1]}${'0'.repeat(decimal - values[1].length)}`;
            return decimal > 0 ? values.join(".") : values[0];
        };

        let parse_value = (state, value, owner, full) => {
            if (state === "absolute") {
                return `${render_int(value)} ₽`;
            } else if (state === "percent_owner") {
                return `${render_percent(owner === 0 ? 0 : value / owner * 100, 2)} %`;
            } else if (state === "percent") {
                return `${render_percent(full === 0 ? 0 : value / full * 100, 2)} %`;
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
            if (event.currentTarget[2].value === "__all__") event.currentTarget[2].disabled = true;
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
