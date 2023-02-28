"use strict";


(($) => {


    Array.prototype.sum = function() {
        return this.reduce((v1, v2) => {
            let sum = 0,
                v1_parse = parseFloat(v1),
                v2_parse = parseFloat(v2);
            if (!isNaN(v1_parse) && `${v1_parse}` === `${v1}`) sum += v1_parse;
            if (!isNaN(v2_parse) && `${v2_parse}` === `${v2}`) sum += v2_parse;
            return sum;
        }, 0);
    };


    $(() => {

        let accumulative_field = $("#field-accumulative"),
            profit_field = $("#field-profit");

        let render_int = (value) => {
            let value_parse = parseFloat(value);
            if (!isNaN(value_parse) && `${value_parse}` === `${value}`)
                return Math.round(value_parse).toString().replace(/\B(?=(\d{3})+(?!\d))/g, " ");
            return "0";
        };

        let recalculate = () => {
            let table = $("#statistics > table"),
                td = table.find("tbody > tr > td"),
                td_zoom = td.filter((_, item) => item.cellIndex === 3).slice(1),
                td_zoom_total = td.filter((_, item) => item.cellIndex === 3)[0],
                td_profit = td.filter((_, item) => item.cellIndex === 4).slice(1),
                td_profit_total = td.filter((_, item) => item.cellIndex === 4)[0],
                td_values = td.filter((_, item) => item.cellIndex > 4 && item.parentElement.rowIndex > 1),
                td_total = td.filter((_, item) => item.cellIndex > 4 && item.parentElement.rowIndex === 1),
                is_accumulative = accumulative_field[0].checked,
                is_profit = profit_field[0].checked;

            td_zoom.each((_, item) => {
                $(item).text(render_int(item.dataset.value));
            });
            td_zoom_total.dataset.value = td_zoom.map((_, item) => item.dataset.value).toArray().sum();
            $(td_zoom_total).text(render_int(td_zoom_total.dataset.value));

            td_profit.each((_, item) => {
                let row = $(item.parentElement).children("td").slice(4),
                    row_td_zoom = $(item.parentElement).children("td")[2],
                    divider = parseInt(is_profit ? row_td_zoom.dataset.value : 1),
                    value = parseInt(row.map((_, item) => item.dataset.value).toArray().sum());
                item.dataset.value = value;
                $(item).text(render_int(value / divider));
            });
            td_profit_total.dataset.value = td_profit.map((_, item) => item.dataset.value).toArray().sum();
            $(td_profit_total).text(render_int(parseInt(td_profit_total.dataset.value) / parseInt(is_profit ? td_zoom_total.dataset.value : 1)));

            td_values.each((_, item) => {
                let row_td_zoom = $(item.parentElement).children("td")[2],
                    divider = parseInt(is_profit ? row_td_zoom.dataset.value : 1),
                    value = parseInt(is_accumulative ? $(item.parentElement).children("td").filter((_, td) => td.cellIndex > 4 && td.cellIndex <= item.cellIndex).map((_, item) => item.dataset.value).toArray().sum() : item.dataset.value);
                $(item).text(`${item.dataset.value}` !== "" ? render_int(value / divider) : "");
            });

            td_total.each((_, item) => {
                let col = td.filter((_, td) => td.cellIndex >= (is_accumulative ? 5 : item.cellIndex) && td.cellIndex <= item.cellIndex && td.parentElement.rowIndex > 1 && `${td.dataset.value}` !== ""),
                    zoom_value = td.filter((_, td_zoom) => td_zoom.cellIndex === 3 && td.filter((_, td) => td.cellIndex === item.cellIndex && td.parentElement.rowIndex > 1 && `${td.dataset.value}` !== "").map((_, item) => item.parentElement.rowIndex).toArray().indexOf(td_zoom.parentElement.rowIndex) > -1).map((_, item) => item.dataset.value).toArray().sum(),
                    value = col.map((_, item) => item.dataset.value).toArray().sum();
                $(item).text(render_int(value / (is_profit ? zoom_value : 1)));
            });
        };

        let render_table = () => {
            recalculate();
            $("#statistics > table").removeClass("hidden");
        };

        accumulative_field.bind("change", recalculate);
        profit_field.bind("change", recalculate);

        $("form").bind("submit", (event) => {
            if (!event.currentTarget[0].value) event.currentTarget[0].disabled = true;
            if (event.currentTarget[1].value === "__all__") event.currentTarget[1].disabled = true;
            if (event.currentTarget[2].value === "__all__") event.currentTarget[2].disabled = true;
        });

        render_table();

    });


})(jQuery);
