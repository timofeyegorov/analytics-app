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
                td_so = td.filter((_, item) => item.cellIndex === 3).slice(1),
                td_profit = td.filter((_, item) => item.cellIndex === 4).slice(1),
                td_values = td.filter((_, item) => item.cellIndex > 4 && item.parentElement.rowIndex > 1),
                td_total = td.filter((_, item) => item.cellIndex > 2 && item.parentElement.rowIndex === 1),
                is_accumulative = accumulative_field[0].checked,
                is_profit = profit_field[0].checked;

            td_so.each((_, item) => {
                item.dataset.data = item.dataset.value;
                $(item).text(render_int(item.dataset.value));
            });

            td_profit.each((_, item) => {
                let row = $(item.parentElement).children("td").slice(4),
                    row_td_so = $(item.parentElement).children("td")[2],
                    divider = is_profit ? parseInt(row_td_so.dataset.value) : 1;
                item.dataset.data = row.map((_, item) => item.dataset.value).toArray().sum() / divider;
                $(item).text(render_int(item.dataset.data));
            });

            td_values.each((_, item) => {
                let row_td_so = $(item.parentElement).children("td")[2],
                    divider = is_profit ? parseInt(row_td_so.dataset.data) : 1,
                    value = is_accumulative ? $(item.parentElement).children("td").filter((_, td) => td.cellIndex > 4 && td.cellIndex <= item.cellIndex).map((_, item) => item.dataset.value).toArray().sum() : item.dataset.value;
                item.dataset.data = `${item.dataset.value}` !== "" ? parseInt(value) / divider : "";
                $(item).text(`${item.dataset.data}` !== "" ? render_int(item.dataset.data) : "");
            });

            td_total.each((_, item) => {
                let td_col = td.filter((_, td) => td.cellIndex === item.cellIndex && td.parentElement.rowIndex > 1);
                $(item).text(render_int(td_col.map((_, item) => item.dataset.data).toArray().sum()));
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
