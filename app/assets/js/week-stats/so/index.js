"use strict";


(($) => {


    Array.prototype.sum = function() {
        return this.reduce((v1, v2) => {
            let sum = 0,
                v1_parse = parseInt(v1),
                v2_parse = parseInt(v2);
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
            if (!isNaN(value_parse) && `${value_parse}` === `${value}`) return Math.ceil(value_parse).toString().replace(/\B(?=(\d{3})+(?!\d))/g, " ");
            return "";
        };

        let recalculate = () => {
            let table = $("#statistics > table"),
                td = table.find("tbody > tr > td"),
                is_accumulative = accumulative_field[0].checked,
                is_profit = profit_field[0].checked,
                zooms = 0;

            td.filter((_, item) => item.cellIndex === 3 && item.parentElement.rowIndex === 1).each((_, item) => {
                zooms = parseInt(item.dataset.value) / (is_accumulative ? td.filter((_, item) => item.cellIndex === 3).slice(1).length : 1);
                item.innerText = render_int(zooms);
            });

            if (is_accumulative) {
                td.filter((_, item) => item.cellIndex === 4).slice(1).each((_, item) => {
                    let row_index = item.parentElement.rowIndex;
                    item.innerText = render_int(parseInt(item.dataset.value) / (is_profit ? parseInt(td.filter((_, item) => item.cellIndex === 3 && item.parentElement.rowIndex === row_index)[0].dataset.value) : 1));
                });
                td.filter((_, item) => item.cellIndex > 4 && item.parentElement.rowIndex > 1 && `${item.dataset.value}` !== "").each((_, item) => {
                    let row_index = item.parentElement.rowIndex,
                        cell_index = item.cellIndex;
                    item.innerText = render_int(td.filter((_, item) => item.cellIndex > 4 && item.cellIndex <= cell_index && item.parentElement.rowIndex === row_index).map((_, item) => item.dataset.value).toArray().sum() / (is_profit ? parseInt(td.filter((_, item) => item.cellIndex === 3 && item.parentElement.rowIndex === row_index)[0].dataset.value) : 1));
                });
                td.filter((_, item) => item.cellIndex === 4 && item.parentElement.rowIndex === 1). each((_, item) => {
                    let row_index = item.parentElement.rowIndex,
                        cell_index = item.cellIndex;
                    item.innerText = render_int(parseInt(item.dataset.value) / td.filter((_, item) => item.cellIndex === cell_index && item.parentElement.rowIndex > 1 && `${item.dataset.value}` !== "").length / (is_profit ? zooms : 1));
                });
                td.filter((_, item) => item.cellIndex > 4 && item.parentElement.rowIndex === 1).each((_, item) => {
                    let row_index = item.parentElement.rowIndex,
                        cell_index = item.cellIndex;
                    item.innerText = render_int(td.filter((_, item) => item.cellIndex > 4 && item.cellIndex <= cell_index && item.parentElement.rowIndex === row_index).map((_, item) => item.dataset.value).toArray().sum() / (is_profit ? parseInt(td.filter((_, item) => item.cellIndex === 3 && item.parentElement.rowIndex === 1)[0].dataset.value) : td.filter((_, item) => item.cellIndex > 4 && item.parentElement.rowIndex === 1).length));
                });
            } else {
                td.filter((_, item) => item.cellIndex > 3 && `${item.dataset.value}` !== "").each((_, item) => {
                    let row_index = item.parentElement.rowIndex;
                    item.innerText = render_int(parseInt(item.dataset.value) / (is_profit ? parseInt(td.filter((_, item) => item.cellIndex === 3 && item.parentElement.rowIndex === row_index)[0].dataset.value) : 1));
                });
            }
        };

        let render_table = () => {
            let table = $("#statistics > table"),
                td = table.find("tbody > tr > td");

            td.filter((_, item) => item.cellIndex === 3).slice(1).each((_, item) => {
                $(item).text(render_int(item.dataset.value));
            });

            td.filter((_, item) => item.cellIndex === 4).slice(1).each((_, item) => {
                item.dataset.value = $(item.parentElement).children("td").slice(4).map((_, item) => item.dataset.value).toArray().sum();
            });

            td.filter((_, item) => item.parentElement.rowIndex === 1).slice(2).each((_, item) => {
                let cell_index = item.cellIndex;
                item.dataset.value = td.filter((_, item) => item.cellIndex === cell_index).slice(1).map((_, item) => item.dataset.value).toArray().sum();
            });

            recalculate();

            table.removeClass("hidden");
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
