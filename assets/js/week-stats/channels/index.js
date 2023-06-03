"use strict";


(($) => {


    $(() => {

        $("#field-month").bind("input", (event) => {
            let value = event.currentTarget.value,
                start_date = new Date(value),
                end_date = new Date(value);
            end_date.setMonth(end_date.getMonth() + 1);
            end_date.setDate(end_date.getDate() - 1);
            $("#field-order_date_from").val(start_date.toISOString().slice(0,10));
            $("#field-profit_date_from").val(start_date.toISOString().slice(0,10));
            $("#field-order_date_to").val(end_date.toISOString().slice(0,10));
            $("#field-profit_date_to").val("");
            $("form").submit();
        });

        $(".filter-block > .field.month > span").bind("click", (event) => {
            $("#field-month").val(event.currentTarget.dataset.value).trigger("input");
        });

    });


})(jQuery);
