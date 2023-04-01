"use strict";


(($) => {


    $(() => {

        $("#field-hide_inactive_managers").bind("change", (event) => {
            let inactive = $("table tr.inactive");
            event.preventDefault();
            event.currentTarget.checked ? inactive.addClass("hidden") : inactive.removeClass("hidden");
        });

        $("#field-month").bind("input", (event) => {
            let value = event.currentTarget.value,
                start_date = new Date(value),
                end_date = new Date(value);
            end_date.setMonth(end_date.getMonth() + 1);
            end_date.setDate(end_date.getDate() - 1);
            $("#field-value_date_from").val(start_date.toISOString().slice(0,10));
            $("#field-profit_date_from").val(start_date.toISOString().slice(0,10));
            $("#field-value_date_to").val(end_date.toISOString().slice(0,10));
            $("#field-profit_date_to").val("");
            $("form").submit();
        });

    });


})(jQuery);
