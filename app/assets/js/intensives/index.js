"use strict";


(($) => {


    $(() => {

        let changeAjax;

        $("form").bind("submit", (event) => {
            if (!event.currentTarget[0].value) event.currentTarget[0].disabled = true;
            if (!event.currentTarget[1].value) event.currentTarget[1].disabled = true;
        });

        $("table input").bind("change", (event) => {
            event.preventDefault();
            let target = event.currentTarget,
                field = $(target),
                tr = field.closest("tr"),
                course = tr.children("td.field-course").data("value"),
                date = tr.children("td.field-date").data("value"),
                data = {};
            $.map(tr.find("input"), (field) => {
                data[field.name] = field.value;
            });
            $("table input").attr("disabled", "disabled");
            if (changeAjax) {
                if (target !== changeAjax.field[0]) {
                    changeAjax.field.val(changeAjax.field.data("value"));
                }
                changeAjax.abort();
            }
            $.map(["deals_registrations", "profit_registrations"], (field_name) => {
                data[field_name] = field.closest("tr").find(`td.field-${field_name}`).data("value");
            });
            changeAjax = $.ajax({
                url: `/api/intensives/${course}/${date}`,
                type: "POST",
                data: data,
                success: (data, status) => {
                    if (status === "success") {
                        field.closest("tr").find(".estimate").text(data.estimate);
                        field.data("value", field.val());
                        field.closest("td").addClass("success");
                        if (target.changeZoomTimeout) clearTimeout(target.changeZoomTimeout);
                        target.changeZoomTimeout = setTimeout(() => {
                            field.closest("td").removeClass("success");
                        }, 1000);
                        $.map(data.fields, (value, field_name) => {
                            field.closest("tr").find(`td.field-${field_name}`).text(value);
                        });
                    }
                },
                error: () => {
                    field.val(field.data("value"));
                    field.closest("td").addClass("error");
                    if (target.changeZoomTimeout) clearTimeout(target.changeZoomTimeout);
                    target.changeZoomTimeout = setTimeout(() => {
                        field.closest("td").removeClass("error");
                    }, 1000);
                },
                complete: () => {
                    $("table input").attr("disabled", null);
                    changeAjax = undefined;
                }
            });
            changeAjax.field = field;
        });

    });


})(jQuery);
