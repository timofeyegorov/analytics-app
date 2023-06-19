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
                course = tr.children("td.course").text(),
                date = tr.children("td.date").text(),
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
                    $(".controllable").attr("disabled", null);
                    changeAjax = undefined;
                }
            });
            changeAjax.field = field;
        });

    });


})(jQuery);
