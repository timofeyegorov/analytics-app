"use strict";


(($) => {


    let reindex = () => {
        $(".utm-filters > .group").each((index, item) => {
            let group = $(item),
                select = group.find("select"),
                input = group.find("input");
            select.attr("id", `${select.attr("id").replace(/(_\d+)$/, "")}_${index}`);
            input.attr("id", `${input.attr("id").replace(/(_\d+)$/, "")}_${index}`);
        });
    };

    let click_remove = (event) => {
        event.preventDefault();
        $(event.currentTarget).closest(".group").remove();
        reindex();
    };

    let append_utm_filter = () => {
        let groups = $(".utm-filters > .group"),
            template = $(groups[0]),
            group = template.clone(),
            remove = $(`<div class="action"><div class="wrapper"><a href="${$("form").attr("action")}" class="btn btn-danger remove">Удалить</a></div></div>`);
        remove.bind("click", click_remove);
        group.children(".action").remove();
        group.find("select[name=utm]").val("");
        group.find("input[name=utm_value]").val("");
        group.append(remove);
        $(groups[groups.length-1]).after(group);
        reindex();
    };


    $(() => {

        $("form").bind("submit", (event) => {
            $(".utm-filters > .group").map((index, item) => {
                let group = $(item),
                    select = group.find("select"),
                    input = group.find("input");
                if (select.val() === "" || input.val() === "") {
                    select.val("").attr("disabled", "disabled");
                    input.val("").attr("disabled", "disabled");
                    if (index !== 0) group.remove();
                }
            });
        });

        $("form > .utm-filters > .group > .action > .wrapper > a.remove").bind("click", click_remove);

        $(".add-utm-filter").bind("click", (event) => {
            event.preventDefault();
            append_utm_filter();
            let groups = $(".utm-filters > .group select");
            $(groups[groups.length-1]).focus();
        });

    });


})(jQuery);
