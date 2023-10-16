"use strict";


(($) => {


    $(() => {

        var xhr_upload = undefined;

        $(".base-links > .upload + input").bind("change", (event) => {
            if (xhr_upload !== undefined) return;

            $(".base-links").append($('<div class="overlay"></div>'));
            $(".upload-data").html("");

            var files = event.currentTarget.files;
            if (!files.length) return;

            var data = new FormData();
            data.append("file", files[0]);

            xhr_upload = $.ajax({
                url: "/api/v1/upload/leads",
                type: "POST",
                processData: false,
                contentType: false,
                data: data,
                success: (data, status, xhr) => {
                    if (status === "success") {
                        $(".upload-data").append(`<div class="item"><span>Найдено в источнике:</span> ${data.source}</div><div class="item"><span>Добавлено в БД:</span> ${data.added}</div>`);
                    }
                },
                error: (xhr) => {
                    $(".upload-data").append(`<div class="btn-danger errors">${xhr.status}: ${xhr.statusText}</div>`);
                },
                complete: () => {
                    $(".base-links > .overlay").remove();
                    xhr_upload = undefined;
                    $(event.currentTarget).val("");
                }
            });
        });

        $(".base-links > .upload").bind("click", (event) => {
            $(event.currentTarget).next("input").trigger("click");
        });

    });


})(jQuery);
