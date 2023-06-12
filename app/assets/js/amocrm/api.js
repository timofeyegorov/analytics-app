"use strict";


(($) => {


    $(() => {

        const container = document.getElementById("jsoneditor");
        const options = {};
        const editor = new JSONEditor(container, options);
        editor.set(window.JSONEditorContent);

        $(window).bind("resize", (event) => {
            let height = $(window).innerHeight(),
                content_padding = parseInt($("#content").css("padding-top")) + parseInt($("#content").css("padding-bottom")),
                h1_height = $("h1").height() + parseInt($("h1").css("margin-bottom")),
                methods_height = $("#methods").height() + parseInt($("#methods").css("margin-bottom"));
            $("#content > .data").height(height - content_padding - h1_height - methods_height);
        }).trigger("resize");

    });


})(jQuery);
