"use strict";

let statisticsZoomAjax;

(($) => {
    
    window.updateStatistic  = async function () {
        const spinner = $('#statisticSpinner');
        const btn = $('#statisticButton');

        spinner.show();
        btn.attr("disabled", true);

        console.log('zooms statistic')
        const data = statistic_json

        statisticsZoomAjax = $.ajax({
            url: `/zooms/statistics`,
            type: "POST",
            contentType: 'application/json',
            data: JSON.stringify(data),
            success: (responseData, status) => {
                // Обновление значений в таблице
                console.log(status)
                responseData = JSON.parse(responseData)
                responseData.data.forEach(item => {
                    const managerName = item.manager;
                    const uploaded = $(`[data-uploaded="${managerName}"]`);
                    uploaded.text(item.uploaded);
                    const dual = $(`[data-dual="${managerName}"]`);
                    dual.text(item.dual);
                });
                spinner.hide();
                btn.attr("disabled", false);
            },
            error: (err) => {
                console.log(err)
            },
            
        });
    }

})(jQuery);

