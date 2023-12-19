"use strict";

let statisticsZoomAjax;

(($) => {
    
    window.updateStatistic  = async function () {
        console.log('getting statistic')
        const managerName = "Алиев Анар";
        const uploadedValue = 12;
        const element = $('[data-uploaded="' + managerName + '"]');
        element.text(uploadedValue);
        const data = statistic_json

        statisticsZoomAjax = $.ajax({
            url: `/zooms/statistics`,
            type: "POST",
            headers: {
                'Content-Type': 'application/json'
            },
            data: JSON.stringify(data),
            success: (data, status) => {
                console.log(data, status)
            },
            error: (err) => {
                console.log(err)
            },
        });
    }

})(jQuery);

