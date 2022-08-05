"use strict";


(($) => {


    $.fn.extend({

        AnalyticGanttChart: function() {

            if (!this.length) return this;

            let _render_gantt = (gantt, container) => {
                let channels = window.AnalyticsChannels.data;
                let min = window.AnalyticsChannels.date_range[0],
                    max = window.AnalyticsChannels.date_range[1],
                    days = (max-min)/86400/1000+1,
                    axis_y = $(`<div class="axis-y"></div>`),
                    board = $(`<div class="board"><div class="wrapper"><div class="gantt"></div></div></div>`);
                let days_layout = $("<div></div>"),
                    axis_x = $(`<div class="item axis-x"><div class="wrapper"></div></div>`);
                for (let i=0; i<days; i++) {
                    let day = new Date(min.getTime()+86400000*i),
                        d = day.getDate(),
                        m = day.getMonth()+1,
                        y = day.getFullYear();
                    d = `${d}`.length === 1 ? `0${d}` : d;
                    m = `${m}`.length === 1 ? `0${m}` : d;
                    days_layout.append($(`<span class="day _${day.getTime()}"></span>`));
                    axis_x.children(".wrapper").append($(`<span class="day">${d}.${m}.${y}</span>`));
                }
                for (let name in channels) {
                    let channel = channels[name];
                    axis_y.append($(`<div class="item">&nbsp;${channel.name}</div>`));
                        let line = $(`<div class="item"><div class="wrapper"></div></div>`);
                    let line_days = days_layout.clone();
                    for (let index in channel.dates) {
                        let d = channel.dates[index].getDate(),
                            m = channel.dates[index].getMonth()+1,
                            y = channel.dates[index].getFullYear();
                        d = `${d}`.length === 1 ? `0${d}` : d;
                        m = `${m}`.length === 1 ? `0${m}` : d;
                        line_days.find(`span._${channel.dates[index].getTime()}`).addClass("colored").attr("title", `${d}.${m}.${y}`);
                    }
                    line.children(".wrapper").append(line_days.find("span"));
                    board.find(".wrapper > .gantt").append(line);
                }
                board.find(".wrapper > .gantt").prepend(axis_x);
                gantt.append(axis_y, board);
            }

            let _render = (container) => {
                _render_gantt(container.find(".gantt > .wrapper"), container);
            }

            let _init = (container) => {
                _render(container);
            }

            return this.each((index, item) => {
                _init($(item));
            });

        }

    });


    $(() => {

        $("#gantt-channels").AnalyticGanttChart();

    });


})(jQuery);
