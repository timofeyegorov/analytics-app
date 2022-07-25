"use strict";


(($) => {


    $.fn.extend({

        AnalyticGanttChart: function() {

            if (!this.length) return this;

            let _parse_channels = (container) => {
                let channels = [];
                container.find(".items > ul > li > input:checked").not("#channel-item-all").each((index, item) => {
                    channels.push(item.value);
                });
                return channels;
            }

            let _render_gantt = (gantt, container, channels_alias) => {
                let channels = {};
                for (let alias in window.AnalyticsChannelsList) {
                    if (channels_alias.indexOf(alias) !== -1) channels[alias] = window.AnalyticsChannelsList[alias];
                }
                let min_max = [];
                for (let group in channels) {
                    let g_min = new Date(Math.min(...channels[group].dates)),
                        g_max = new Date(Math.max(...channels[group].dates));
                    min_max.push(g_min, g_max);
                }
                let min = new Date(Math.min(...min_max)),
                    max = new Date(Math.max(...min_max)),
                    axis_y_width = container.find(".items > ul").width(),
                    days = (max-min)/86400/1000,
                    axis_y = $(`<div class="axis-y"></div>`),
                    board = $(`<div class="board"><div class="wrapper"><div class="gantt"></div></div></div>`);
                axis_y.width(axis_y_width);
                board.css("padding-left", `${axis_y_width}px`);
                gantt.closest(".gantt").css("padding-left", `${axis_y_width}px`).css("margin-left", `-${axis_y_width}px`);
                let days_layout = $("<div></div>"),
                    axis_x = $(`<div class="item axis-x"><div class="wrapper"></div></div>`);
                for (let i=0; i<days; i++) {
                    let day = new Date(min.getTime()+86400000*i),
                        d = day.getDate(),
                        m = day.getMonth(),
                        y = day.getFullYear();
                    d = `${d}`.length === 1 ? `0${d}` : d;
                    m = `${m}`.length === 1 ? `0${m}` : d;
                    days_layout.append($(`<span class="day _${day.getTime()}"></span>`));
                    axis_x.children(".wrapper").append($(`<span class="day">${d}.${m}.${y}</span>`));
                }
                for (let name in channels) {
                    let channel = channels[name];
                    axis_y.append($(`<div class="item">${channel.name}</div>`));
                        let line = $(`<div class="item"><div class="wrapper"></div></div>`);
                    let line_days = days_layout.clone();
                    for (let index in channel.dates) {
                        let d = channel.dates[index].getDate(),
                            m = channel.dates[index].getMonth(),
                            y = channel.dates[index].getFullYear();
                        d = `${d}`.length === 1 ? `0${d}` : d;
                        m = `${m}`.length === 1 ? `0${m}` : d;
                        line_days.find(`span._${channel.dates[index].getTime()}`).addClass("colored").attr("title", `${d}.${m}.${y}`);
                    }
                    line.children(".wrapper").append(line_days.find("span"));
                    board.find(".wrapper > .gantt").append(line);
                }
                board.find(".wrapper > .gantt").append(axis_x);
                gantt.append(axis_y, board);
            }

            let _render = (container) => {
                let channels = _parse_channels(container),
                    gantt = container.find(".gantt > .wrapper");
                gantt.html("");
                if (channels.length) {
                    _render_gantt(gantt, container, channels);
                } else {
                    gantt.html("Каналы не выбраны");
                }
            }

            let _change_channel_check = (container, item) => {
                if (item[0].id === "channel-item-all") {
                    let checked = $("#channel-item-all")[0].checked;
                    container.find(".items > ul > li > input").each((index, item) => {
                        item.checked = checked;
                    });
                } else {
                    $("#channel-item-all")[0].checked = container.find(".items > ul > li > input:checked").not("#channel-item-all").length === container.find(".items > ul > li > input").not("#channel-item-all").length;
                }
                _render(container);
            }

            let _init = (container) => {
                container.find(".items > ul > li > input").bind("change", (event) => {
                    _change_channel_check(container, $(event.currentTarget));
                });
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
