"use strict";


(($) => {


    $.fn.extend({


        TableOrdering: function() {

            if (!this.length) return this;

            let parse_orderby = (table) => {
                let th = $(table).find("th.ordering"),
                    url = new URL(window.location),
                    orderby = url.searchParams.get("orderby"),
                    available = [],
                    output = [];
                th.each((_, item) => {
                    available.push(item.dataset.orderby);
                });
                if (orderby) {
                    orderby.split(",").forEach((item) => {
                        let direction = "asc";
                        if (item[0] === "-") {
                            direction = "desc";
                            item = item.substring(1);
                        }
                        if (available.indexOf(item) > -1) {
                            output.push({
                                name: item,
                                direction: direction
                            });
                        }
                    });
                }
                return output;
            }

            let define_props = (o) => {
                Object.defineProperties(o, {
                    names: {
                        get: () => {
                            let items = [];
                            o.forEach((item) => {
                                items.push(item.name);
                            })
                            return items;
                        }
                    },
                    query: {
                        get: () => {
                            let output = [];
                            o.forEach((item) => {
                                output.push(`${item.direction === "desc" ? "-" : ""}${item.name}`);
                            });
                            return output.join(",");
                        }
                    }
                })
            }

            let init = (table) => {
                let to = parse_orderby(table);
                define_props(to);
                table.TableOrdering = to;
                $(table).find("th.ordering").bind("click", (event) => {
                    let target = event.target,
                        url = new URL(window.location),
                        name = $(target).closest("th")[0].dataset.orderby,
                        index = table.TableOrdering.names.indexOf(name);
                    if ($(target).hasClass("remove")) {
                        table.TableOrdering = table.TableOrdering.filter(item => item.name !== name);
                        define_props(table.TableOrdering);
                    } else {
                        if (index === -1) {
                            table.TableOrdering.push({
                                name: name,
                                direction: "desc"
                            });
                        } else {
                            table.TableOrdering[index].direction = table.TableOrdering[index].direction === "asc" ? "desc" : "asc";
                        }
                    }
                    if (table.TableOrdering.query) {
                        url.searchParams.set("orderby", table.TableOrdering.query);
                    } else {
                        url.searchParams.delete("orderby");
                    }
                    window.location.href = decodeURI(url.href);
                });
            }

            return this.each((_, item) => {
                init(item);
            });

        }


    });


    $(() => {

        $("table").TableOrdering();

    });


})(jQuery);
