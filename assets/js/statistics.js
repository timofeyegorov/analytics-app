"use strict";


(($) => {


    $.extend({


        StatisticsFilter: function() {

            let _provider, _account, _campaign, _group;

            Object.defineProperties(this, {
                provider: {
                    set: (value) => {
                        _provider = value;
                    },
                    get: () => {
                        return _provider;
                    }
                },
               account: {
                    set: (value) => {
                        _account = value;
                    },
                    get: () => {
                        return _account;
                    }
                },
                campaign: {
                    set: (value) => {
                        _campaign = value;
                    },
                    get: () => {
                        return _campaign;
                    }
                },
                group: {
                    set: (value) => {
                        _group = value;
                    },
                    get: () => {
                        return _group;
                    }
                }
            });

            let url = new URL(window.location);

            this.provider = url.searchParams.get("provider");
            this.account = url.searchParams.get("account");
            this.campaign = url.searchParams.get("campaign");
            this.group = url.searchParams.get("group");

        }


    });


    $.fn.extend({


        SelectProvider: function() {

            if (!this.length) return this;

            let onchange = (event) => {
                let item = event.currentTarget,
                    value = item.value;
                item.reset();
                if (value) {
                    item.XHR = $.ajax({
                        url: `/api/statistics/accounts/${value}`,
                        success: (data) => {
                            if (item.account) item.account.delete();
                            filter.provider = value;
                            if (data.accounts.length) {
                                let field = $(`<div class="field"><label for="field-account">Кабинет</label><select name="account" id="field-account"><option value="">-- Все --</option></select></div>`);
                                data.accounts.forEach((account) => {
                                    field.find("select").append($(`<option value="${account.value}">${account.name}</option>`));
                                });
                                item.account = field.find("select")[0];
                                $(item).closest(".field").after(field);
                                field.find("select").SelectAccount();
                            } else {
                                item.account = null;
                            }
                        },
                        error: () => {
                            if (item.account) item.account.delete();
                            item.value = "";
                        }
                    });
                } else {
                    if (item.account) item.account.delete();
                }
            };

            return this.each((_, item) => {
                item.reset = function() {
                    if (this.XHR) {
                        this.XHR.abort();
                        this.XHR = undefined;
                    }
                    filter.provider = null;
                };
                $(item).bind("change", onchange).val(filter.provider).trigger("change");
            });

        },


        SelectAccount: function() {

            if (!this.length) return this;

            let onchange = (event) => {
                let item = event.currentTarget,
                    value = item.value;
                item.reset();
                if (value) {
                    item.XHR = $.ajax({
                        url: `/api/statistics/campaigns/${filter.provider}/${value}`,
                        success: (data) => {
                            if (item.campaign) item.campaign.delete();
                            filter.account = value;
                            if (data.campaigns.length) {
                                let field = $(`<div class="field"><label for="field-campaign">Кампания</label><select name="campaign" id="field-campaign"><option value="">-- Все --</option></select></div>`);
                                data.campaigns.forEach((campaign) => {
                                    field.find("select").append($(`<option value="${campaign.value}">${campaign.name}</option>`));
                                });
                                item.campaign = field.find("select")[0];
                                $(item).closest(".field").after(field);
                                field.find("select").SelectCampaign();
                            } else {
                                item.campaign = null;
                            }
                        },
                        error: () => {
                            if (item.campaign) item.campaign.delete();
                            item.value = "";
                        }
                    });
                } else {
                    if (item.campaign) item.campaign.delete();
                }
            };

            return this.each((_, item) => {
                item.reset = function() {
                    if (this.XHR) {
                        this.XHR.abort();
                        this.XHR = undefined;
                    }
                    filter.account = null;
                };
                item.delete = function() {
                    let field = this.closest(".field");
                    item.reset();
                    if (item.campaign) item.campaign.delete();
                    field.remove();
                };
                $(item).bind("change", onchange).val(filter.account).trigger("change");
            });

        },


        SelectCampaign: function() {

            if (!this.length) return this;

            let onchange = (event) => {
                let item = event.currentTarget,
                    value = item.value;
                item.reset();
                if (value) {
                    item.XHR = $.ajax({
                        url: `/api/statistics/groups/${filter.provider}/${value}`,
                        success: (data) => {
                            if (item.group) item.group.delete();
                            filter.campaign = value;
                            if (data.groups.length) {
                                let field = $(`<div class="field"><label for="field-group">Группа объявлений</label><select name="group" id="field-group"><option value="">-- Все --</option></select></div>`);
                                data.groups.forEach((group) => {
                                    field.find("select").append($(`<option value="${group.value}">${group.name}</option>`));
                                });
                                item.group = field.find("select")[0];
                                $(item).closest(".field").after(field);
                                field.find("select").SelectGroup();
                            } else {
                                item.group = null;
                            }
                        },
                        error: () => {
                            if (item.group) item.group.delete();
                            item.value = "";
                        }
                    });
                } else {
                    if (item.group) item.group.delete();
                }
            };

            return this.each((_, item) => {
                item.reset = function() {
                    if (this.XHR) {
                        this.XHR.abort();
                        this.XHR = undefined;
                    }
                    filter.campaign = null;
                };
                item.delete = function() {
                    let field = this.closest(".field");
                    item.reset();
                    if (item.group) item.group.delete();
                    field.remove();
                };
                $(item).bind("change", onchange).val(filter.campaign).trigger("change");
            });

        },


        SelectGroup: function() {

            if (!this.length) return this;

            return this.each((_, item) => {
                item.reset = function() {
                    if (this.XHR) {
                        this.XHR.abort();
                        this.XHR = undefined;
                    }
                    filter.group = null;
                };
                item.delete = function() {
                    let field = this.closest(".field");
                    item.reset();
                    field.remove();
                };
                $(item).val(filter.group);
            });

        }


    });


    let filter = new $.StatisticsFilter();


    $(() => {

        $("select[name=provider]").SelectProvider();

    });


})(jQuery);
