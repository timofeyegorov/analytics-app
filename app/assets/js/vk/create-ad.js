"use strict";


(($) => {


    $(() => {

        let account_field = $("#account_id")[0],
            campaign_field = $("#campaign_id")[0];

        let reload_dependes_fields = () => {
            if (!this.locked) {
                this.locked = true;
                $.ajax({
                    url: `/api/vk/create-ad/dependes-fields`,
                    data: {
                        account_id: account_field.value,
                        campaign_id: campaign_field.value,
                    },
                    success: (data) => {
                        account_field.available_options = data.accounts || [];
                        campaign_field.available_options = data.camaigns || [];
                    },
                    error: () => {
                        account_field.available_options = [];
                        campaign_field.available_options = [];
                    },
                    complete: () => {
                        this.locked = false;
                    }
                })
            }
        }

        Object.defineProperties(this,{
            locked:{
                get: () => {
                    return this._locked || false;
                },
                set: (value) => {
                    if (typeof value === "boolean") {
                        this._locked = value;
                    }
                }
            }
        });

        Object.defineProperties(account_field, {
            locked: {
                get: () => {
                    return this._locked || false;
                },
                set: (value) => {
                    if (typeof value === "boolean") {
                        account_field._locked = value;
                        if (value) {
                            account_field.setAttribute("disabled", "disabled");
                        } else {
                            account_field.removeAttribute("disabled");
                        }
                    }
                }
            },
            available_options: {
                set: (value) => {
                    let options = [];
                    account_field.locked = !value.length;
                    value.forEach((item) => {
                        options.push($(`<option value="${item[0]}"${item[2] ? ' selected="selected"' : ''}>${item[1]}</option>`))
                    });
                    $(account_field).html(options);
                }
            }
        });

        Object.defineProperties(campaign_field, {
            locked: {
                get: () => {
                    return this._locked || false;
                },
                set: (value) => {
                    if (typeof value === "boolean") {
                        campaign_field._locked = value;
                        if (value) {
                            campaign_field.setAttribute("disabled", "disabled");
                        } else {
                            campaign_field.removeAttribute("disabled");
                        }
                    }
                }
            },
            available_options: {
                set: (value) => {
                    let options = [];
                    campaign_field.locked = !value.length;
                    value.forEach((item) => {
                        options.push($(`<option value="${item[0]}"${item[2] ? ' selected="selected"' : ''}>${item[1]}</option>`))
                    });
                    $(campaign_field).html(options);
                }
            }
        });

        $(account_field, campaign_field).bind("change", (event) => {
            event.preventDefault();
            reload_dependes_fields();
        });

        reload_dependes_fields();

    });


})(jQuery);
