"use strict";


(($) => {


    $(() => {

        let account_field = $("#account_id")[0],
            campaign_field = $("#campaign_id")[0],
            cost_type_field = $("#cost_type")[0],
            ad_format_field = $("#ad_format")[0],
            link_url_field = $("#link_url")[0],
            title_field = $("#title")[0],
            description_field = $("#description")[0],
            photo_file_field = $("#photo_file")[0],
            photo_field = $("#photo")[0];

        let prepare_fields = (is_init) => {
            let account_field_value = account_field.value,
                campaign_field_value = campaign_field.value,
                cost_type_field_value = cost_type_field.value,
                ad_format_field_value = ad_format_field.value,
                link_url_field_value = link_url_field.value,
                title_field_value = title_field.value,
                description_field_value = description_field.value,
                photo_field_value = photo_field.value;
            if (is_init) {
                account_field_value = account_field.dataset.value;
                campaign_field_value = campaign_field.dataset.value;
                cost_type_field_value = cost_type_field.dataset.value;
                ad_format_field_value = ad_format_field.dataset.value;
                link_url_field_value = link_url_field.dataset.value;
                title_field_value = title_field.dataset.value;
                description_field_value = description_field.dataset.value;
                photo_field_value = photo_field.dataset.value;
            }
            $(cost_type_field).val(cost_type_field_value);
            $(ad_format_field).val(ad_format_field_value);
            $(link_url_field).val(link_url_field_value);
            $(title_field).val(title_field_value);
            $(description_field).val(description_field_value);
            $(photo_field).val(photo_field_value);
            if (!this.locked) {
                this.locked = true;
                $.ajax({
                    url: `/api/vk/create-ad/dependes-fields`,
                    data: {
                        account_id: account_field_value,
                        campaign_id: campaign_field_value,
                        cost_type: cost_type_field_value,
                        ad_format: ad_format_field_value,
                    },
                    success: (data) => {
                        account_field.available_options = data.accounts || [];
                        campaign_field.available_options = data.campaigns || [];
                        cost_type_field.available_options = data.cost_type || [];
                        ad_format_field.available_options = data.ad_format || [];
                    },
                    error: () => {
                        account_field.available_options = [];
                        campaign_field.available_options = [];
                        cost_type_field.available_options = [];
                        ad_format_field.available_options = [];
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
                        options.push($(`<option value="${item[0]}"${item[2] ? ' selected="selected"' : ''}>${item[1]}</option>`));
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
                        options.push($(`<option value="${item[0]}"${item[2] ? ' selected="selected"' : ''}>${item[1]}</option>`));
                    });
                    $(campaign_field).html(options);
                }
            }
        });

        Object.defineProperties(cost_type_field, {
            locked: {
                get: () => {
                    return this._locked || false;
                },
                set: (value) => {
                    if (typeof value === "boolean") {
                        cost_type_field._locked = value;
                        if (value) {
                            cost_type_field.setAttribute("disabled", "disabled");
                        } else {
                            cost_type_field.removeAttribute("disabled");
                        }
                    }
                }
            },
            available_options: {
                set: (value) => {
                    let options = [];
                    cost_type_field.locked = !value.length;
                    value.forEach((item) => {
                        options.push($(`<option value="${item[0]}"${item[2] ? ' selected="selected"' : ''}>${item[1]}</option>`));
                    });
                    $(cost_type_field).html(options);
                }
            }
        });

        Object.defineProperties(ad_format_field, {
            locked: {
                get: () => {
                    return this._locked || false;
                },
                set: (value) => {
                    if (typeof value === "boolean") {
                        ad_format_field._locked = value;
                        if (value) {
                            ad_format_field.setAttribute("disabled", "disabled");
                        } else {
                            ad_format_field.removeAttribute("disabled");
                        }
                    }
                }
            },
            available_options: {
                set: (value) => {
                    let options = [];
                    ad_format_field.locked = !value.length;
                    value.forEach((item) => {
                        options.push($(`<option value="${item[0]}"${item[2] ? ' selected="selected"' : ''}>${item[1]}</option>`));
                    });
                    $(ad_format_field).html(options);
                }
            }
        });

        $(account_field).bind("change", (event) => {
            event.preventDefault();
            prepare_fields();
        });
        $(photo_file_field).bind("input", (event) => {
            event.preventDefault();
            $(photo_field).val("");
        });

        prepare_fields(true);

    });


})(jQuery);
