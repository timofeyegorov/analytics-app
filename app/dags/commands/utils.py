import re

from typing import List, Dict, Any, Optional

from app.data import StatisticsRoistatPackageEnum


def roistat_detect_package(value) -> Optional[str]:
    if re.match(r"^vk.+$", value):
        return StatisticsRoistatPackageEnum.vk.name
    if (
        re.match(r"^direct\d+.*$", value)
        or re.match(r"^:openstat:direct\.yandex\.ru$", value)
        or re.match(r"^direct$", value)
    ):
        return StatisticsRoistatPackageEnum.yandex_direct.name
    if re.match(r"^ya\.master$", value) or re.match(r"^yulyayamaster$", value):
        return StatisticsRoistatPackageEnum.yandex_master.name
    if re.match(r"^facebook\d+.*$", value):
        return StatisticsRoistatPackageEnum.facebook.name
    if re.match(r"^mytarget\d+$", value):
        return StatisticsRoistatPackageEnum.mytarget.name
    if re.match(r"^google\d+$", value) or re.match(r"^g-adwords\d+$", value):
        return StatisticsRoistatPackageEnum.google.name
    if re.match(r"^site$", value):
        return StatisticsRoistatPackageEnum.site.name
    if re.match(r"^seo$", value):
        return StatisticsRoistatPackageEnum.seo.name
    if re.match(r"^:utm:.+$", value):
        return StatisticsRoistatPackageEnum.utm.name
    if value:
        print("Undefined package:", value)
    return StatisticsRoistatPackageEnum.undefined.name


def roistat_get_levels(
    dimensions: Dict[str, Dict[str, str]],
) -> Dict[str, Optional[str]]:
    output = {
        "package": StatisticsRoistatPackageEnum.undefined.name,
        "marker_level_1": "",
        "marker_level_2": "",
        "marker_level_3": "",
        "marker_level_4": "",
        "marker_level_5": "",
        "marker_level_6": "",
        "marker_level_7": "",
        "marker_level_1_title": StatisticsRoistatPackageEnum.undefined.value,
        "marker_level_2_title": StatisticsRoistatPackageEnum.undefined.value,
        "marker_level_3_title": StatisticsRoistatPackageEnum.undefined.value,
        "marker_level_4_title": StatisticsRoistatPackageEnum.undefined.value,
        "marker_level_5_title": StatisticsRoistatPackageEnum.undefined.value,
        "marker_level_6_title": StatisticsRoistatPackageEnum.undefined.value,
        "marker_level_7_title": StatisticsRoistatPackageEnum.undefined.value,
    }
    levels = dict(sorted(dimensions.items()))
    for name, level in levels.items():
        level_value = level.get("value", "")
        level_title = level.get("title", "")
        if not level_value:
            level_title = StatisticsRoistatPackageEnum.undefined.value
        output.update({name: level_value, f"{name}_title": level_title})
    output.update({"package": roistat_detect_package(output.get("marker_level_1"))})
    return output


def roistat_get_metrics(
    metrics: List[Dict[str, Any]], available_metrics: List[str]
) -> Dict[str, str]:
    return dict(
        map(
            lambda item: (item.get("metric_name"), item.get("value")),
            list(
                filter(
                    lambda value: value.get("metric_name") in available_metrics, metrics
                )
            ),
        )
    )
