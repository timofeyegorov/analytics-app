from .segments import calculate as segments_calculate
from .turnover import calculate as turnover_calculate
from .clusters import calculate as clusters_calculate
from .landings import calculate as landings_calculate
from .traffic_sources import calculate as traffic_sources_calculate
from .segments_stats import calculate as segments_stats_calculate
from .leads_ta_stats import calculate as leads_ta_stats_calculate
from .audience import (
    calculate_tables_by_date as audience_tables_by_date_calculate,
    calculate_type_result as audience_type_result_calculate,
    calculate_type_percent_result as audience_type_percent_result_calculate,
)
