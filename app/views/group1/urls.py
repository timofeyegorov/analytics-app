from flask import current_app

from . import views


current_app.append("segments", "segments", views.SegmentsView)
current_app.append("turnover", "turnover", views.TurnoverView)
current_app.append("clusters", "clusters", views.ClustersView)
current_app.append("landings", "landings", views.LandingsView)
current_app.append("channels", "channels", views.ChannelsView)
current_app.append("traffic-sources", "traffic_sources", views.TrafficSourcesView)
current_app.append("segments-stats", "segments_stats", views.SegmentsStatsView)
current_app.append("leads-ta-stats", "leads_ta_stats", views.LeadsTAStatsView)
