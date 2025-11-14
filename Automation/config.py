"""
Configuration des dashboards et panels Grafana
"""

# Périodes à analyser
TIME_PERIODS = [
    {"label": "14 derniers jours", "days": 14, "grafana_range": "14d"},
    {"label": "2 derniers mois", "days": 60, "grafana_range": "60d"},
    {"label": "3 derniers mois", "days": 90, "grafana_range": "90d"}
]

# Configuration des dashboards et leurs panels
DASHBOARDS = [
    {
        "name": "Toolchain Performance",
        "uid": "cebe2faxirjeob",
        "panels": [
            {
                "name": "Demand Success Rate",
                "panel_id": "1",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "cebe2faxirjeob"
                },
                "sql_query": """
                    SELECT
                        (SUM(CASE WHEN dem.status = 'SUCCESS' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
                    FROM m_demand_statistics dem
                    WHERE dem.product IN (
                        'cos', 'cos.bucket', 'kafka.cluster', 'kafka.topic', 'lb.roksfast14',
                        'lb.rokshttps', 'lb.vsifast14', 'lb.vsihttps', 'mongodb.enterprise',
                        'mongodb.standard', 'mqaas.queue_manager', 'mqaas.reserved_capacity',
                        'mqaas.reserved_deployment', 'oradbaas.cdb', 'oradbaas.cluster_mgr',
                        'oradbaas.db_home', 'oradbaas.pdb', 'oradbaas.vm_cluster',
                        'package.dmzre_open', 'postgres', 'realm', 'realm.apcode',
                        'roks', 'roks.ns', 'server.vsi'
                    )
                    AND dem.action IN ('delete', 'update', 'create')
                    AND dem.status IN ('SUCCESS', 'ON_ERROR')
                    AND dem.create_date >= '{time_from}'
                    AND dem.end_date <= '{time_to}'
                    AND dem.requestor_uid LIKE 'Serviceld%'
                """,
                "format": "table",
                "aggregation": "last",
                "unit": "%",
                "thresholds": {"warning": 95, "critical": 90}
            },
            {
                "name": "Total Demands",
                "panel_id": "2",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "cebe2faxirjeob"
                },
                "sql_query": """
                    SELECT COUNT(*) as total
                    FROM m_demand_statistics dem
                    WHERE dem.product IN (
                        'cos', 'cos.bucket', 'kafka.cluster', 'kafka.topic', 'lb.roksfast14',
                        'lb.rokshttps', 'lb.vsifast14', 'lb.vsihttps', 'mongodb.enterprise',
                        'mongodb.standard', 'mqaas.queue_manager', 'mqaas.reserved_capacity',
                        'mqaas.reserved_deployment', 'oradbaas.cdb', 'oradbaas.cluster_mgr',
                        'oradbaas.db_home', 'oradbaas.pdb', 'oradbaas.vm_cluster',
                        'package.dmzre_open', 'postgres', 'realm', 'realm.apcode',
                        'roks', 'roks.ns', 'server.vsi'
                    )
                    AND dem.action IN ('delete', 'update', 'create')
                    AND dem.create_date >= '{time_from}'
                    AND dem.end_date <= '{time_to}'
                """,
                "format": "table",
                "aggregation": "last",
                "unit": "",
                "thresholds": {"warning": 1000, "critical": 500}
            },
            {
                "name": "Average Processing Time",
                "panel_id": "3",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "cebe2faxirjeob"
                },
                "sql_query": """
                    SELECT AVG(EXTRACT(EPOCH FROM (dem.end_date - dem.create_date))) as avg_time
                    FROM m_demand_statistics dem
                    WHERE dem.product IN (
                        'cos', 'postgres', 'mongodb.enterprise', 'roks'
                    )
                    AND dem.status = 'SUCCESS'
                    AND dem.create_date >= '{time_from}'
                    AND dem.end_date <= '{time_to}'
                """,
                "format": "table",
                "aggregation": "avg",
                "unit": "s",
                "thresholds": {"warning": 300, "critical": 600}
            }
        ]
    },
    {
        "name": "Infrastructure Monitoring",
        "uid": "infra-dashboard-uid",
        "panels": [
            {
                "name": "Server VSI Success Rate",
                "panel_id": "10",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "cebe2faxirjeob"
                },
                "sql_query": """
                    SELECT
                        (SUM(CASE WHEN dem.status = 'SUCCESS' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
                    FROM m_demand_statistics dem
                    WHERE dem.product = 'server.vsi'
                    AND dem.action IN ('create', 'delete', 'update')
                    AND dem.status IN ('SUCCESS', 'ON_ERROR')
                    AND dem.create_date >= '{time_from}'
                    AND dem.end_date <= '{time_to}'
                """,
                "format": "table",
                "aggregation": "last",
                "unit": "%",
                "thresholds": {"warning": 95, "critical": 90}
            },
            {
                "name": "ROKS Cluster Success Rate",
                "panel_id": "11",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "cebe2faxirjeob"
                },
                "sql_query": """
                    SELECT
                        (SUM(CASE WHEN dem.status = 'SUCCESS' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
                    FROM m_demand_statistics dem
                    WHERE dem.product IN ('roks', 'roks.ns')
                    AND dem.action IN ('create', 'delete')
                    AND dem.status IN ('SUCCESS', 'ON_ERROR')
                    AND dem.create_date >= '{time_from}'
                    AND dem.end_date <= '{time_to}'
                """,
                "format": "table",
                "aggregation": "last",
                "unit": "%",
                "thresholds": {"warning": 95, "critical": 90}
            }
        ]
    },
    {
        "name": "Database Services",
        "uid": "db-services-uid",
        "panels": [
            {
                "name": "PostgreSQL Provisioning Rate",
                "panel_id": "20",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "cebe2faxirjeob"
                },
                "sql_query": """
                    SELECT
                        (SUM(CASE WHEN dem.status = 'SUCCESS' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
                    FROM m_demand_statistics dem
                    WHERE dem.product = 'postgres'
                    AND dem.action = 'create'
                    AND dem.status IN ('SUCCESS', 'ON_ERROR')
                    AND dem.create_date >= '{time_from}'
                    AND dem.end_date <= '{time_to}'
                """,
                "format": "table",
                "aggregation": "last",
                "unit": "%",
                "thresholds": {"warning": 95, "critical": 90}
            },
            {
                "name": "MongoDB Operations Success",
                "panel_id": "21",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "cebe2faxirjeob"
                },
                "sql_query": """
                    SELECT
                        (SUM(CASE WHEN dem.status = 'SUCCESS' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
                    FROM m_demand_statistics dem
                    WHERE dem.product IN ('mongodb.enterprise', 'mongodb.standard')
                    AND dem.action IN ('create', 'delete', 'update')
                    AND dem.status IN ('SUCCESS', 'ON_ERROR')
                    AND dem.create_date >= '{time_from}'
                    AND dem.end_date <= '{time_to}'
                """,
                "format": "table",
                "aggregation": "last",
                "unit": "%",
                "thresholds": {"warning": 95, "critical": 90}
            },
            {
                "name": "Oracle DB Success Rate",
                "panel_id": "22",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "cebe2faxirjeob"
                },
                "sql_query": """
                    SELECT
                        (SUM(CASE WHEN dem.status = 'SUCCESS' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
                    FROM m_demand_statistics dem
                    WHERE dem.product IN ('oradbaas.cdb', 'oradbaas.pdb', 'oradbaas.db_home')
                    AND dem.action IN ('create', 'delete', 'update')
                    AND dem.status IN ('SUCCESS', 'ON_ERROR')
                    AND dem.create_date >= '{time_from}'
                    AND dem.end_date <= '{time_to}'
                """,
                "format": "table",
                "aggregation": "last",
                "unit": "%",
                "thresholds": {"warning": 95, "critical": 90}
            }
        ]
    }
]