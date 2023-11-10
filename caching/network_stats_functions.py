from util.data_transformer import re_sql

def read_from_database(
    stat,
    aggregation_epochs,
    database,
    period=None,
    all_periods=False,
):
    sql = """
        SELECT
            data
        FROM
            network_stats
        WHERE
            stat = 'epoch'
    """
    last_epoch = database.sql_return_one(re_sql(sql))
    if not last_epoch:
        last_epoch = 0
    else:
        last_epoch = last_epoch[0]

    sql = """
        SELECT
            from_epoch,
            to_epoch,
            data
        FROM
            network_stats
        WHERE
            stat = '%s'
    """ % stat
    if period:
        if period == [None, None]:
            sql += """
                AND
                    from_epoch IS NULL
                AND
                    to_epoch IS NULL
            """
        else:
            sql += """
                AND
                    from_epoch >= %s
                AND
                    to_epoch <= %s
            """ % (period[0], period[1])
    elif not all_periods:
        last_epoch_floored = int(last_epoch / aggregation_epochs) * aggregation_epochs
        sql += """
            AND
                from_epoch >= %s
        """ % last_epoch_floored
    stats_data = database.sql_return_all(re_sql(sql))

    return last_epoch, stats_data


def aggregate_nodes(data):
    # Aggregate data of multiple periods
    aggregated_nodes = {}
    for nodes in data:
        for identity, amount in nodes.items():
            if identity not in aggregated_nodes:
                aggregated_nodes[identity] = amount
            else:
                aggregated_nodes[identity] += amount

    # Reverse sort and extract the top 100
    top_100_aggregated_nodes = sorted(aggregated_nodes.items(), key=lambda l: (l[1], int(l[0])), reverse=True)[:100]

    return len(aggregated_nodes), top_100_aggregated_nodes
