sql_epoch_times = """
    SELECT
        key,
        int_val
    FROM
        consensus_constants
    WHERE
        KEY = 'checkpoints_period'
    OR
        key = 'checkpoint_zero_timestamp'
    ORDER BY
        key
    ASC
"""

sql_last_confirmed_block = """
    SELECT
        block_hash,
        epoch
    FROM
        blocks
    WHERE
        confirmed=true
    ORDER BY
        epoch
    DESC
    LIMIT 1
"""

sql_last_block = """
    SELECT
        block_hash,
        epoch
    FROM
        blocks
    ORDER BY
        epoch
    DESC
    LIMIT 1
"""
