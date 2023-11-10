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
