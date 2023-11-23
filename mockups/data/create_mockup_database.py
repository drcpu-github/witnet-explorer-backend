import argparse
import json
import sqlite3

import toml

from util.database_manager import DatabaseManager


def create_tables(database):
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    tables = [
        """
            CREATE TABLE IF NOT EXISTS addresses (
                id INT,
                address TEXT PRIMARY KEY,
                label TEXT,
                active INT,
                block INT,
                mint INT,
                value_transfer INT,
                data_request INT,
                'commit' INT,
                reveal INT,
                tally INT
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS hashes (
                hash TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                epoch INT
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS blocks (
                block_hash TEXT PRIMARY KEY,
                value_transfer INT NOT NULL,
                data_request INT NOT NULL,
                'commit' INT NOT NULL,
                reveal INT NOT NULL,
                tally INT NOT NULL,
                dr_weight INT NOT NULL,
                vt_weight INT NOT NULL,
                block_weight INT NOT NULL,
                epoch INT NOT NULL,
                tapi_signals INT,
                confirmed TEXT NOT NULL,
                reverted TEXT
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS mint_txns (
                txn_hash TEXT PRIMARY KEY,
                miner TEXT NOT NULL,
                output_addresses TEXT NOT NULL,
                output_values TEXT NOT NULL,
                epoch INT NOT NULL
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS value_transfer_txns (
                txn_hash TEXT PRIMARY KEY,
                input_addresses TEXT NOT NULL,
                input_values TEXT NOT NULL,
                input_utxos TEXT NOT NULL,
                output_addresses TEXT NOT NULL,
                output_values TEXT NOT NULL,
                timelocks TEXT NOT NULL,
                weight INT NOT NULL,
                epoch INT NOT NULL
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS data_request_txns (
                txn_hash TEXT PRIMARY KEY,
                input_addresses TEXT NOT NULL,
                input_values TEXT NOT NULL,
                input_utxos TEXT NOT NULL,
                output_address TEXT,
                output_value INT,
                witnesses INT NOT NULL,
                witness_reward INT NOT NULL,
                collateral INT NOT NULL,
                consensus_percentage INT NOT NULL,
                commit_and_reveal_fee INT NOT NULL,
                weight INT NOT NULL,
                kinds TEXT NOT NULL,
                urls TEXT NOT NULL,
                headers TEXT NOT NULL,
                bodies TEXT NOT NULL,
                scripts TEXT NOT NULL,
                aggregate_filters TEXT NOT NULL,
                aggregate_reducer TEXT NOT NULL,
                tally_filters TEXT NOT NULL,
                tally_reducer TEXT NOT NULL,
                RAD_bytes_hash TEXT NOT NULL,
                DRO_bytes_hash TEXT NOT NULL,
                epoch INT NOT NULL
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS commit_txns (
                txn_hash TEXT PRIMARY KEY,
                txn_address TEXT NOT NULL,
                input_values TEXT NOT NULL,
                input_utxos TEXT NOT NULL,
                output_value INT,
                data_request TEXT NOT NULL,
                epoch INT NOT NULL
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS reveal_txns (
                txn_hash TEXT PRIMARY KEY,
                txn_address TEXT NOT NULL,
                data_request TEXT NOT NULL,
                result TEXT NOT NULL,
                success TEXT NOT NULL,
                epoch INT NOT NULL
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS tally_txns (
                txn_hash TEXT PRIMARY KEY,
                output_addresses TEXT NOT NULL,
                output_values TEXT NOT NULL,
                data_request TEXT NOT NULL,
                error_addresses TEXT NOT NULL,
                liar_addresses TEXT NOT NULL,
                result TEXT NOT NULL,
                success TEXT NOT NULL,
                epoch INT NOT NULL
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS data_request_mempool (
                timestamp INT NOT NULL,
                fee TEXT NOT NULL,
                weight TEXT NOT NULL
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS value_transfer_mempool (
                timestamp INT NOT NULL,
                fee TEXT NOT NULL,
                weight TEXT NOT NULL
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS wips (
                id INT,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                urls TEXT NOT NULL,
                activation_epoch INT,
                tapi_start_epoch INT,
                tapi_stop_epoch INT,
                tapi_bit INT,
                tapi_json TEXT
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS consensus_constants (
                key TEXT PRIMARY KEY,
                int_val INT,
                str_val TEXT
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS network_stats (
                stat TEXT NOT NULL,
                from_epoch INT,
                to_epoch INT,
                data TEXT NOT NULL
            )
        """,
    ]
    for sql in tables:
        cursor.execute(sql)
    connection.commit()


def insert_address_data(database):
    address_data = [
        [
            1,
            "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq",
            "drcpu0",
            2024098,
            68,
            68,
            90,
            4272,
            1866,
            1866,
            3051,
        ],
        [
            2,
            "wit1drcpu2gf386tm29mh62cce0seun76rrvk5nca6",
            "drcpu2",
            1657960,
            21,
            21,
            29,
            2246,
            658,
            658,
            1406,
        ],
        [
            3,
            "wit1z8p6qp2f5z6j2nfex3kme5qee0vurpvd8yn5hj",
            None,
            1075951,
            133,
            131,
            7,
            0,
            744,
            743,
            744,
        ],
        [
            4,
            "wit1xshwjs5huexwfxkldvue7kc6cx230vuhtmme2s",
            None,
            1100848,
            130,
            124,
            8,
            0,
            1278,
            1275,
            1278,
        ],
        [
            5,
            "wit17ue5kphnvajes4y05s525e6y9hjr48tpxc3ruc",
            None,
            1113874,
            126,
            125,
            9,
            0,
            851,
            851,
            851,
        ],
        [
            6,
            "wit1my5tgl0r3lsft38kz748zaa7z48dd9994aq895",
            None,
            1100849,
            122,
            113,
            13,
            0,
            1354,
            1352,
            1354,
        ],
        [
            7,
            "wit1d90hma685ghdw33c30svx9889sdckw7kq4j4uf",
            None,
            1110639,
            120,
            119,
            9,
            0,
            738,
            738,
            738,
        ],
        [
            8,
            "wit1xc6002zplgwjdhgnrdxu9gpsg6d9czueeshnsz",
            None,
            1088845,
            118,
            106,
            7,
            0,
            951,
            951,
            951,
        ],
        [
            9,
            "wit1azrj8h2mg6dnq7nem8cp7c2mqcs887djd4e2wh",
            None,
            1101872,
            118,
            115,
            12,
            0,
            1401,
            1399,
            1401,
        ],
        [
            10,
            "wit1j3eyct469cpkz63kxx7mhffhltv5q7v45mgda3",
            None,
            1110633,
            117,
            113,
            9,
            0,
            850,
            846,
            850,
        ],
        [
            11,
            "wit1dvj7cshhvcqttua9afmlfkhk7vwvh0qwfjnwgc",
            None,
            1100843,
            116,
            104,
            8,
            0,
            773,
            773,
            773,
        ],
        [
            12,
            "wit1v9emzryhvu9czp39tyaz76de056g9wdtk5cfcz",
            None,
            1088839,
            111,
            107,
            9,
            0,
            1036,
            1035,
            1036,
        ],
        [
            13,
            "wit1yr9807edzm4l4k7thmw7duufvmm4mkzgqfsnr6",
            None,
            1845148,
            47,
            47,
            6,
            0,
            3181,
            3181,
            3181,
        ],
        [
            14,
            "wit1ajwk8tcajkwuqq5984rt07km7w9etgrvn59kfa",
            None,
            1842795,
            73,
            73,
            5,
            0,
            2977,
            2975,
            2977,
        ],
        [
            15,
            "wit1pr02yrydlm0qd7jhtj0mp4355aj2g3gcy9smaq",
            None,
            1824847,
            64,
            64,
            6,
            0,
            2862,
            2861,
            2862,
        ],
        [
            16,
            "wit15wd25cpstddkvxvfdzydcsnwym3d4mvwhjn2u2",
            None,
            1856712,
            41,
            41,
            7,
            0,
            2830,
            2826,
            2830,
        ],
        [
            17,
            "wit1l3yps6rl2ct8tleh2v632mcwts5s4cuhrvzmmu",
            None,
            1819644,
            52,
            52,
            7,
            0,
            2792,
            2785,
            2792,
        ],
        [
            18,
            "wit15k0huw65x8wkq3p06dmqxf3a46cdhkuljculsm",
            None,
            1389040,
            30,
            30,
            7,
            0,
            2767,
            2762,
            2767,
        ],
        [
            19,
            "wit1nwgm7dz3m339h3uxhyflvr6yfutk7kvfy6auff",
            None,
            1258429,
            22,
            22,
            8,
            0,
            2716,
            2709,
            2716,
        ],
        [
            20,
            "wit1w2hxy8h86p43wx9g722mx8ygs0dsdqlut3n7gw",
            None,
            1393126,
            33,
            33,
            6,
            0,
            2700,
            2698,
            2700,
        ],
        [
            21,
            "wit16m7pxuajzs8jsgwqnukqk39v3qc8dxypqahqdj",
            None,
            1259231,
            34,
            34,
            6,
            0,
            2644,
            2639,
            2644,
        ],
        [
            22,
            "wit15rv5eypwq54u6u3xc2ckd7vfyj53nnddmrmryt",
            None,
            2048900,
            62,
            62,
            7,
            0,
            3021,
            3019,
            3021,
        ],
        [
            23,
            "wit133wnd4ueheeme4dxjjy052s73sjf8uslah70s4",
            None,
            1071415,
            26,
            26,
            2,
            46955,
            340,
            340,
            29808,
        ],
        [
            24,
            "wit1k6vpqaajekgyx5whdp4ag7dp2h2627dcfjyngs",
            None,
            1071457,
            13,
            13,
            2,
            0,
            441,
            441,
            441,
        ],
        [
            25,
            "wit18j3jsfn6y6lc43v4x5tn3lgk8vzlj37hx6esqy",
            None,
            1071481,
            12,
            12,
            2,
            0,
            503,
            501,
            503,
        ],
        [
            26,
            "wit1aufqegyctt7h64eyyp2u6a68ultgkla5gnsxw8",
            None,
            1071495,
            16,
            16,
            2,
            0,
            353,
            353,
            353,
        ],
        [
            27,
            "wit14ef2z0l3l9plkuuqvcsqnq2azc86c2v7udjfeg",
            None,
            1071507,
            10,
            10,
            2,
            0,
            278,
            278,
            278,
        ],
        [
            28,
            "wit1dsxjndrkhpmgmd6nmt95nvmtqf667e5pmq2eqr",
            None,
            1071527,
            11,
            11,
            2,
            0,
            164,
            164,
            164,
        ],
        [
            29,
            "wit1ewhsfsjz5gdern8kaz8x5yd9ph04lyg3ewxssc",
            None,
            1071543,
            4,
            4,
            3,
            0,
            104,
            103,
            104,
        ],
        [
            30,
            "wit1g5n9m4s0lqa2am0edgfevnppu8y207yzjdhqag",
            None,
            1071547,
            9,
            9,
            2,
            0,
            211,
            211,
            211,
        ],
        [
            31,
            "wit1r7gszcq5hu2xvxhpufgs56c0m47tuwtnl3qdpa",
            None,
            1071554,
            8,
            8,
            2,
            0,
            261,
            261,
            261,
        ],
        [
            32,
            "wit10c45atl93vaudvpcmqtl8pqgt5lwzkk9r9mvrs",
            None,
            1071563,
            7,
            7,
            3,
            0,
            224,
            224,
            224,
        ],
        [
            33,
            "wit1a82dxlj8cy3afpxna6kqgq2r9plraf9raqq35m",
            None,
            1071421,
            27,
            27,
            2,
            0,
            1121,
            1121,
            1121,
        ],
        [
            34,
            "wit1gzntj0duaqjgexcjnwhgww9f564l7edx0khl6y",
            None,
            1071434,
            22,
            22,
            2,
            0,
            348,
            348,
            348,
        ],
        [
            35,
            "wit1ccm40u8d8z6ps28tkx6f6ruh340l77psgwhf95",
            None,
            1071451,
            17,
            17,
            2,
            0,
            361,
            360,
            361,
        ],
        [
            36,
            "wit1m6xt7zh3km2u9xzceqzaepkl5nj5qr4ss8zfed",
            None,
            1071473,
            21,
            21,
            2,
            0,
            525,
            525,
            525,
        ],
        [
            37,
            "wit1zxg8m7t6rqs0mpptcmmd8fr5hxe3hu7e849xr8",
            None,
            1071488,
            6,
            6,
            3,
            0,
            99,
            99,
            99,
        ],
        [
            38,
            "wit1m3u3hhs9fvqgt5a8d0zm7w7mhsytgz53pyf9hz",
            None,
            1071761,
            14,
            14,
            2,
            0,
            210,
            209,
            210,
        ],
        [
            39,
            "wit1xe6j5kf8a20xvhqjf7jsej06k45pxhu99u6dza",
            None,
            1071750,
            20,
            20,
            3,
            0,
            756,
            754,
            756,
        ],
        [
            40,
            "wit1kzmck6qyy503lme89lj02jxc3nuj45wg4l8r4c",
            None,
            1071734,
            14,
            14,
            2,
            0,
            398,
            398,
            398,
        ],
        [
            41,
            "wit1e49dg9me24esfmz8gkvhhszr7dvasc77mmq6nj",
            None,
            1071421,
            9,
            9,
            4,
            0,
            145,
            145,
            145,
        ],
        [
            42,
            "wit1astv06rz2m4pg9gaq5l79k34sh9w0j5nj2xe6k",
            None,
            1071426,
            29,
            29,
            2,
            0,
            573,
            573,
            573,
        ],
        [
            43,
            "wit19ejd5fgeypn9d8f6r8st2jkdx89e3vmp4eu3zv",
            None,
            1071425,
            21,
            21,
            2,
            0,
            473,
            473,
            473,
        ],
        [
            44,
            "wit1m5wsdsv7ard8va55tut5jk06nrx2gmxhgphqhl",
            None,
            1071428,
            14,
            14,
            2,
            0,
            300,
            300,
            300,
        ],
        [
            45,
            "wit1ulra63922mnls2rvktaqh4hhrruama9eqjnkc8",
            None,
            1071433,
            5,
            5,
            4,
            0,
            179,
            179,
            179,
        ],
        [
            46,
            "wit1n4gsyx57khmancx0cmzg2q975vvqayq55rlg0y",
            None,
            1071436,
            21,
            21,
            2,
            0,
            509,
            498,
            509,
        ],
        [
            47,
            "wit1fz0wz7dcpanadsdlke75zgjlhqxuv2vusvte8f",
            None,
            1071434,
            11,
            11,
            2,
            0,
            123,
            122,
            123,
        ],
        [
            48,
            "wit140uxs8r7asudphc7zzqc2ze8fk5ytkd6auxjf4",
            None,
            1071440,
            7,
            7,
            2,
            0,
            151,
            151,
            151,
        ],
        [
            49,
            "wit1mts8na78rrdg9j405uh5t9lklkpsdfm8tfze95",
            None,
            1071441,
            15,
            15,
            2,
            0,
            307,
            307,
            307,
        ],
        [
            50,
            "wit1k58yzyjse9frx5yx7xfg0g43mnuv5xw0y4venf",
            None,
            1259154,
            35,
            35,
            3,
            0,
            1010,
            1010,
            1010,
        ],
        [
            51,
            "wit1epgja4fpkyupwlxjawnth39usajywqdh9cxxlx",
            None,
            1071430,
            21,
            21,
            2,
            0,
            101,
            101,
            101,
        ],
        [
            52,
            "wit1gue84sf650hns8qsq4kn2x7awy29mrhdexdste",
            None,
            1071439,
            31,
            31,
            2,
            0,
            819,
            818,
            819,
        ],
    ]

    sql = """
        INSERT INTO
            addresses
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    cursor.executemany(sql, address_data)
    connection.commit()


def insert_consensus_constants(database):
    consensus_constants = [
        ["activity_period", 2000, None],
        [
            "bootstrap_hash",
            None,
            "[666564676f6573627272727c2f3030312f3738392f3432382f6130312e676966]",
        ],
        [
            "bootstrapping_committee",
            None,
            "[wit1g0rkajsgwqux9rnmkfca5tz6djg0f87x7ms5qx,wit1cyrlc64hyu0rux7hclmg9rxwxpa0v9pevyaj2c,wit1asdpcspwysf0hg5kgwvgsp2h6g65y5kg9gj5dz,wit13l337znc5yuualnxfg9s2hu9txylntq5pyazty,wit17nnjuxmfuu92l6rxhque2qc3u2kvmx2fske4l9,wit1etherz02v4fvqty6jhdawefd0pl33qtevy7s4z,wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq,wit1gxf0ca67vxtg27kkmgezg7dd84hwmzkxn7c62x,wit1hujx8v0y8rzqchmmagh8yw95r943cdddnegtgc,wit1yd97y52ezvhq4kzl6rph6d3v6e9yya3n0kwjyr,wit1fn5yxmgkphnnuu6347s2dlqpyrm4am280s6s9t,wit12khyjjk0s2hyuzyyhv5v2d5y5snws7l58z207g]",
        ],
        ["checkpoint_zero_timestamp", 1602666000, None],
        ["checkpoints_period", 45, None],
        ["collateral_age", 1000, None],
        ["collateral_minimum", 1000000000, None],
        ["epochs_with_minimum_difficulty", 2000, None],
        ["extra_rounds", 3, None],
        [
            "genesis_hash",
            None,
            "[6ca267d9accde3336739331d42d63509b799c6431e8d02b2d2cc9d3943d7ab02]",
        ],
        ["halving_period", 3500000, None],
        ["initial_block_reward", 250000000000, None],
        ["max_dr_weight", 80000, None],
        ["max_vt_weight", 20000, None],
        ["minimum_difficulty", 2000, None],
        ["mining_backup_factor", 8, None],
        ["mining_replication_factor", 3, None],
        ["reputation_expire_alpha_diff", 20000, None],
        ["reputation_issuance", 1, None],
        ["reputation_issuance_stop", 1048576, None],
        ["reputation_penalization_factor", 50, None],
        ["superblock_committee_decreasing_period", 5, None],
        ["superblock_committee_decreasing_step", 5, None],
        ["superblock_period", 10, None],
        ["superblock_signing_committee_size", 100, None],
    ]
    sql = """
        INSERT INTO
            consensus_constants
        VALUES
            (?, ?, ?)
    """
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    cursor.executemany(sql, consensus_constants)
    connection.commit()


def insert_network_stats(database):
    network_stats = [
        [
            "rollbacks",
            None,
            None,
            [
                [1667618550, 1443390, 1443971, 582],
                [1667577600, 1442480, 1443101, 622],
                [1667403900, 1438620, 1442461, 3842],
            ],
        ],
        ["epoch", None, None, 2024098],
        [
            "miners",
            None,
            None,
            {
                "amount": 416061,
                "top-100": [
                    [3, 133],
                    [4, 130],
                    [5, 126],
                    [6, 122],
                    [7, 120],
                    [8, 118],
                    [9, 118],
                    [10, 117],
                    [11, 116],
                    [12, 111],
                ],
            },
        ],
        [
            "data_request_solvers",
            None,
            None,
            {
                "amount": 574157,
                "top-100": [
                    [13, 3181],
                    [14, 2977],
                    [15, 2862],
                    [16, 2830],
                    [17, 2792],
                    [18, 2767],
                    [19, 2716],
                    [20, 2700],
                    [21, 2644],
                    [22, 2597],
                ],
            },
        ],
        [
            "miners",
            1000000,
            1001000,
            {
                "23": 1,
                "24": 1,
                "25": 1,
                "26": 1,
                "27": 1,
                "28": 1,
                "29": 1,
                "30": 1,
                "31": 1,
                "32": 1,
            },
        ],
        [
            "miners",
            1001000,
            1002000,
            {
                "33": 1,
                "34": 1,
                "35": 2,
                "36": 1,
                "37": 1,
                "38": 1,
                "26": 1,
                "39": 1,
                "40": 1,
                "30": 1,
            },
        ],
        [
            "data_request_solvers",
            1000000,
            1001000,
            {
                "41": 1,
                "42": 4,
                "43": 2,
                "44": 1,
                "45": 9,
                "46": 5,
                "47": 1,
                "34": 1,
                "48": 1,
                "49": 2,
            },
        ],
        [
            "data_request_solvers",
            1001000,
            1002000,
            {
                "50": 4,
                "41": 9,
                "42": 20,
                "43": 10,
                "51": 1,
                "45": 16,
                "46": 16,
                "34": 7,
                "52": 1,
                "49": 7,
            },
        ],
        [
            "staking",
            None,
            None,
            {
                "ars": [
                    69270770065,
                    242603004906,
                    302440834321,
                    384493409284,
                    501020356631,
                    595242122870,
                    1015683336066,
                    1141982577664,
                    1330485899039,
                ],
                "trs": [
                    215951770049,
                    301726789939,
                    384776953771,
                    492547412632,
                    579258511220,
                    807292242377,
                    1100113261644,
                    1216784921902,
                    1424372511663,
                ],
                "percentiles": [90, 80, 70, 60, 50, 40, 30, 20, 10],
            },
        ],
        [
            "data_requests",
            1000000,
            1001000,
            [
                1066,
                1057,
                3909,
                0,
                5,
                {"2": 5, "10": 813, "100": 248},
                {"1": 248, "500000": 5, "1000000": 813},
                {"1000000000": 5, "2500000000": 248, "5000000000": 813},
            ],
        ],
        [
            "data_requests",
            1001000,
            1002000,
            [
                1079,
                1073,
                4110,
                0,
                5,
                {"2": 5, "10": 826, "100": 248},
                {"1": 248, "500000": 5, "1000000": 826},
                {"1000000000": 5, "2500000000": 248, "5000000000": 826},
            ],
        ],
        [
            "data_requests",
            2023000,
            2024000,
            [
                748,
                740,
                2478,
                77,
                1,
                {"2": 1, "10": 500, "100": 247},
                {"1": 247, "500000": 1, "1000000": 500},
                {"1000000000": 1, "2500000000": 247, "5000000000": 500},
            ],
        ],
        [
            "data_requests",
            2024000,
            2025000,
            [
                1011,
                988,
                4012,
                97,
                1,
                {"2": 1, "10": 764, "100": 246},
                {"1": 246, "500000": 1, "1000000": 764},
                {"1000000000": 1, "2500000000": 246, "5000000000": 764},
            ],
        ],
        ["lie_rate", 1000000, 1001000, [32940, 367, 310, 239]],
        ["lie_rate", 1001000, 1002000, [33070, 271, 321, 220]],
        ["lie_rate", 2023000, 2024000, [29702, 305, 544, 98]],
        ["lie_rate", 2024000, 2025000, [32242, 654, 594, 135]],
        ["burn_rate", 1000000, 1001000, [0, 0]],
        ["burn_rate", 1001000, 1002000, [0, 0]],
        ["burn_rate", 2023000, 2024000, [0, 543000000000]],
        ["burn_rate", 2024000, 2025000, [0, 550500000000]],
        ["value_transfers", 1000000, 1001000, [312]],
        ["value_transfers", 1001000, 1002000, [266]],
        ["value_transfers", 2023000, 2024000, [57]],
        ["value_transfers", 2024000, 2025000, [98]],
    ]

    sql = """
        INSERT INTO
            network_stats
        VALUES
            (?, ?, ?, ?)
    """
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    cursor.executemany(
        sql, [[ns[0], ns[1], ns[2], json.dumps(ns[3])] for ns in network_stats]
    )
    connection.commit()


def insert_pending_transaction(database):
    connection = sqlite3.connect(database)
    cursor = connection.cursor()

    pending_data_requests = [
        [1696016220, [27379], [1]],
        [1696016340, [3, 14021], [1, 1]],
        [1696016520, [3, 27334], [1, 1]],
        [1696016580, [27871], [1]],
        [1696016640, [27364, 27379], [1, 1]],
        [1696016700, [3], [1]],
        [1696016760, [13928], [1]],
        [1696016880, [3], [1]],
        [1696017000, [14011, 27379], [1, 1]],
    ]

    sql = """
        INSERT INTO
            data_request_mempool
        VALUES
            (?, ?, ?)
    """
    cursor.executemany(
        sql,
        [
            [pdr[0], json.dumps(pdr[1]), json.dumps(pdr[2])]
            for pdr in pending_data_requests
        ],
    )

    pending_value_transfers = [
        [1696016160, [96, 120, 240, 240, 360, 360, 360], [1, 1, 2, 2, 3, 3, 3]],
        [1696016640, [120], [1]],
        [1696016700, [98], [1]],
        [1696016760, [119], [1]],
        [1696016820, [120], [1]],
        [1696016880, [77], [1]],
        [1696016940, [57], [1]],
        [1696017000, [37], [1]],
    ]

    sql = """
        INSERT INTO
            value_transfer_mempool
        VALUES
            (?, ?, ?)
    """
    cursor.executemany(
        sql,
        [
            [pvt[0], json.dumps(pvt[1]), json.dumps(pvt[2])]
            for pvt in pending_value_transfers
        ],
    )

    connection.commit()


def insert_wips(database):
    wips = [
        [
            1,
            "WIP0008",
            "Limit data request concurrency",
            ["https://github.com/witnet/WIPs/blob/master/wip-0008.md"],
            192000,
            None,
            None,
            None,
            None,
        ],
        [
            2,
            "WIP0009-0011-0012",
            "Adjust mining probability (WIP0009), improve superblock voting (WIP0011) and set minimum mining difficulty (WIP0012)",
            [
                "https://github.com/witnet/WIPs/blob/master/wip-0009.md,https://github.com/witnet/WIPs/blob/master/wip-0011.md,https://github.com/witnet/WIPs/blob/master/wip-0012.md"
            ],
            376320,
            None,
            None,
            None,
            None,
        ],
        [
            3,
            "THIRD_HARD_FORK",
            "Set a maximum eligibility for data requests",
            ["https://github.com/witnet/witnet-rust/pull/1957"],
            445440,
            None,
            None,
            None,
            None,
        ],
        [
            4,
            "WIP0014-0016",
            "Activation of TAPI itself (WIP0014) and setting a minimum data request mining difficulty (WIP0016)",
            [
                "https://github.com/witnet/WIPs/blob/master/wip-0014.md,https://github.com/witnet/WIPs/blob/master/wip-0016.md"
            ],
            549141,
            522240,
            549120,
            0,
            None,
        ],
        [
            5,
            "WIP0017-0018-0019",
            "Add a median RADON reducer (WIP0017), modify the UnhandledIntercept RADON error (WIP0018) and add RNG functionality to Witnet (WIP0019)",
            [
                "https://github.com/witnet/WIPs/blob/master/wip-0017.md,https://github.com/witnet/WIPs/blob/master/wip-0018.md,https://github.com/witnet/WIPs/blob/master/wip-0019.md"
            ],
            683541,
            656640,
            683520,
            1,
            None,
        ],
        [
            6,
            "WIP0020-0021",
            "Add support HTTP-POST (WIP0020) and add an XML parsing operator (WIP0021)",
            [
                "https://github.com/witnet/WIPs/blob/master/wip-0020.md,https://github.com/witnet/WIPs/blob/master/wip-0021.md"
            ],
            1059861,
            1032960,
            1059840,
            2,
            None,
        ],
        [
            7,
            "WIP0022 (defeated)",
            "Set a data request reward collateral ratio",
            ["https://github.com/witnet/WIPs/blob/master/wip-0022.md"],
            None,
            1655120,
            1682000,
            3,
            {
                "bit": 3,
                "urls": ["https://github.com/witnet/WIPs/blob/master/wip-0022.md"],
                "rates": [
                    {
                        "global_rate": 1.171875,
                        "periodic_rate": 31.5,
                        "relative_rate": 31.5,
                    },
                    {
                        "global_rate": 2.392113095238095,
                        "periodic_rate": 32.800000000000004,
                        "relative_rate": 32.15,
                    },
                    {
                        "global_rate": 3.616071428571429,
                        "periodic_rate": 32.9,
                        "relative_rate": 32.4,
                    },
                    {
                        "global_rate": 4.769345238095238,
                        "periodic_rate": 31.0,
                        "relative_rate": 32.05,
                    },
                    {
                        "global_rate": 6.045386904761905,
                        "periodic_rate": 34.300000000000004,
                        "relative_rate": 32.5,
                    },
                    {
                        "global_rate": 7.3065476190476195,
                        "periodic_rate": 33.900000000000006,
                        "relative_rate": 32.733333333333334,
                    },
                    {
                        "global_rate": 8.537946428571429,
                        "periodic_rate": 33.1,
                        "relative_rate": 32.785714285714285,
                    },
                    {
                        "global_rate": 9.873511904761905,
                        "periodic_rate": 35.9,
                        "relative_rate": 33.175,
                    },
                    {
                        "global_rate": 11.116071428571427,
                        "periodic_rate": 33.4,
                        "relative_rate": 33.2,
                    },
                    {
                        "global_rate": 12.566964285714285,
                        "periodic_rate": 39.0,
                        "relative_rate": 33.78,
                    },
                    {
                        "global_rate": 14.166666666666666,
                        "periodic_rate": 43.0,
                        "relative_rate": 34.61818181818182,
                    },
                    {
                        "global_rate": 15.982142857142856,
                        "periodic_rate": 48.8,
                        "relative_rate": 35.8,
                    },
                    {
                        "global_rate": 17.87202380952381,
                        "periodic_rate": 50.8,
                        "relative_rate": 36.95384615384615,
                    },
                    {
                        "global_rate": 19.750744047619047,
                        "periodic_rate": 50.5,
                        "relative_rate": 37.92142857142857,
                    },
                    {
                        "global_rate": 21.648065476190474,
                        "periodic_rate": 51.0,
                        "relative_rate": 38.79333333333334,
                    },
                    {
                        "global_rate": 23.645833333333332,
                        "periodic_rate": 53.7,
                        "relative_rate": 39.725,
                    },
                    {
                        "global_rate": 25.691964285714285,
                        "periodic_rate": 55.00000000000001,
                        "relative_rate": 40.62352941176471,
                    },
                    {
                        "global_rate": 27.67857142857143,
                        "periodic_rate": 53.400000000000006,
                        "relative_rate": 41.333333333333336,
                    },
                    {
                        "global_rate": 29.694940476190478,
                        "periodic_rate": 54.2,
                        "relative_rate": 42.01052631578948,
                    },
                    {
                        "global_rate": 31.685267857142858,
                        "periodic_rate": 53.5,
                        "relative_rate": 42.585,
                    },
                    {
                        "global_rate": 33.757440476190474,
                        "periodic_rate": 55.7,
                        "relative_rate": 43.20952380952381,
                    },
                    {
                        "global_rate": 35.967261904761905,
                        "periodic_rate": 59.4,
                        "relative_rate": 43.945454545454545,
                    },
                    {
                        "global_rate": 38.36309523809524,
                        "periodic_rate": 64.4,
                        "relative_rate": 44.83478260869565,
                    },
                    {
                        "global_rate": 40.94122023809524,
                        "periodic_rate": 69.3,
                        "relative_rate": 45.85416666666667,
                    },
                    {
                        "global_rate": 43.47470238095238,
                        "periodic_rate": 68.10000000000001,
                        "relative_rate": 46.744,
                    },
                    {
                        "global_rate": 46.29092261904762,
                        "periodic_rate": 75.7,
                        "relative_rate": 47.857692307692304,
                    },
                    {
                        "global_rate": 48.98065476190476,
                        "periodic_rate": 82.1590909090909,
                        "relative_rate": 48.98065476190476,
                    },
                ],
                "title": "WIP0022 (defeated)",
                "active": False,
                "tapi_id": 7,
                "finished": True,
                "activated": False,
                "stop_time": 1678356045,
                "start_time": 1677146445,
                "stop_epoch": 1682000,
                "description": "Set a data request reward collateral ratio",
                "start_epoch": 1655120,
                "last_updated": 1695549179,
                "current_epoch": 2064069,
                "global_acceptance_rate": 48.98065476190476,
                "relative_acceptance_rate": 48.98065476190476,
            },
        ],
        [
            8,
            "WIP0023 (defeated)",
            "Burn slashed collateral",
            ["https://github.com/witnet/WIPs/blob/master/wip-0023.md"],
            None,
            1655120,
            1682000,
            4,
            None,
        ],
        [
            9,
            "WIP0024 (defeated)",
            "Improve the processing of numbers in oracle queries",
            ["https://github.com/witnet/WIPs/blob/master/wip-0024.md"],
            None,
            1655120,
            1682000,
            5,
            None,
        ],
        [
            10,
            "WIP0025 (defeated)",
            "Follow HTTP redirects in retrievals",
            ["https://github.com/witnet/WIPs/blob/master/wip-0025.md"],
            None,
            1655120,
            1682000,
            6,
            None,
        ],
        [
            11,
            "WIP0026 (defeated)",
            "Introduce a new EncodeReveal RADON error",
            ["https://github.com/witnet/WIPs/blob/master/wip-0026.md"],
            None,
            1655120,
            1682000,
            7,
            None,
        ],
        [
            12,
            "WIP0027 (defeated)",
            "Increase the age requirement for using transaction outputs as collateral",
            ["https://github.com/witnet/WIPs/blob/master/wip-0027.md"],
            None,
            1655120,
            1682000,
            8,
            None,
        ],
        [
            13,
            "WIP0022",
            "Set a data request reward collateral ratio",
            ["https://github.com/witnet/WIPs/blob/master/wip-0022.md"],
            1708901,
            1682000,
            1708880,
            3,
            {
                "bit": 3,
                "urls": ["https://github.com/witnet/WIPs/blob/master/wip-0022.md"],
                "rates": [
                    {
                        "global_rate": 3.0729166666666665,
                        "periodic_rate": 82.6,
                        "relative_rate": 82.6,
                    },
                    {
                        "global_rate": 6.045386904761905,
                        "periodic_rate": 79.9,
                        "relative_rate": 81.25,
                    },
                    {
                        "global_rate": 9.08110119047619,
                        "periodic_rate": 81.6,
                        "relative_rate": 81.36666666666666,
                    },
                    {
                        "global_rate": 12.001488095238095,
                        "periodic_rate": 78.5,
                        "relative_rate": 80.65,
                    },
                    {
                        "global_rate": 14.832589285714285,
                        "periodic_rate": 76.1,
                        "relative_rate": 79.74,
                    },
                    {
                        "global_rate": 17.83110119047619,
                        "periodic_rate": 80.60000000000001,
                        "relative_rate": 79.88333333333333,
                    },
                    {
                        "global_rate": 20.788690476190478,
                        "periodic_rate": 79.5,
                        "relative_rate": 79.82857142857142,
                    },
                    {
                        "global_rate": 23.783482142857142,
                        "periodic_rate": 80.5,
                        "relative_rate": 79.9125,
                    },
                    {
                        "global_rate": 26.6889880952381,
                        "periodic_rate": 78.10000000000001,
                        "relative_rate": 79.71111111111111,
                    },
                    {
                        "global_rate": 29.70982142857143,
                        "periodic_rate": 81.2,
                        "relative_rate": 79.86,
                    },
                    {
                        "global_rate": 32.641369047619044,
                        "periodic_rate": 78.8,
                        "relative_rate": 79.76363636363637,
                    },
                    {
                        "global_rate": 35.61383928571429,
                        "periodic_rate": 79.9,
                        "relative_rate": 79.77499999999999,
                    },
                    {
                        "global_rate": 38.64955357142857,
                        "periodic_rate": 81.6,
                        "relative_rate": 79.91538461538461,
                    },
                    {
                        "global_rate": 41.75595238095238,
                        "periodic_rate": 83.5,
                        "relative_rate": 80.17142857142858,
                    },
                    {
                        "global_rate": 44.851190476190474,
                        "periodic_rate": 83.2,
                        "relative_rate": 80.37333333333333,
                    },
                    {
                        "global_rate": 47.97247023809524,
                        "periodic_rate": 83.89999999999999,
                        "relative_rate": 80.59375,
                    },
                    {
                        "global_rate": 51.06026785714286,
                        "periodic_rate": 83.0,
                        "relative_rate": 80.73529411764706,
                    },
                    {
                        "global_rate": 54.055059523809526,
                        "periodic_rate": 80.5,
                        "relative_rate": 80.72222222222221,
                    },
                    {
                        "global_rate": 57.12797619047619,
                        "periodic_rate": 82.6,
                        "relative_rate": 80.82105263157895,
                    },
                    {
                        "global_rate": 60.25297619047619,
                        "periodic_rate": 84.0,
                        "relative_rate": 80.97999999999999,
                    },
                    {
                        "global_rate": 63.370535714285715,
                        "periodic_rate": 83.8,
                        "relative_rate": 81.11428571428571,
                    },
                    {
                        "global_rate": 66.45833333333333,
                        "periodic_rate": 83.0,
                        "relative_rate": 81.2,
                    },
                    {
                        "global_rate": 69.58333333333333,
                        "periodic_rate": 84.0,
                        "relative_rate": 81.32173913043478,
                    },
                    {
                        "global_rate": 72.85714285714285,
                        "periodic_rate": 88.0,
                        "relative_rate": 81.6,
                    },
                    {
                        "global_rate": 76.02306547619048,
                        "periodic_rate": 85.1,
                        "relative_rate": 81.74,
                    },
                    {
                        "global_rate": 79.21875,
                        "periodic_rate": 85.9,
                        "relative_rate": 81.89999999999999,
                    },
                    {
                        "global_rate": 82.14657738095238,
                        "periodic_rate": 89.43181818181817,
                        "relative_rate": 82.14657738095238,
                    },
                ],
                "title": "WIP0022",
                "active": False,
                "tapi_id": 13,
                "finished": True,
                "activated": True,
                "stop_time": 1679565645,
                "start_time": 1678356045,
                "stop_epoch": 1708880,
                "description": "Set a data request reward collateral ratio",
                "start_epoch": 1682000,
                "last_updated": 1695549180,
                "current_epoch": 2064069,
                "global_acceptance_rate": 82.14657738095238,
                "relative_acceptance_rate": 82.14657738095238,
            },
        ],
        [
            14,
            "WIP0023",
            "Burn slashed collateral",
            ["https://github.com/witnet/WIPs/blob/master/wip-0023.md"],
            1708901,
            1682000,
            1708880,
            4,
            None,
        ],
        [
            15,
            "WIP0024",
            "Improve the processing of numbers in oracle queries",
            ["https://github.com/witnet/WIPs/blob/master/wip-0024.md"],
            1708901,
            1682000,
            1708880,
            5,
            None,
        ],
        [
            16,
            "WIP0025",
            "Follow HTTP redirects in retrievals",
            ["https://github.com/witnet/WIPs/blob/master/wip-0025.md"],
            1708901,
            1682000,
            1708880,
            6,
            None,
        ],
        [
            17,
            "WIP0026",
            "Introduce a new EncodeReveal RADON error",
            ["https://github.com/witnet/WIPs/blob/master/wip-0026.md"],
            1708901,
            1682000,
            1708880,
            7,
            None,
        ],
        [
            18,
            "WIP0027",
            "Increase the age requirement for using transaction outputs as collateral",
            ["https://github.com/witnet/WIPs/blob/master/wip-0027.md"],
            1708901,
            1682000,
            1708880,
            8,
            None,
        ],
    ]
    sql = """
        INSERT INTO
            wips
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    connection = sqlite3.connect(database)
    cursor = connection.cursor()
    cursor.executemany(
        sql,
        [
            [
                wip[0],
                wip[1],
                wip[2],
                json.dumps(wip[3]),
                wip[4],
                wip[5],
                wip[6],
                wip[7],
                json.dumps(wip[8]) if wip[8] else None,
            ]
            for wip in wips
        ],
    )
    connection.commit()


def get_epoch_data(config, epochs):
    database = DatabaseManager(config["database"], custom_types=["utxo", "filter"])

    epoch_data = {}
    hashes_seen = set()

    epoch_params = "epoch=%s OR " * len(epochs)
    epoch_params = epoch_params[:-4]

    sql = f"""
        SELECT
            hash,
            type,
            epoch
        FROM
            hashes
        WHERE
            {epoch_params}
    """
    data = database.sql_return_all(sql, parameters=epochs)
    epoch_data["hash_data"] = []
    for d in data:
        if d[0].hex() not in hashes_seen:
            hashes_seen.add(d[0].hex())
            epoch_data["hash_data"].append([f"\\x{d[0].hex()}", d[1], d[2]])

    sql = f"""
        SELECT
            block_hash,
            value_transfer,
            data_request,
            commit,
            reveal,
            tally,
            dr_weight,
            vt_weight,
            block_weight,
            epoch,
            tapi_signals,
            confirmed,
            reverted
        FROM
            blocks
        WHERE
            {epoch_params}
    """
    data = database.sql_return_all(sql, parameters=epochs)
    epoch_data["block_data"] = [
        [
            f"\\x{d[0].hex()}",
            d[1],
            d[2],
            d[3],
            d[4],
            d[5],
            d[6],
            d[7],
            d[8],
            d[9],
            d[10],
            f"{d[11]}",
            f"{d[12]}",
        ]
        for d in data
    ]

    sql = f"""
        SELECT
            txn_hash,
            miner,
            output_addresses,
            output_values,
            epoch
        FROM
            mint_txns
        WHERE
            {epoch_params}
    """
    data = database.sql_return_all(sql, parameters=epochs)
    epoch_data["mint_data"] = [
        [
            f"\\x{d[0].hex()}",
            d[1],
            str(d[2]).replace("'", ""),
            str(d[3]),
            d[4],
        ]
        for d in data
    ]

    sql = f"""
        SELECT
            txn_hash,
            input_addresses,
            input_values,
            input_utxos,
            output_addresses,
            output_values,
            timelocks,
            weight,
            epoch
        FROM
            value_transfer_txns
         WHERE
            {epoch_params}
        """
    data = database.sql_return_all(sql, parameters=epochs)
    epoch_data["value_transfer_data"] = [
        [
            f"\\x{d[0].hex()}",
            str(d[1]).replace("'", ""),
            str(d[2]),
            str([f"\\x{u.transaction.hex()}:{u.idx}" for u in d[3]])
            .replace("'", "")
            .replace("\\\\", "\\"),
            str(d[4]).replace("'", ""),
            str(d[5]),
            str(d[6]),
            d[7],
            d[8],
        ]
        for d in data
    ]

    sql = f"""
        SELECT
            txn_hash,
            input_addresses,
            input_values,
            input_utxos,
            output_address,
            output_value,
            witnesses,
            witness_reward,
            collateral,
            consensus_percentage,
            commit_and_reveal_fee,
            weight,
            kinds,
            urls,
            headers,
            bodies,
            scripts,
            aggregate_filters,
            aggregate_reducer,
            tally_filters,
            tally_reducer,
            RAD_bytes_hash,
            DRO_bytes_hash,
            epoch
        FROM
            data_request_txns
        WHERE
            {epoch_params}
    """
    data = database.sql_return_all(
        sql, parameters=epochs, custom_types=["utxo", "filter"]
    )
    epoch_data["data_request_data"] = [
        [
            f"\\x{d[0].hex()}",
            str(d[1]).replace("'", ""),
            str(d[2]),
            str([f"\\x{u.transaction.hex()}:{u.idx}" for u in d[3]])
            .replace("'", "")
            .replace("\\\\", "\\"),
            d[4],
            d[5],
            d[6],
            d[7],
            d[8],
            d[9],
            d[10],
            d[11],
            str(d[12]).replace("{", "[").replace("}", "]"),
            str(d[13]).replace("'", ""),
            str(d[14]),
            str([f"\\x{h.hex()}" for h in d[15]])
            .replace("'", "")
            .replace("\\\\", "\\"),
            str([f"\\x{s.hex()}" for s in d[16]])
            .replace("'", "")
            .replace("\\\\", "\\"),
            str([f"filter({f.type}, \\x{f.args.hex()})" for f in d[17]])
            .replace("'", "")
            .replace("\\\\", "\\"),
            str(d[18]),
            str([f"filter({f.type}, \\x{f.args.hex()})" for f in d[19]])
            .replace("'", "")
            .replace("\\\\", "\\"),
            str(d[20]),
            f"\\x{d[21].hex()}",
            f"\\x{d[22].hex()}",
            d[23],
        ]
        for d in data
    ]

    # Also insert RAD and DRO bytes hashes into the hash table
    for d in sorted(data, key=lambda epoch: epoch[23], reverse=True):
        if d[21].hex() not in hashes_seen:
            hashes_seen.add(d[21].hex())
            epoch_data["hash_data"].append(
                [f"\\x{d[21].hex()}", "RAD_bytes_hash", d[23]]
            )
        if d[22].hex() not in hashes_seen:
            hashes_seen.add(d[22].hex())
            epoch_data["hash_data"].append(
                [f"\\x{d[22].hex()}", "DRO_bytes_hash", d[23]]
            )

    sql = f"""
        SELECT
            txn_hash,
            txn_address,
            input_values,
            input_utxos,
            output_value,
            data_request,
            epoch
        FROM
            commit_txns
        WHERE
            {epoch_params}
    """
    data = database.sql_return_all(sql, parameters=epochs)
    epoch_data["commit_data"] = [
        [
            f"\\x{d[0].hex()}",
            d[1],
            str(d[2]),
            str([f"\\x{u.transaction.hex()}:{u.idx}" for u in d[3]])
            .replace("'", "")
            .replace("\\\\", "\\"),
            d[4],
            f"\\x{d[5].hex()}",
            d[6],
        ]
        for d in data
    ]

    sql = f"""
        SELECT
            txn_hash,
            txn_address,
            data_request,
            result,
            success,
            epoch
        FROM
            reveal_txns
        WHERE
            {epoch_params}
    """
    data = database.sql_return_all(sql, parameters=epochs)
    epoch_data["reveal_data"] = [
        [
            f"\\x{d[0].hex()}",
            d[1],
            f"\\x{d[2].hex()}",
            f"\\x{d[3].hex()}",
            str(d[4]),
            d[5],
        ]
        for d in data
    ]

    sql = f"""
        SELECT
            txn_hash,
            output_addresses,
            output_values,
            data_request,
            error_addresses,
            liar_addresses,
            result,
            success,
            epoch
        FROM
            tally_txns
        WHERE
            {epoch_params}
    """
    data = database.sql_return_all(sql, parameters=epochs)
    epoch_data["tally_data"] = [
        [
            f"\\x{d[0].hex()}",
            str(d[1]).replace("'", ""),
            str(d[2]),
            f"\\x{d[3].hex()}",
            str(d[4]).replace("'", ""),
            str(d[5]).replace("'", ""),
            f"\\x{d[6].hex()}",
            str(d[7]),
            d[8],
        ]
        for d in data
    ]

    return epoch_data


def insert_epoch_data(database, epoch_data):
    connection = sqlite3.connect(database)
    cursor = connection.cursor()

    sql = """
        INSERT INTO
            hashes
        VALUES
            (?, ?, ?)
    """
    cursor.executemany(sql, epoch_data["hash_data"])

    sql = """
        INSERT INTO
            blocks
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(sql, epoch_data["block_data"])

    sql = """
        INSERT INTO
            mint_txns
        VALUES
            (?, ?, ?, ?, ?)
    """
    cursor.executemany(sql, epoch_data["mint_data"])

    sql = """
        INSERT INTO
            value_transfer_txns
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(sql, epoch_data["value_transfer_data"])

    sql = """
        INSERT INTO
            data_request_txns
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(sql, epoch_data["data_request_data"])

    sql = """
        INSERT INTO
            commit_txns
        VALUES
            (?, ?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(sql, epoch_data["commit_data"])

    sql = """
        INSERT INTO
            reveal_txns
        VALUES
            (?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(sql, epoch_data["reveal_data"])

    sql = """
        INSERT INTO
            tally_txns
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(sql, epoch_data["tally_data"])

    connection.commit()


def main():
    parser = argparse.ArgumentParser(
        prog="CreateMockupDatabase",
        description="Create mockup database for running tests",
    )
    parser.add_argument(
        "--config-file",
        type=str,
        default="explorer.toml",
        dest="config_file",
    )
    parser.add_argument(
        "--database",
        type=str,
        default="database.sqlite3",
        dest="database",
    )
    args = parser.parse_args()

    # fmt: off
    epochs = [
        753, 766, 30727, 31355, 31361, 48144, 135477, 135482, 135490, 135494, 135495,
        135500, 686551, 686556, 687204, 687209, 983037, 1004126, 1004130, 1059883,
        1059887, 1059888, 1791134, 1826734, 1859757, 1885263, 1926888, 1937617, 1059886,
        1955191, 1982909, 1998908, 1998909, 1998910, 1998911, 1998925, 1998926, 1998927,
        1998928, 1999122, 1999124, 1999125, 1999126, 2001244, 2001245, 2001246, 2001247,
        2001260, 2001261, 2001262, 2001263, 2002284, 2002285, 2002286, 2002287, 2002462,
        2002465, 2002482, 2002497, 2002517, 2002525, 2002529, 2002534, 2002537, 2002549,
        2002552, 2002557, 2002561, 2004897, 2004898, 2004899, 2004900, 2004938, 2004939,
        2004940, 2004941, 2005583, 2005584, 2005585, 2005586, 2005781, 2005782, 2005783,
        2005784, 2005793, 2011834, 2011835, 2011836, 2011837, 2011838, 2024094, 2024095,
        2024096, 2024097, 2024098, 2024099, 2024100
    ]
    # fmt: on

    config = toml.load(args.config_file)

    create_tables(args.database)

    epoch_data = get_epoch_data(config, epochs)
    insert_epoch_data(args.database, epoch_data)

    insert_address_data(args.database)

    insert_consensus_constants(args.database)

    insert_network_stats(args.database)

    insert_pending_transaction(args.database)

    insert_wips(args.database)


if __name__ == "__main__":
    main()
