import argparse
import json
import os
import sqlite3

import toml
import tqdm

from blockchain.config import BlockchainConfig
from blockchain.objects.wip import WIP
from blockchain.transactions.reveal import translate_reveal
from blockchain.transactions.tally import translate_tally
from node.witnet_node import WitnetNode
from util.address_generator import AddressGenerator
from util.data_transformer import bytes2hex
from util.protobuf_encoder import ProtobufEncoder


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
                stake INT NOT NULL,
                unstake INT NOT NULL,
                dr_weight INT NOT NULL,
                vt_weight INT NOT NULL,
                st_weight INT NOT NULL,
                ut_weight INT NOT NULL,
                block_weight INT NOT NULL,
                txns_fees INT NOT NULL,
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
        """CREATE TABLE IF NOT EXISTS stake_txns (
            txn_hash TEXT PRIMARY KEY,
            input_addresses TEXT NOT NULL,
            input_values INT NOT NULL,
            input_utxos TEXT NOT NULL,
            change_address TEXT,
            change_value INT,
            weight INT NOT NULL,
            validator TEXT NOT NULL,
            withdrawer TEXT NOT NULL,
            stake_value INT NOT NULL,
            epoch INT NOT NULL
        );""",
        """CREATE TABLE IF NOT EXISTS unstake_txns (
            txn_hash TEXT PRIMARY KEY,
            validator TEXT NOT NULL,
            withdrawer TEXT NOT NULL,
            unstake_value INT NOT NULL,
            fee INT NOT NULL,
            nonce INT NOT NULL,
            weight INT NOT NULL,
            epoch INT NOT NULL
        );""",
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
            CREATE TABLE IF NOT EXISTS stake_mempool (
                timestamp INT NOT NULL,
                fee TEXT NOT NULL,
                weight TEXT NOT NULL
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS unstake_mempool (
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
            "twit1najvm34rta4vnkpfax8kk0vhpntg5lgdz8wc33",
            "label 1",
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
            "twit1w9vaa7we6h8qyc3uawdwnp9n40602hdgsxkzf6",
            "label 2",
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
            "twit1z8p6qp2f5z6j2nfex3kme5qee0vurpvd8yn5hj",
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
            "twit1xshwjs5huexwfxkldvue7kc6cx230vuhtmme2s",
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
            "twit17ue5kphnvajes4y05s525e6y9hjr48tpxc3ruc",
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
            "twit1my5tgl0r3lsft38kz748zaa7z48dd9994aq895",
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
            "twit1d90hma685ghdw33c30svx9889sdckw7kq4j4uf",
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
            "twit1xc6002zplgwjdhgnrdxu9gpsg6d9czueeshnsz",
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
            "twit1azrj8h2mg6dnq7nem8cp7c2mqcs887djd4e2wh",
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
            "twit1j3eyct469cpkz63kxx7mhffhltv5q7v45mgda3",
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
            "twit1dvj7cshhvcqttua9afmlfkhk7vwvh0qwfjnwgc",
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
            "twit1v9emzryhvu9czp39tyaz76de056g9wdtk5cfcz",
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
            "twit1yr9807edzm4l4k7thmw7duufvmm4mkzgqfsnr6",
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
            "twit1ajwk8tcajkwuqq5984rt07km7w9etgrvn59kfa",
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
            "twit1pr02yrydlm0qd7jhtj0mp4355aj2g3gcy9smaq",
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
            "twit15wd25cpstddkvxvfdzydcsnwym3d4mvwhjn2u2",
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
            "twit1l3yps6rl2ct8tleh2v632mcwts5s4cuhrvzmmu",
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
            "twit15k0huw65x8wkq3p06dmqxf3a46cdhkuljculsm",
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
            "twit1nwgm7dz3m339h3uxhyflvr6yfutk7kvfy6auff",
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
            "twit1w2hxy8h86p43wx9g722mx8ygs0dsdqlut3n7gw",
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
            "twit16m7pxuajzs8jsgwqnukqk39v3qc8dxypqahqdj",
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
            "twit15rv5eypwq54u6u3xc2ckd7vfyj53nnddmrmryt",
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
            "twit133wnd4ueheeme4dxjjy052s73sjf8uslah70s4",
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
            "twit1k6vpqaajekgyx5whdp4ag7dp2h2627dcfjyngs",
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
            "twit18j3jsfn6y6lc43v4x5tn3lgk8vzlj37hx6esqy",
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
            "twit1aufqegyctt7h64eyyp2u6a68ultgkla5gnsxw8",
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
            "twit14ef2z0l3l9plkuuqvcsqnq2azc86c2v7udjfeg",
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
            "twit1dsxjndrkhpmgmd6nmt95nvmtqf667e5pmq2eqr",
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
            "twit1ewhsfsjz5gdern8kaz8x5yd9ph04lyg3ewxssc",
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
            "twit1g5n9m4s0lqa2am0edgfevnppu8y207yzjdhqag",
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
            "twit1r7gszcq5hu2xvxhpufgs56c0m47tuwtnl3qdpa",
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
            "twit10c45atl93vaudvpcmqtl8pqgt5lwzkk9r9mvrs",
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
            "twit1a82dxlj8cy3afpxna6kqgq2r9plraf9raqq35m",
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
            "twit1gzntj0duaqjgexcjnwhgww9f564l7edx0khl6y",
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
            "twit1ccm40u8d8z6ps28tkx6f6ruh340l77psgwhf95",
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
            "twit1m6xt7zh3km2u9xzceqzaepkl5nj5qr4ss8zfed",
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
            "twit1zxg8m7t6rqs0mpptcmmd8fr5hxe3hu7e849xr8",
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
            "twit1m3u3hhs9fvqgt5a8d0zm7w7mhsytgz53pyf9hz",
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
            "twit1xe6j5kf8a20xvhqjf7jsej06k45pxhu99u6dza",
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
            "twit1kzmck6qyy503lme89lj02jxc3nuj45wg4l8r4c",
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
            "twit1e49dg9me24esfmz8gkvhhszr7dvasc77mmq6nj",
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
            "twit1astv06rz2m4pg9gaq5l79k34sh9w0j5nj2xe6k",
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
            "twit19ejd5fgeypn9d8f6r8st2jkdx89e3vmp4eu3zv",
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
            "twit1m5wsdsv7ard8va55tut5jk06nrx2gmxhgphqhl",
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
            "twit1ulra63922mnls2rvktaqh4hhrruama9eqjnkc8",
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
            "twit1n4gsyx57khmancx0cmzg2q975vvqayq55rlg0y",
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
            "twit1fz0wz7dcpanadsdlke75zgjlhqxuv2vusvte8f",
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
            "twit140uxs8r7asudphc7zzqc2ze8fk5ytkd6auxjf4",
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
            "twit1mts8na78rrdg9j405uh5t9lklkpsdfm8tfze95",
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
            "twit1k58yzyjse9frx5yx7xfg0g43mnuv5xw0y4venf",
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
            "twit1epgja4fpkyupwlxjawnth39usajywqdh9cxxlx",
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
            "twit1gue84sf650hns8qsq4kn2x7awy29mrhdexdste",
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
            "[twit1f0am8c97q2ygkz3q6jyd2x29s8zaxqlxcqltxx,twit1pc8jzqph4t0md02e6fgwgsw26yll20p98c3pgh,twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp,twit1najvm34rta4vnkpfax8kk0vhpntg5lgdz8wc33]",
        ],
        ["checkpoint_zero_timestamp", 1738180800, None],
        ["checkpoints_period", 45, None],
        ["collateral_age", 1000, None],
        ["collateral_minimum", 100000000, None],
        ["epochs_with_minimum_difficulty", 2000, None],
        ["extra_rounds", 3, None],
        [
            "genesis_hash",
            None,
            "[5aaafb853ce897c1431ee5babc5533650d37c4fab44045bde9c74a3fff8c080e]",
        ],
        ["halving_period", 3500000, None],
        ["initial_block_reward", 250000000000, None],
        ["max_dr_weight", 80000, None],
        ["max_vt_weight", 20000, None],
        ["minimum_difficulty", 0, None],
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
        ["wit2_checkpoints_period", 20, None],
        ["wit2_minimum_total_stake_nanowits", 30_000_000_000_000_000, None],
        ["wit2_activation_delay_epochs", 101, None],
        ["wit2_maximum_stake_block_weight", 10_000_000, None],
        ["wit2_maximum_unstake_block_weight", 5_000, None],
        ["wit2_unstaking_delay_seconds", 3_600, None],
        ["wit2_min_stake_nanowits", 10_000_000_000_000, None],
        ["wit2_max_stake_nanowits", 10_000_000_000_000_000, None],
        ["wit2_block_reward", 50_000_000_000, None],
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
                [1740387825, 110000, 110009, 10],
                [1740187825, 100000, 100019, 20],
                [1739789225, 80070, 80099, 30],
            ],
        ],
        ["epoch", None, None, 115910],
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
            100000,
            101000,
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
            101000,
            102000,
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
            100000,
            101000,
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
            101000,
            102000,
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
            "data_requests",
            100000,
            101000,
            [
                1066,
                1057,
                3909,
                0,
                5,
                {"2": 5, "10": 813, "100": 248},
                {"1": 248, "500000": 5, "100000": 813},
                {"100000000": 5, "2500000000": 248, "5000000000": 813},
            ],
        ],
        [
            "data_requests",
            101000,
            102000,
            [
                1079,
                1073,
                4110,
                0,
                5,
                {"2": 5, "10": 826, "100": 248},
                {"1": 248, "500000": 5, "100000": 826},
                {"100000000": 5, "2500000000": 248, "5000000000": 826},
            ],
        ],
        ["lie_rate", 100000, 101000, [32940, 367, 310, 239]],
        ["lie_rate", 101000, 102000, [33070, 271, 321, 220]],
        ["lie_rate", 2023000, 2024000, [29702, 305, 544, 98]],
        ["lie_rate", 2024000, 2025000, [32242, 654, 594, 135]],
        ["burn_rate", 100000, 101000, [0, 0]],
        ["burn_rate", 101000, 102000, [0, 0]],
        ["burn_rate", 2023000, 2024000, [0, 543000000000]],
        ["burn_rate", 2024000, 2025000, [0, 550500000000]],
        ["value_transfers", 100000, 101000, [312]],
        ["value_transfers", 101000, 102000, [266]],
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
        [1740752780, [166666670], [2225]],
        [1740752790, [166666670], [2225]],
        [1740752800, [166666670], [2225]],
        [1740752810, [166666670], [2225]],
        [1740752820, [166666670], [2225]],
        [1740752830, [166666670], [2225]],
        [1740753050, [166666670], [2520]],
        [1740758250, [166666670], [2520]],
        [1740758260, [166666670], [2690]],
        [1740758270, [166666670], [2690]],
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
        [1740752780, [1000], [853]],
        [1740752790, [1000], [853]],
        [1740752800, [1000], [853]],
        [1740752810, [1000, 1000], [853, 853]],
        [1740752820, [1000], [853]],
        [1740752830, [1000], [853]],
        [1740753050, [1000], [626]],
        [1740758250, [1000], [18133]],
        [1740758260, [1000], [18133]],
        [1740758270, [1000], [18133]],
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

    pending_stakes = [
        [1740752780, [1000], [939]],
        [1740752790, [1000], [939]],
        [1740752800, [1000], [9983]],
        [1740752810, [1000, 2000], [407, 274]],
        [1740752820, [1000], [274]],
        [1740752830, [1000], [274]],
        [1740753050, [1000], [274]],
        [1740758250, [1000], [2136]],
        [1740758260, [1000], [1737]],
        [1740758270, [1000], [2402]],
    ]

    sql = """
        INSERT INTO
            stake_mempool
        VALUES
            (?, ?, ?)
    """
    cursor.executemany(
        sql,
        [[pvt[0], json.dumps(pvt[1]), json.dumps(pvt[2])] for pvt in pending_stakes],
    )

    pending_unstakes = [
        [1740752780, [1000], [153]],
        [1740752790, [1000], [153]],
        [1740752800, [1000], [153]],
        [1740752810, [1000, 2000], [153, 153]],
        [1740752820, [1000], [153]],
        [1740752830, [1000], [153]],
        [1740753050, [1000], [153]],
        [1740758250, [1000], [153]],
        [1740758260, [1000], [153]],
        [1740758270, [1000], [153]],
    ]

    sql = """
        INSERT INTO
            unstake_mempool
        VALUES
            (?, ?, ?)
    """
    cursor.executemany(
        sql,
        [[pvt[0], json.dumps(pvt[1]), json.dumps(pvt[2])] for pvt in pending_unstakes],
    )

    connection.commit()


def insert_wips(database):
    wips = [
        [
            1,
            "WIP0008",
            "Limit data request concurrency",
            ["https://github.com/witnet/WIPs/blob/master/wip-0008.md"],
            0,
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
            0,
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
            0,
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
            0,
            0,
            0,
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
            0,
            0,
            0,
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
            0,
            0,
            0,
            2,
            None,
        ],
        [
            7,
            "WIP0022 (defeated)",
            "Set a data request reward collateral ratio",
            ["https://github.com/witnet/WIPs/blob/master/wip-0022.md"],
            None,
            0,
            0,
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
                        "periodic_rate": 68.1000000000001,
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
            0,
            0,
            4,
            None,
        ],
        [
            9,
            "WIP0024 (defeated)",
            "Improve the processing of numbers in oracle queries",
            ["https://github.com/witnet/WIPs/blob/master/wip-0024.md"],
            None,
            0,
            0,
            5,
            None,
        ],
        [
            10,
            "WIP0025 (defeated)",
            "Follow HTTP redirects in retrievals",
            ["https://github.com/witnet/WIPs/blob/master/wip-0025.md"],
            None,
            0,
            0,
            6,
            None,
        ],
        [
            11,
            "WIP0026 (defeated)",
            "Introduce a new EncodeReveal RADON error",
            ["https://github.com/witnet/WIPs/blob/master/wip-0026.md"],
            None,
            0,
            0,
            7,
            None,
        ],
        [
            12,
            "WIP0027 (defeated)",
            "Increase the age requirement for using transaction outputs as collateral",
            ["https://github.com/witnet/WIPs/blob/master/wip-0027.md"],
            None,
            0,
            0,
            8,
            None,
        ],
        [
            13,
            "WIP0022",
            "Set a data request reward collateral ratio",
            ["https://github.com/witnet/WIPs/blob/master/wip-0022.md"],
            0,
            0,
            0,
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
                        "periodic_rate": 78.1000000000001,
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
            0,
            0,
            0,
            4,
            None,
        ],
        [
            15,
            "WIP0024",
            "Improve the processing of numbers in oracle queries",
            ["https://github.com/witnet/WIPs/blob/master/wip-0024.md"],
            0,
            0,
            0,
            5,
            None,
        ],
        [
            16,
            "WIP0025",
            "Follow HTTP redirects in retrievals",
            ["https://github.com/witnet/WIPs/blob/master/wip-0025.md"],
            0,
            0,
            0,
            6,
            None,
        ],
        [
            17,
            "WIP0026",
            "Introduce a new EncodeReveal RADON error",
            ["https://github.com/witnet/WIPs/blob/master/wip-0026.md"],
            0,
            0,
            0,
            7,
            None,
        ],
        [
            18,
            "WIP0027",
            "Increase the age requirement for using transaction outputs as collateral",
            ["https://github.com/witnet/WIPs/blob/master/wip-0027.md"],
            0,
            0,
            0,
            8,
            None,
        ],
        [
            19,
            "WIP0028",
            "Enable upgrade to wit/2",
            ["https://github.com/witnet/WIPs/blob/master/wip-0028.md"],
            101,
            0,
            80,
            9,
            None,
        ],
        [
            20,
            "wit/2",
            "Activate wit/2",
            ["https://github.com/witnet/WIPs/blob/master/wip-0028.md"],
            281,
            101,
            181,
            -1,
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


def get_input_addresses(address_generator, signatures):
    return [
        address_generator.signature_to_address(
            signature["public_key"]["compressed"],
            signature["public_key"]["bytes"],
        )
        for signature in signatures
    ]


def get_input_values(witnet_node, requested_txns, output_pointers):
    input_values = []

    for txn_input in output_pointers:
        txn_hash = txn_input["output_pointer"].split(":")[0]
        txn_idx = int(txn_input["output_pointer"].split(":")[1])

        if txn_hash in requested_txns:
            input_txn = requested_txns[txn_hash]
        else:
            transaction = witnet_node.get_transaction(txn_hash)
            input_txn = transaction["result"]["transaction"]
            requested_txns[txn_hash] = input_txn

        txn_type = list(input_txn.keys())[0]
        if txn_type in ("Tally", "Mint"):
            outputs = input_txn[txn_type]["outputs"]
            # Append the correct output to the list of input_values
            input_values.append(outputs[txn_idx]["value"])
        elif txn_type in (
            "DataRequest",
            "Commit",
            "ValueTransfer",
        ):
            outputs = input_txn[txn_type]["body"]["outputs"]
            # Append the correct output to the list of input_values
            input_values.append(outputs[txn_idx]["value"])
        elif txn_type == "Stake":
            output = input_txn[txn_type]["body"]["change"]
            # Append the correct output to the list of input_values
            input_values.append(output["value"])
        elif txn_type == "Unstake":
            output = input_txn[txn_type]["body"]["withdrawal"]
            # Append the correct output to the list of input_values
            input_values.append(output["value"])

    return input_values


def calculate_block_fees(witnet_node, requested_txns, block):
    block_fees = 0

    for value_transfer in block["txns"]["value_transfer_txns"]:
        vt_body = value_transfer["body"]
        input_values = get_input_values(
            witnet_node,
            requested_txns,
            vt_body["inputs"],
        )
        block_fees += sum(input_values)
        block_fees -= sum([output["value"] for output in vt_body["outputs"]])

    for data_request in block["txns"]["data_request_txns"]:
        dr_output = data_request["body"]["dr_output"]
        witnesses = dr_output["witnesses"]
        witness_reward = dr_output["witness_reward"]
        commit_and_reveal_fee = dr_output["commit_and_reveal_fee"]
        dro_fee = witnesses * (witness_reward + 2 * commit_and_reveal_fee)

        input_values = get_input_values(
            witnet_node,
            requested_txns,
            data_request["body"]["inputs"],
        )
        if len(data_request["body"]["outputs"]) > 0:
            output_value = data_request["body"]["outputs"][0]["value"]
        else:
            output_value = 0

        miner_fee = sum(input_values) - output_value - dro_fee

        block_fees += miner_fee

    for commit in block["txns"]["commit_txns"]:
        dr_pointer = commit["body"]["dr_pointer"]
        if dr_pointer in requested_txns:
            data_request = requested_txns[dr_pointer]
        else:
            transaction = witnet_node.get_transaction(dr_pointer)
            data_request = transaction["result"]["transaction"]
            requested_txns[dr_pointer] = data_request
        dr_output = data_request["DataRequest"]["body"]["dr_output"]
        block_fees += dr_output["commit_and_reveal_fee"]

    for reveal in block["txns"]["reveal_txns"]:
        dr_pointer = reveal["body"]["dr_pointer"]
        if dr_pointer in requested_txns:
            data_request = requested_txns[dr_pointer]
        else:
            transaction = witnet_node.get_transaction(reveal["body"]["dr_pointer"])
            data_request = transaction["result"]["transaction"]
            requested_txns[dr_pointer] = data_request
        dr_output = data_request["DataRequest"]["body"]["dr_output"]
        block_fees += dr_output["commit_and_reveal_fee"]

    for stake in block["txns"]["stake_txns"]:
        st_body = stake["body"]
        input_values = get_input_values(
            witnet_node,
            requested_txns,
            st_body["inputs"],
        )
        block_fees += sum(input_values)
        if st_body["change"]:
            block_fees -= st_body["change"]["value"]
        block_fees -= st_body["output"]["value"]

    for unstake in block["txns"]["unstake_txns"]:
        block_fees += unstake["body"]["fee"]

    return block_fees


def parse_mint(address_generator, txn_hash, block_sig, mint_txn, epoch):
    miner = get_input_addresses(address_generator, [block_sig])[0]

    return [
        f"\\x{txn_hash}",
        miner,
        str([",".join([output["pkh"] for output in mint_txn["outputs"]])]).replace(
            "'", ""
        ),
        str(
            [",".join([str(output["value"]) for output in mint_txn["outputs"]])]
        ).replace("'", ""),
        epoch,
    ]


def parse_value_transfer(
    address_generator,
    witnet_node,
    requested_txns,
    txn_hash,
    value_transfer,
    txn_weight,
    epoch,
):
    input_values = get_input_values(
        witnet_node,
        requested_txns,
        value_transfer["body"]["inputs"],
    )

    return [
        f"\\x{txn_hash}",
        str(
            get_input_addresses(address_generator, value_transfer["signatures"])
        ).replace("'", ""),
        str(input_values),
        str(
            [
                f"\\x{txn_input['output_pointer']}"
                for txn_input in value_transfer["body"]["inputs"]
            ]
        )
        .replace("'", "")
        .replace("\\\\", "\\"),
        str([output["pkh"] for output in value_transfer["body"]["outputs"]]).replace(
            "'", ""
        ),
        str([output["value"] for output in value_transfer["body"]["outputs"]]),
        str([output["time_lock"] for output in value_transfer["body"]["outputs"]]),
        txn_weight,
        epoch,
    ]


def parse_data_request(
    address_generator,
    witnet_node,
    requested_txns,
    txn_hash,
    data_request,
    txn_weight,
    RAD_hash,
    DRO_hash,
    epoch,
):
    dr_outputs = data_request["body"]["outputs"]
    dr_output = data_request["body"]["dr_output"]
    rad_request = dr_output["data_request"]

    input_addresses = get_input_addresses(address_generator, data_request["signatures"])
    input_values = get_input_values(
        witnet_node,
        requested_txns,
        data_request["body"]["inputs"],
    )

    return [
        f"\\x{txn_hash}",
        str(input_addresses).replace("'", ""),
        str(input_values),
        str(
            [
                f"\\x{txn_input['output_pointer']}"
                for txn_input in data_request["body"]["inputs"]
            ]
        )
        .replace("'", "")
        .replace("\\\\", "\\"),
        dr_outputs[0]["pkh"] if len(dr_outputs) > 0 else "",
        dr_outputs[0]["value"] if len(dr_outputs) > 0 else 0,
        dr_output["witnesses"],
        dr_output["witness_reward"],
        dr_output["collateral"],
        dr_output["min_consensus_percentage"],
        dr_output["commit_and_reveal_fee"],
        txn_weight,
        "{"
        + ",".join([retrieve["kind"] for retrieve in rad_request["retrieve"]])
        + "}",
        str(
            [
                ",".join(
                    [
                        retrieve["url"] if "url" in retrieve else "'"
                        for retrieve in rad_request["retrieve"]
                    ]
                )
            ]
        ).replace("'", ""),
        str(
            [
                ",".join(
                    [
                        (
                            str(retrieve["header"]).replace("'", "")
                            if "header" in retrieve
                            else str([""]).replace("'", "")
                        )
                        for retrieve in rad_request["retrieve"]
                    ]
                )
            ]
        ).replace("'", ""),
        str(
            [
                ",".join(
                    [
                        (
                            f"\\x{bytes2hex(retrieve['body'])}"
                            if "body" in retrieve
                            else "\\x"
                        )
                        for retrieve in rad_request["retrieve"]
                    ]
                )
            ]
        )
        .replace("'", "")
        .replace("\\\\", "\\"),
        str(
            [
                ",".join(
                    [
                        f"\\x{bytes2hex(retrieve['script'])}"
                        for retrieve in rad_request["retrieve"]
                    ]
                )
            ]
        )
        .replace("'", "")
        .replace("\\\\", "\\"),
        str(
            [
                f"filter({aggregate_filter['op']}, \\x{bytes2hex(aggregate_filter['args'])})"
                for aggregate_filter in rad_request["aggregate"]["filters"]
            ]
        )
        .replace("'", "")
        .replace("\\\\", "\\"),
        str([rad_request["aggregate"]["reducer"]]),
        str(
            [
                f"filter({tally_filter['op']}, \\x{bytes2hex(tally_filter['args'])})"
                for tally_filter in rad_request["tally"]["filters"]
            ]
        )
        .replace("'", "")
        .replace("\\\\", "\\"),
        str([rad_request["tally"]["reducer"]]),
        f"\\x{RAD_hash}",
        f"\\x{DRO_hash}",
        epoch,
    ]


def parse_commit(
    address_generator,
    witnet_node,
    requested_txns,
    txn_hash,
    commit,
    epoch,
):
    input_values = get_input_values(
        witnet_node,
        requested_txns,
        commit["body"]["collateral"],
    )

    return [
        f"\\x{txn_hash}",
        get_input_addresses(address_generator, commit["signatures"])[0],
        str(input_values),
        str(
            [
                f"\\x{txn_input['output_pointer']}"
                for txn_input in commit["body"]["collateral"]
            ]
        )
        .replace("'", "")
        .replace("\\\\", "\\"),
        (
            commit["body"]["outputs"][0]["value"]
            if len(commit["body"]["outputs"]) > 0
            else 0
        ),
        f"\\x{commit['body']['dr_pointer']}",
        epoch,
    ]


def parse_reveal(address_generator, txn_hash, reveal, epoch):
    success, _ = translate_reveal(txn_hash, reveal["body"]["reveal"])

    return [
        f"\\x{txn_hash}",
        get_input_addresses(address_generator, reveal["signatures"])[0],
        f"\\x{reveal['body']['dr_pointer']}",
        f"\\x{bytes2hex(reveal['body']['reveal'])}",
        str(success),
        epoch,
    ]


def parse_tally(txn_hash, tally, epoch):
    success, _ = translate_tally(txn_hash, tally["tally"])

    return [
        f"\\x{txn_hash}",
        str([output["pkh"] for output in tally["outputs"]]).replace("'", ""),
        str([output["value"] for output in tally["outputs"]]),
        f"\\x{tally['dr_pointer']}",
        str(tally["error_committers"]),
        str(list(set(tally["out_of_consensus"]) - set(tally["error_committers"]))),
        f"\\x{bytes2hex(tally['tally'])}",
        str(success),
        epoch,
    ]


def parse_stake(
    address_generator,
    witnet_node,
    requested_txns,
    txn_hash,
    stake,
    weight,
    epoch,
):
    input_addresses = get_input_addresses(address_generator, stake["signatures"])

    return [
        f"\\x{txn_hash}",
        str(input_addresses).replace("'", ""),
        str(get_input_values(witnet_node, requested_txns, stake["body"]["inputs"])),
        str(
            [
                f"\\x{txn_input['output_pointer']}"
                for txn_input in stake["body"]["inputs"]
            ]
        )
        .replace("'", "")
        .replace("\\\\", "\\"),
        (stake["body"]["change"]["pkh"] if stake["body"]["change"] else None),
        (stake["body"]["change"]["value"] if stake["body"]["change"] else None),
        weight,
        stake["body"]["output"]["key"]["validator"],
        stake["body"]["output"]["key"]["withdrawer"],
        stake["body"]["output"]["value"],
        epoch,
    ]


def parse_unstake(txn_hash, unstake, weight, epoch):
    return [
        f"\\x{txn_hash}",
        unstake["body"]["operator"],
        unstake["body"]["withdrawal"]["pkh"],
        unstake["body"]["withdrawal"]["value"],
        unstake["body"]["fee"],
        unstake["body"]["nonce"],
        weight,
        epoch,
    ]


def get_block_data_from_node(blocks, requested_txns):
    address_generator = AddressGenerator("twit")
    witnet_node = WitnetNode()

    hashes_seen = set()
    block_data = {
        "hash_data": [],
        "block_data": [],
        "mint_data": [],
        "value_transfer_data": [],
        "data_request_data": [],
        "commit_data": [],
        "reveal_data": [],
        "tally_data": [],
        "stake_data": [],
        "unstake_data": [],
    }
    input_txns = set()

    for block_hash in tqdm.tqdm(
        blocks,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
    ):
        block = witnet_node.get_block(block_hash)["result"]

        epoch = block["block_header"]["beacon"]["checkpoint"]
        txns = block["txns"]
        txns_hashes = block["txns_hashes"]
        txns_weights = block["txns_weights"]

        dr_weight = block["dr_weight"]
        vt_weight = block["vt_weight"]
        st_weight = block["st_weight"] if "st_weight" in block else 0
        ut_weight = block["ut_weight"] if "ut_weight" in block else 0

        block_fees = calculate_block_fees(witnet_node, requested_txns, block)

        # Add block data
        block_data["hash_data"].append([f"\\x{block_hash}", "block", epoch])
        block_data["block_data"].append(
            [
                f"\\x{block_hash}",
                len(txns_hashes["value_transfer"]),
                len(txns_hashes["data_request"]),
                len(txns_hashes["commit"]),
                len(txns_hashes["reveal"]),
                len(txns_hashes["tally"]),
                len(txns_hashes["stake"]) if "stake" in txns_hashes else 0,
                len(txns_hashes["unstake"]) if "unstake" in txns_hashes else 0,
                dr_weight,
                vt_weight,
                st_weight,
                ut_weight,
                dr_weight + vt_weight + st_weight + ut_weight,
                block_fees,
                epoch,
                block["block_header"]["signals"],
                "True" if block["confirmed"] else "False",
                "False" if block["confirmed"] else "True",
            ]
        )

        # Process mint transaction
        mint_hash = txns_hashes["mint"]
        hashes_seen.update([mint_hash])
        block_data["hash_data"].append([f"\\x{mint_hash}", "mint_txn", epoch])
        block_data["mint_data"].append(
            parse_mint(
                address_generator,
                mint_hash,
                block["block_sig"],
                txns["mint"],
                epoch,
            )
        )

        # Process value transfer transactions
        hashes_seen.update(txns_hashes["value_transfer"])
        for txn_hash, value_transfer, txn_weight in zip(
            txns_hashes["value_transfer"],
            txns["value_transfer_txns"],
            txns_weights["value_transfer"],
        ):
            block_data["hash_data"].append(
                [f"\\x{txn_hash}", "value_transfer_txn", epoch]
            )

            for txn_input in value_transfer["body"]["inputs"]:
                input_txns.add(txn_input["output_pointer"].split(":")[0])

            block_data["value_transfer_data"].append(
                parse_value_transfer(
                    address_generator,
                    witnet_node,
                    requested_txns,
                    txn_hash,
                    value_transfer,
                    txn_weight,
                    epoch,
                )
            )

        # Process data request transactions
        protobuf_encoder = ProtobufEncoder(WIP(mockup=True))
        hashes_seen.update(txns_hashes["data_request"])
        for txn_hash, data_request, txn_weight in zip(
            txns_hashes["data_request"],
            block["txns"]["data_request_txns"],
            txns_weights["data_request"],
        ):
            block_data["hash_data"].append(
                [f"\\x{txn_hash}", "data_request_txn", epoch]
            )

            dr_body = data_request["body"]

            for txn_input in dr_body["inputs"]:
                input_txns.add(txn_input["output_pointer"].split(":")[0])

            protobuf_encoder.set_transaction(data_request)
            RAD_hash, _ = protobuf_encoder.get_RAD_bytecode(epoch)
            DRO_hash, _ = protobuf_encoder.get_DRO_bytecode(epoch)

            if RAD_hash not in hashes_seen:
                hashes_seen.add(RAD_hash)
                block_data["hash_data"].append(
                    [f"\\x{RAD_hash}", "RAD_bytes_hash", epoch]
                )
            if DRO_hash not in hashes_seen:
                hashes_seen.add(DRO_hash)
                block_data["hash_data"].append(
                    [f"\\x{DRO_hash}", "DRO_bytes_hash", epoch]
                )

            block_data["data_request_data"].append(
                parse_data_request(
                    address_generator,
                    witnet_node,
                    requested_txns,
                    txn_hash,
                    data_request,
                    txn_weight,
                    RAD_hash,
                    DRO_hash,
                    epoch,
                )
            )

        # Process commit transactions
        hashes_seen.update(txns_hashes["commit"])
        for txn_hash, commit in zip(
            txns_hashes["commit"], block["txns"]["commit_txns"]
        ):
            block_data["hash_data"].append([f"\\x{txn_hash}", "commit_txn", epoch])

            for txn_input in commit["body"]["collateral"]:
                input_txns.add(txn_input["output_pointer"].split(":")[0])

            block_data["commit_data"].append(
                parse_commit(
                    address_generator,
                    witnet_node,
                    requested_txns,
                    txn_hash,
                    commit,
                    epoch,
                )
            )

        # Process reveal transactions
        hashes_seen.update(txns_hashes["reveal"])
        for txn_hash, reveal in zip(
            txns_hashes["reveal"], block["txns"]["reveal_txns"]
        ):
            block_data["hash_data"].append([f"\\x{txn_hash}", "reveal_txn", epoch])
            block_data["reveal_data"].append(
                parse_reveal(
                    address_generator,
                    txn_hash,
                    reveal,
                    epoch,
                )
            )

        # Process tally transactions
        hashes_seen.update(txns_hashes["tally"])
        for txn_hash, tally in zip(txns_hashes["tally"], block["txns"]["tally_txns"]):
            block_data["hash_data"].append([f"\\x{txn_hash}", "tally_txn", epoch])
            block_data["tally_data"].append(parse_tally(txn_hash, tally, epoch))

        # Process stake transactions
        if "stake" in txns_hashes:
            hashes_seen.update(txns_hashes["stake"])
            for txn_hash, stake, txn_weight in zip(
                txns_hashes["stake"],
                block["txns"]["stake_txns"],
                txns_weights["stake"],
            ):
                block_data["hash_data"].append([f"\\x{txn_hash}", "stake_txn", epoch])

                for txn_input in stake["body"]["inputs"]:
                    input_txns.add(txn_input["output_pointer"].split(":")[0])

                block_data["stake_data"].append(
                    parse_stake(
                        address_generator,
                        witnet_node,
                        requested_txns,
                        txn_hash,
                        stake,
                        txn_weight,
                        epoch,
                    )
                )

        # Process unstake transactions
        if "unstake" in txns_hashes:
            hashes_seen.update(txns_hashes["unstake"])
            for txn_hash, unstake, weight in zip(
                txns_hashes["unstake"],
                block["txns"]["unstake_txns"],
                txns_weights["unstake"],
            ):
                block_data["hash_data"].append([f"\\x{txn_hash}", "unstake_txn", epoch])
                block_data["unstake_data"].append(
                    parse_unstake(
                        txn_hash,
                        unstake,
                        weight,
                        epoch,
                    )
                )

    # Filter out input transactions we already saw in one of the processed blocks
    input_txns = list(input_txns)
    for txns in block_data.values():
        for txn in txns:
            if txn[0][2:] in input_txns:
                input_txns.remove(txn[0][2:])

    return block_data, input_txns


def get_input_transactions(transactions, requested_txns):
    address_generator = AddressGenerator("twit")
    protobuf_encoder = ProtobufEncoder(WIP(mockup=True))
    witnet_node = WitnetNode()

    block_data = {
        "hash_data": [],
        "block_data": [],
        "mint_data": [],
        "value_transfer_data": [],
        "data_request_data": [],
        "commit_data": [],
        "reveal_data": [],
        "tally_data": [],
        "stake_data": [],
        "unstake_data": [],
    }

    blocks_processed = set()
    for txn_hash in tqdm.tqdm(
        transactions,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
    ):
        input_txn = witnet_node.get_transaction(txn_hash)["result"]
        block_hash = input_txn["blockHash"]
        epoch = input_txn["blockEpoch"]

        # Add block metadata (if necessary)
        if block_hash not in blocks_processed:
            block = witnet_node.get_block(block_hash)["result"]
            txns_hashes = block["txns_hashes"]

            dr_weight = block["dr_weight"]
            vt_weight = block["vt_weight"]
            st_weight = block["st_weight"] if "st_weight" in block else 0
            ut_weight = block["ut_weight"] if "ut_weight" in block else 0

            block_fees = calculate_block_fees(witnet_node, requested_txns, block)

            block_data["block_data"].append(
                [
                    f"\\x{block_hash}",
                    len(txns_hashes["value_transfer"]),
                    len(txns_hashes["data_request"]),
                    len(txns_hashes["commit"]),
                    len(txns_hashes["reveal"]),
                    len(txns_hashes["tally"]),
                    len(txns_hashes["stake"]) if "stake" in txns_hashes else 0,
                    len(txns_hashes["unstake"]) if "unstake" in txns_hashes else 0,
                    dr_weight,
                    vt_weight,
                    st_weight,
                    ut_weight,
                    dr_weight + vt_weight + st_weight + ut_weight,
                    block_fees,
                    epoch,
                    block["block_header"]["signals"],
                    "True" if block["confirmed"] else "False",
                    "False" if block["confirmed"] else "True",
                ]
            )
            blocks_processed.add(block_hash)

            # Process mint transaction
            mint_hash = txns_hashes["mint"]
            block_data["hash_data"].append([f"\\x{mint_hash}", "mint_txn", epoch])
            block_data["mint_data"].append(
                parse_mint(
                    address_generator,
                    mint_hash,
                    block["block_sig"],
                    block["txns"]["mint"],
                    epoch,
                )
            )

        txn_type = list(input_txn["transaction"].keys())[0]
        if txn_type == "ValueTransfer":
            block_data["hash_data"].append(
                [f"\\x{txn_hash}", "value_transfer_txn", epoch]
            )

            value_transfer = input_txn["transaction"]["ValueTransfer"]

            block_data["value_transfer_data"].append(
                parse_value_transfer(
                    address_generator,
                    witnet_node,
                    requested_txns,
                    txn_hash,
                    value_transfer,
                    input_txn["weight"],
                    epoch,
                )
            )
        elif txn_type == "DataRequest":
            block_data["hash_data"].append(
                [f"\\x{txn_hash}", "data_request_txn", epoch]
            )

            data_request = input_txn["transaction"]["DataRequest"]

            protobuf_encoder.set_transaction(data_request)
            RAD_hash, _ = protobuf_encoder.get_RAD_bytecode(epoch)
            DRO_hash, _ = protobuf_encoder.get_DRO_bytecode(epoch)

            block_data["data_request_data"].append(
                parse_data_request(
                    address_generator,
                    witnet_node,
                    requested_txns,
                    txn_hash,
                    data_request,
                    input_txn["weight"],
                    RAD_hash,
                    DRO_hash,
                    epoch,
                )
            )
        elif txn_type == "Commit":
            block_data["hash_data"].append([f"\\x{txn_hash}", "commit_txn", epoch])

            commit = input_txn["transaction"]["Commit"]

            block_data["commit_data"].append(
                parse_commit(
                    address_generator,
                    witnet_node,
                    requested_txns,
                    txn_hash,
                    commit,
                    epoch,
                )
            )
        elif txn_type == "Tally":
            block_data["hash_data"].append([f"\\x{txn_hash}", "tally_txn", epoch])

            tally = input_txn["transaction"]["Tally"]

            block_data["tally_data"].append(parse_tally(txn_hash, tally, epoch))
        elif txn_type == "Stake":
            block_data["hash_data"].append([f"\\x{txn_hash}", "stake_txn", epoch])

            stake = input_txn["transaction"]["Stake"]

            block_data["stake_data"].append(
                parse_stake(
                    address_generator,
                    witnet_node,
                    requested_txns,
                    txn_hash,
                    stake,
                    input_txn["weight"],
                    epoch,
                )
            )
        elif txn_type == "Unstake":
            block_data["hash_data"].append([f"\\x{txn_hash}", "unstake_txn", epoch])

            unstake = input_txn["transaction"]["Unstake"]

            block_data["unstake_data"].append(
                parse_unstake(
                    txn_hash,
                    unstake,
                    input_txn["weight"],
                    epoch,
                )
            )

    return block_data


def insert_data(database, epoch_data):
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
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    sql = """
        INSERT INTO
            stake_txns
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(sql, epoch_data["stake_data"])

    sql = """
        INSERT INTO
            unstake_txns
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(sql, epoch_data["unstake_data"])

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
    args = parser.parse_args()

    # Parse the configuration
    config = toml.load(args.config_file)
    BlockchainConfig.config = config

    # Create database
    database = (
        f"{os.path.realpath(os.path.dirname(__file__))}/mockups/data/database.sqlite3"
    )
    if os.path.exists(database):
        os.remove(database)

    create_tables(database)

    # Define blocks containing transactions for tests using mockups
    blocks = [
        # Mint transaction (pre-wit/2): 55b1e998e00e62a71664b52191450788fe0c4a62bab32cc995cf89281845998f
        "efbe6fbdabe9754441e424de95e540ef35da4087f407f0b9b968043ae1c4960a",
        # Value transfer (change): de12b689901bb1a9ddd5e3034d6d20146f8ffdf2176438b7513b64775923853c
        "e737df5565547ad8136b6aa52ce1f4f234f947168be6763d6ac7d336ee45f2ce",
        # Value transfer (no change): e22cad472149be98e113cfdd6b8da1a91195452ffcdf279d0e2b7fa581e8880d
        "82a014777204c746245fbf4cdf7048a3ffc266be004621122304c5b40f273641",
        # Data request (HTTP-GET, pre-wit/2): d05b3d4622dd7f5bbb54fb597f944343d6d1367286fcef53e071c48531a2caac
        "65214181d85e0457db23358e577cf4269a2e0a6c0b8a029c4f99a8fccbc308fa",
        # Commits for data request: d05b3d4622dd7f5bbb54fb597f944343d6d1367286fcef53e071c48531a2caac
        "e1130ee3282ee37ca32081d5468ff4d4ebfb8eeb23a1e0f279d114369bd88aa2",
        # Reveals for data request: d05b3d4622dd7f5bbb54fb597f944343d6d1367286fcef53e071c48531a2caac
        "a89c04ff368f322367ac39954a8928aedb60627302ab5808155d364c01f39b20",
        # Tally for data request: d05b3d4622dd7f5bbb54fb597f944343d6d1367286fcef53e071c48531a2caac
        "acfb91728ffdd9c497c5e9c01ee823f8692f6c1f8bb4832a3658acc424aac864",
        # Extra blocks for DRO history
        "63f227b6ccac2a67b22fcbf1df018d1c0e56680e247e9fd25a300af07f5ffa9a",
        "6d048ed60137b565976b2d5f14d7e6cc5027c439871342342019143dc5506188",
        # Extra blocks for RAD history
        "b16d2f83b7f087b1796292085cc16b4549c25bb4ca380951d9b55726a1ad92e7",
        "943d9718fcac0ba35c0f13ed224774bbd15078117758807b97b6ffd9dcf1a861",
        "8fed9c50e9ad39075ceec659b12658c2afb48accdced4ab7fac4307f0a48d523",
        "ade52c026bbbbf463cd97324bbbd10e46e6b89956e6361c16cb1a0ed3ba82b9f",
        # Mint transaction (post-wit/2): ebda819d3e69db289ed43e66c58c8dfd155c3de1ece316f1bccceca47446e556
        # Data request (HTTP-GET, post-wit/2): 3bf3a7d038ecc3ca3da27083919653453ddce9f1e06a8be10fc360893cd87919
        "40e8448d93d4e28e30e310cb1502918830343711bc673f6dd7d359c15d3442f6",
        # Commits for data request: 3bf3a7d038ecc3ca3da27083919653453ddce9f1e06a8be10fc360893cd87919
        "d7569187caac509846bae2d94fffe3c240830b0f3e687bed9591e238c53137bb",
        # Reveals for data request: 3bf3a7d038ecc3ca3da27083919653453ddce9f1e06a8be10fc360893cd87919
        "352817e548d5a308eca11a001d637e06a8deec97a00f36e307c8f8be6be582fb",
        # Tally for data request: 3bf3a7d038ecc3ca3da27083919653453ddce9f1e06a8be10fc360893cd87919
        "1386fb207837f26ea69ae332ecb8e200b63a8fc35911b1dc3df46f81967cf6ad",
        # Data request (RNG, tooManyWitnesses): 81bd191c77e5ce8a795911846c6adf1e0337ba5828e945c7d38531b511478f41
        "f641f12371b6136fadc0444d58dd189c297be6a702aa7d00f9ab28f6634992cb",
        # Stake transaction (change): 7fdbfba5239650557dae61e0ae94bec383dcdc96a2e8b7f77a36b916ad1b0a0b
        "bae93cc611419c17c0d6b004b218c4c90a7a2220c351607615e919dbb32aeab6",
        # Stake transaction (no change): 0ea59be6f3c154836b26387dcdfbb5372f47a338706b8f108da342b699f0f1b3
        "5f14f59d63fc6da7128f12f3d55bd7c6af26c3b7266b1bba12613ac40b14a554",
        # Unstake transaction: c66e40e48763678b4ada2442e59b5a04b69a620a2bf72799abf0cd652a5a68e7
        "d25914d1190f1b2dba0cd4119919d9f32e962a6b9d703b224c58ac090c90cd0a",
        # Blocks for API tests
        "be325f40cf9a05e9066caf39027660d1ef3ba03c135e7667ff7d6d59ae5affc3",
        "d3d7e127e1789f9c098eafc584f187555cb1b0e9ef5ab2b99bd0fde9dde989b0",
        "70ac2d6d10dbdc99d5258b9ea1275dd3f7e3431efc95703a8b682e470705e1c5",
        "06e32259a77efd5125e0e435cfe1eb003af3983e782e083ea7a035b2b4ab21ae",
        "f8c50b004b475bb88da1e7e1183906de9506e8779ef5633625d949b83a24dbf1",
        "d83762224a9a44bab0055458739bd97c6fe7dbfb955768bdca42157ba1b9116f",
        "f993c9526c8c5784852fbc4b66de654aa7bd0a09531d904e16340260d2d1de92",
        # Blocks for stake views (twit1pc8jzqph4t0md02e6fgwgsw26yll20p98c3pgh)
        "795cc4c0c2e9ff54df6c9b93d2f3548f87118bb20d7936d831134e2336be6e16",
        "b63def3b79fdeb86b9ffdb4b472e8fef3550a1cc0396851118c66299a24864cb",
        "f60b61ffb71f780a5feab03dbd7f0e0a650290a0dbb8287888fe439a300279a8",
        "5ae3f5a4375876dff6168212985f19aa2f36275afe204304ae897ebd74fcd464",
        "1c434e5c1b500f85489fe0de90c576cea1c8d2cc0e7ccefa05deba9bb84d58a7",
        "94d124e41865847babe1efe0ad0ee5fb6de90d5cc1549527a742d5062c2c3267",
        "229a00c51d0f4e4de69b808e0b6bf25dfaab6e22f130d71af987579acfbf1b92",
        "6c7118c2dd8fa6a71f509543babb70b1426fe095db08760ac5a3c18e6430514b",
        "b357b5bffb815287ed944486edad5d70bbf299d510abe1f086f7d95778cd63fc",
        "a7ad269f0a027696a75ff9d9b80c9842a0184e0ab390a289bdd769dd230181a9",
        # Blocks for unstake views (twit1f0am8c97q2ygkz3q6jyd2x29s8zaxqlxcqltxx)
        "4dda7a67e0548faeb0bdcfd56ec85b0245b5e34ffd8e4f5d53bfd7da4df70545",
        "e12da3586652fd66b9f47704ce5afadb4ba348ce8791f9b6419b7806f65c9429",
        "464cb477db28ad0288fa6875e1499e14373e81fc052bb89e532bc0bc664af01c",
        "fcccc8f0d7246a8b21e93641efaf482f0e9c9dfb24fa99048a5596dad20aa9a3",
        "a3cf0abdcd43310fddd7aa330337f8a7b09db7cffdc685e7b12d1b1ad6c3e2b0",
        "94150ab5fd4ed773acf37bfe42442e96969eb2ca8674042bf342c5212fa2a77f",
        "3d0ba78c2c838c38413e2e3877e9816529451aa27287ee6e0a3dd708e96350f2",
        "962bbb92febf38a57183205ae0aa18b7e779dab5a755193bb3c621a5bba57e7c",
        "a1530ed89009225efef860aa64f2ebc38f15d5c98224f7191490f79c851b65dd",
        "5039888ee1ecc5807fe8e6b64d306293cd51d2f77ea88e35a8bbbb2c35016f43",
        # Blocks for data requests created (twit19rnvq8yjrmpa6tjdahct4pd49ha5lmvxjeyfha)
        "618f492c12a7500115df8d6f28859f40b8c5ab209981056d328638dad0d554bf",
        "908869efd655ee5854af970ac2c8ccdfbcc1f1d00d7e3cdd076a740b4ea09e5a",
        "1881f92594596d69d5b06954c7607bc950fea73aa24e62b12ece99e99e8310ef",
        "51ce0e234ee3addf8c8ecdcac6edfc83e7cb0874cc3f44d5608a57df5d0669c8",
        "00e8ec994c00aaf47217f18355d5bbda3a79d7bd6d0da653104dc02694f40606",
        "a7c2b91babb1a5438649de9ae5335abf97a37e87ba61096db1ef29eaf0d6f28a",
        "744cd2f1cac4db669eefc4f2b9261e4f9e335bebb3f615da3a65f3aa385773ea",
        "4bbe4da4a38b69714dec8a692bc28fcbf928f9e937a128708d286542d6c1261d",
        "f3dea1ec4687408c88cd928c04e2216758e6d580be1d3359b78047445565f27d",
        "7c2da9767443895d6734b8c5351651faf7e34d7800390512fba897f6fd61b732",
        "c566a897fa0b17285cb399ac0a02580b2042b24b48a9bfa2c5d08162acc4070e",
        "91b99a3d2c245b5acde86037972eff3ca9840d8036312481fc99f3f37eb1f019",
        "4723a5b6453317c1498f6cbc7acbd06e1d37eadde47c4afcd3f205af3148a1dd",
        "b393029ef97f8d383eb1224da9c69ded46eee2e582675f191d234def95069765",
        "1889c6830e542ca96dc0cd37f027b556271447495c9a28489e886533995f03a0",
        "d964dcf3007b79a7d8ffe943da507d88c295a2c6bfb719e49fa84465e5e4629d",
        "e8ad17773ef86e2b7eca020becf8dd0967c8adda4c9af9c309344524e2afd3f7",
        "91722537ea0ae4b24562af091b8f25c2356d27f39c5d62c7723faf87a50da09b",
        "be2960f91c1cafba5fd5789042400885e85bfc37f4436e4ad9932f1487dbc882",
        "c56fefced51d4f168a234b58a19b8197712ac03a64b4e8635286381c5db90e3d",
        # Blocks for data requests solved (twit1a0dkarqa5w36quq37tvyxthlhl5049wg63z3zu)
        "c2bd2164686f638f56de50e42211cf9d9d9749be0c533fe1b83bfe5f0b5ddff4",
        "b166923b6d09e1f290e9bc77981d048710b9983b743dc3d678a5add913ad6c84",
        "8fe004a453259b5f2f77416df9d80d649f19bfc680110d2297530520b2abf30b",
        "ccda8d2bdf8526602fdc20fdfc977fc62baae340306b43289ddf7f5514d8a754",
        "821a27b283c2f29dec0c34e81376a7846ce5b77e249ad0348d74841aeb0ef7ca",
        "0022665de7f57e83e10124c39d3c707143c93148d198d312bb0f3b8a0598a776",
        "0d55ae3a454fd7ce9679db924efc9243f7a3a57432a411ed3284c6dfd649c93d",
        "b47c4be0d3bb8bbb289ca5b23a19bbed612d88a69959fd94f4322b98a1eaa422",
        "f2e9716e00028b1e0453d71cc8241a7f4e77a9b9d5449491e8b148279314d25e",
        "c44fcd2b036afcc0950b55f92e5b67f3c9e085a8575a52db27775b3817b0aa11",
        "851dfe4973ed5a5057589e2a920d28f450082505447bc9afcefbc2ffd148f2b6",
        "3e9f0fb4a83542fa8824b0c30f99a13f1cfc0a17aa12643f55b522a15dcfebc3",
        "793459983cf7e4ff99a49084312563a578c8d86bae1750b469b186db43178466",
        "4a48653480311669181dbb1ad45b8e781bb994cfbe82c6f1abadabc330801e6e",
        "49e646a7581746127bdb1480a7b853929f75c7387b1a5a7d9384ab181a2f081b",
        "8e550ea561d6833645caeb7b16f78d9c933b92af089f4ab2cbae780b43c3cd5e",
        "9d712d32a7703aaf352d3c1edf413042f2fd1219d3d8c689f40066172e16a889",
        "ff2e8ca9f7c8fad66757aaeab4ffbd62209882507f82070d4b9124ee5da728ad",
        "75f83aae492d00b27595cdf49eff7834bbcc39e85eb2ad8ac97801008bd95bf1",
        "cb3d0b7f6f2de20305951b2b08b010d216e5efefb7d996086d9d0e36fc9e0828",
        "0a81063013e59fa48abe1aab2b71d101bd821de450d6f43ac0fc25a72e53b5c8",
        "09902eb9afd1ee10384fce301d831e055065ee5956ecabc324df3b5d5cde1ea0",
        "bc531265f3d7cbcce253d6a39ef4d0d006f9d55c3484bb45b4c0323461a8afb5",
        "faa1f16ce532b13d18f05e8b69b224c7760f73c787cc38010f81dbf4082a3656",
        "80497e52923082a5210b938506e0584735b22cc6584abe45d78c83a95041feb1",
        "63ab64e57228c36d57464dde2dfbdc00908728da51516e5113848c4f1fdd7bdd",
        "3aca83907e9719ae39bc532700e97b6f31b980159186c7c0faa9948231d27696",
        "626acdf90845f5f250ca516ef2b71c0c2c432274b3847e62c21d12cdcafee42a",
    ]

    # First insert the WIPs in the database
    insert_wips(database)

    # Now, create a WIP object using the inserted data
    BlockchainConfig.wip = WIP(mockup=True)

    # Parse and insert data from above defined blocks
    print("Getting full block data")
    requested_txns = {}
    block_data, input_txns = get_block_data_from_node(blocks, requested_txns)
    insert_data(database, block_data)

    # Parse and insert all transactions used as input in the transactions extracted from above blocks
    print("Getting all input transactions")
    inputs_data = get_input_transactions(input_txns, requested_txns)
    insert_data(database, inputs_data)

    insert_address_data(database)

    insert_consensus_constants(database)

    insert_network_stats(database)

    insert_pending_transaction(database)


if __name__ == "__main__":
    main()
