from util.blockchain_functions import (
    calculate_epoch_from_timestamp,
    calculate_start_timestamp_wit2,
    calculate_timestamp_from_epoch,
)


def test_calculate_start_timestamp_wit2():
    assert calculate_start_timestamp_wit2() == 1_738_193_445


def test_calculate_timestamp_from_epoch():
    assert calculate_timestamp_from_epoch(0) == 1_738_180_800
    assert calculate_timestamp_from_epoch(1) == 1_738_180_845
    assert calculate_timestamp_from_epoch(2) == 1_738_180_890
    assert calculate_timestamp_from_epoch(280) == 1_738_193_400
    assert calculate_timestamp_from_epoch(281) == 1_738_193_445
    assert calculate_timestamp_from_epoch(282) == 1_738_193_465
    assert calculate_timestamp_from_epoch(1000) == 1_738_207_825
    assert calculate_timestamp_from_epoch(1001) == 1_738_207_845
    assert calculate_timestamp_from_epoch(1002) == 1_738_207_865


def test_calculate_epoch_from_timestamp():
    assert calculate_epoch_from_timestamp(1_738_180_800) == 0
    assert calculate_epoch_from_timestamp(1_738_180_845) == 1
    assert calculate_epoch_from_timestamp(1_738_180_890) == 2
    assert calculate_epoch_from_timestamp(1_738_193_400) == 280
    assert calculate_epoch_from_timestamp(1_738_193_445) == 281
    assert calculate_epoch_from_timestamp(1_738_193_465) == 282
    assert calculate_epoch_from_timestamp(1_738_207_825) == 1000
    assert calculate_epoch_from_timestamp(1_738_207_845) == 1001
    assert calculate_epoch_from_timestamp(1_738_207_865) == 1002
