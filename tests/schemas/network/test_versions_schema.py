import pytest
from marshmallow import ValidationError

from schemas.network.version_schema import (
    NetworkVersion,
    NetworkVersionArgs,
    NetworkVersionResponse,
)

valid_versions = [
    "V1_7",
    "V1_8",
    "V2_0",
]


def test_network_version_args_success():
    data = {"key": "current"}
    network_version = NetworkVersionArgs().load(data)
    assert network_version["key"] == "current"


def test_network_version_args_failure_one_of():
    data = {"key": "none"}
    with pytest.raises(ValidationError) as err_info:
        NetworkVersionArgs().load(data)
    assert err_info.value.messages["key"][0] == "Must be one of: all, current."


def test_network_version_args_failure():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkVersionArgs().load(data)
    assert err_info.value.messages["key"][0] == "Missing data for required field."


@pytest.fixture
def network_version():
    return {
        "version": "V1_7",
        "epoch": 1_000,
        "period": 45,
    }


def test_network_version_success(network_version):
    NetworkVersion().load(network_version)


def test_network_version_failure_one_of(network_version):
    network_version["version"] = "V1_6"
    with pytest.raises(ValidationError) as err_info:
        NetworkVersion().load(network_version)
    assert (
        err_info.value.messages["version"][0]
        == f"Must be one of: {', '.join(valid_versions)}."
    )


def test_network_version_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkVersion().load(data)
    assert len(err_info.value.messages) == 3
    assert err_info.value.messages["version"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["period"][0] == "Missing data for required field."


@pytest.fixture
def network_versions():
    return [
        {
            "version": "V1_7",
            "epoch": 1_000,
            "period": 45,
        },
        {
            "version": "V1_8",
            "epoch": 2_000,
            "period": 45,
        },
    ]


def test_network_version_response_success(network_versions):
    # Test with required fields only
    NetworkVersionResponse().load(
        {
            "current_version": "V1_8",
            "current_epoch": 3_000,
        }
    )

    # Test with all fields
    NetworkVersionResponse().load(
        {
            "current_version": "V1_8",
            "current_epoch": 3_000,
            "versions": network_versions,
        }
    )


def test_network_version_response_failure_one_of(network_version):
    network_version["current_version"] = "V1.6"
    with pytest.raises(ValidationError) as err_info:
        NetworkVersionResponse().load(network_version)
    assert (
        err_info.value.messages["current_version"][0]
        == f"Must be one of: {', '.join(valid_versions)}."
    )


def test_network_version_response_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkVersionResponse().load(data)
    assert len(err_info.value.messages) == 2
    assert (
        err_info.value.messages["current_version"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["current_epoch"][0]
        == "Missing data for required field."
    )
