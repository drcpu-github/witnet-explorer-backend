import pytest
from marshmallow import ValidationError

from schemas.network.tapi_schema import (
    AcceptanceRates,
    NetworkTapiArgs,
    NetworkTapiResponse,
)


def test_network_tapi_args_success():
    data = {}
    tapi = NetworkTapiArgs().load(data)
    assert not tapi["return_all"]

    data = {"return_all": "True"}
    reputation = NetworkTapiArgs().load(data)
    assert reputation["return_all"]


def test_network_tapi_args_failure_type():
    data = {"return_all": "abc"}
    with pytest.raises(ValidationError) as err_info:
        NetworkTapiArgs().load(data)
    assert err_info.value.messages["return_all"][0] == "Not a valid boolean."


def test_acceptance_rates_success():
    data = {"global_rate": 1.1, "periodic_rate": 29.7, "relative_rate": 29.7}
    AcceptanceRates().load(data)

    data = [
        {"global_rate": 1.1, "periodic_rate": 29.7, "relative_rate": 29.7},
        {"global_rate": 2.9, "periodic_rate": 48.1, "relative_rate": 38.9},
    ]
    AcceptanceRates(many=True).load(data)


def test_acceptance_rates_failure_range():
    data = {"global_rate": -1.0, "periodic_rate": -1.0, "relative_rate": -1.0}
    with pytest.raises(ValidationError) as err_info:
        AcceptanceRates().load(data)
    assert (
        err_info.value.messages["global_rate"][0]
        == "Must be greater than or equal to 0."
    )
    assert (
        err_info.value.messages["periodic_rate"][0]
        == "Must be greater than or equal to 0."
    )
    assert (
        err_info.value.messages["relative_rate"][0]
        == "Must be greater than or equal to 0."
    )


def test_acceptance_rates_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        AcceptanceRates().load(data)
    assert (
        err_info.value.messages["global_rate"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["periodic_rate"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["relative_rate"][0]
        == "Missing data for required field."
    )


def test_network_tapi_response_success():
    data = {
        "activated": True,
        "active": False,
        "bit": 0,
        "current_epoch": 1945885,
        "description": "Activation of TAPI itself (WIP0014) and setting a minimum data request mining difficulty (WIP0016)",
        "finished": True,
        "global_acceptance_rate": 81.6,
        "last_updated": 1690230905,
        "plot": "fake_plot_string_1",
        "rates": [
            {"global_rate": 1.1, "periodic_rate": 29.7, "relative_rate": 29.7},
            {"global_rate": 2.9, "periodic_rate": 48.1, "relative_rate": 38.9},
            {"global_rate": 5.1, "periodic_rate": 60.5, "relative_rate": 46.1},
        ],
        "relative_acceptance_rate": 81.6,
        "start_epoch": 522240,
        "start_time": 1626166845,
        "stop_epoch": 549120,
        "stop_time": 1627376445,
        "tapi_id": 4,
        "title": "WIP0014-0016",
        "urls": [
            "https://github.com/witnet/WIPs/blob/master/wip-0014.md",
            "https://github.com/witnet/WIPs/blob/master/wip-0016.md",
        ],
    }
    NetworkTapiResponse().load(data)

    data = [
        {
            "activated": True,
            "active": False,
            "bit": 0,
            "current_epoch": 1945885,
            "description": "Activation of TAPI itself (WIP0014) and setting a minimum data request mining difficulty (WIP0016)",
            "finished": True,
            "global_acceptance_rate": 81.6,
            "last_updated": 1690230905,
            "plot": "fake_plot_string_1",
            "rates": [
                {"global_rate": 1.1, "periodic_rate": 29.7, "relative_rate": 29.7},
                {"global_rate": 2.9, "periodic_rate": 48.1, "relative_rate": 38.9},
                {"global_rate": 5.1, "periodic_rate": 60.5, "relative_rate": 46.1},
            ],
            "relative_acceptance_rate": 81.6,
            "start_epoch": 522240,
            "start_time": 1626166845,
            "stop_epoch": 549120,
            "stop_time": 1627376445,
            "tapi_id": 4,
            "title": "WIP0014-0016",
            "urls": [
                "https://github.com/witnet/WIPs/blob/master/wip-0014.md",
                "https://github.com/witnet/WIPs/blob/master/wip-0016.md",
            ],
        },
        {
            "activated": True,
            "active": False,
            "bit": 1,
            "current_epoch": 1945885,
            "description": "Add a median RADON reducer (WIP0017), modify the UnhandledIntercept RADON error (WIP0018) and add RNG functionality to Witnet (WIP0019)",
            "finished": True,
            "global_acceptance_rate": 94.2,
            "last_updated": 1690230905,
            "plot": "fake_plot_string_2",
            "rates": [
                {"global_rate": 2.9, "periodic_rate": 78.1, "relative_rate": 78.1},
                {"global_rate": 6.1, "periodic_rate": 85.2, "relative_rate": 81.65},
                {"global_rate": 9.3, "periodic_rate": 86.5, "relative_rate": 83.3},
                {"global_rate": 12.5, "periodic_rate": 87.0, "relative_rate": 84.2},
            ],
            "relative_acceptance_rate": 94.2,
            "start_epoch": 656640,
            "start_time": 1632214845,
            "stop_epoch": 683520,
            "stop_time": 1633424445,
            "tapi_id": 5,
            "title": "WIP0017-0018-0019",
            "urls": [
                "https://github.com/witnet/WIPs/blob/master/wip-0017.md",
                "https://github.com/witnet/WIPs/blob/master/wip-0018.md",
                "https://github.com/witnet/WIPs/blob/master/wip-0019.md",
            ],
        },
    ]
    NetworkTapiResponse(many=True).load(data)


def test_network_tapi_response_failure_url():
    data = {
        "activated": True,
        "active": False,
        "bit": 0,
        "current_epoch": 1945885,
        "description": "Activation of TAPI itself (WIP0014)",
        "finished": True,
        "global_acceptance_rate": 81.6,
        "last_updated": 1690230905,
        "plot": "fake_plot_string_1",
        "rates": [],
        "relative_acceptance_rate": 81.6,
        "start_epoch": 522240,
        "start_time": 1626166845,
        "stop_epoch": 549120,
        "stop_time": 1627376445,
        "tapi_id": 4,
        "title": "WIP0014-0016",
        "urls": ["http://github.com/witnet/WIPs/blob/master/wip-0014.md"],
    }
    with pytest.raises(ValidationError) as err_info:
        NetworkTapiResponse().load(data)
    assert err_info.value.messages["urls"][0][0] == "Not a valid URL."

    data["urls"] = ["github.com/witnet/WIPs/blob/master/wip-0014.md"]
    with pytest.raises(ValidationError) as err_info:
        NetworkTapiResponse().load(data)
    assert err_info.value.messages["urls"][0][0] == "Not a valid URL."


def test_network_tapi_response_failure_required():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkTapiResponse().load(data)
    assert len(err_info.value.messages) == 17
    assert err_info.value.messages["activated"][0] == "Missing data for required field."
    assert err_info.value.messages["active"][0] == "Missing data for required field."
    assert err_info.value.messages["bit"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["current_epoch"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["description"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["finished"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["global_acceptance_rate"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["last_updated"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["rates"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["relative_acceptance_rate"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["start_epoch"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["start_time"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["stop_epoch"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["stop_time"][0] == "Missing data for required field."
    assert err_info.value.messages["tapi_id"][0] == "Missing data for required field."
    assert err_info.value.messages["title"][0] == "Missing data for required field."
    assert err_info.value.messages["urls"][0] == "Missing data for required field."


def test_network_tapi_response_failure_range():
    data = {
        "activated": True,
        "active": False,
        "bit": -1,
        "current_epoch": -1,
        "description": "Activation of TAPI itself (WIP0014)",
        "finished": True,
        "global_acceptance_rate": -1,
        "last_updated": -1,
        "plot": "fake_plot_string_1",
        "rates": [],
        "relative_acceptance_rate": -1,
        "start_epoch": -1,
        "start_time": -1,
        "stop_epoch": -1,
        "stop_time": -1,
        "tapi_id": -1,
        "title": "WIP0014-0016",
        "urls": ["https://github.com/witnet/WIPs/blob/master/wip-0014.md"],
    }
    with pytest.raises(ValidationError) as err_info:
        NetworkTapiResponse().load(data)
    assert len(err_info.value.messages) == 10
    assert err_info.value.messages["bit"][0] == "Must be greater than or equal to 0."
    assert (
        err_info.value.messages["current_epoch"][0]
        == "Must be greater than or equal to 0."
    )
    assert (
        err_info.value.messages["global_acceptance_rate"][0]
        == "Must be greater than or equal to 0."
    )
    assert (
        err_info.value.messages["last_updated"][0]
        == "Must be greater than or equal to 0."
    )
    assert (
        err_info.value.messages["relative_acceptance_rate"][0]
        == "Must be greater than or equal to 0."
    )
    assert (
        err_info.value.messages["start_epoch"][0]
        == "Must be greater than or equal to 0."
    )
    assert (
        err_info.value.messages["start_time"][0]
        == "Must be greater than or equal to 0."
    )
    assert (
        err_info.value.messages["stop_epoch"][0]
        == "Must be greater than or equal to 0."
    )
    assert (
        err_info.value.messages["stop_time"][0] == "Must be greater than or equal to 0."
    )
    assert (
        err_info.value.messages["tapi_id"][0] == "Must be greater than or equal to 0."
    )
