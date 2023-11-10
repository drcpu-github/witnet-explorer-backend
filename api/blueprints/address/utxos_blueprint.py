from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from schemas.address.utxos_schema import AddressUtxosArgs, AddressUtxosResponse
from schemas.misc.abort_schema import AbortSchema

address_utxos_blueprint = Blueprint(
    "address utxos",
    "address utxos",
    description="Fetch utxos for one or more addresses.",
)


@address_utxos_blueprint.route("/utxos")
class Reputation(MethodView):
    @address_utxos_blueprint.arguments(AddressUtxosArgs, location="query")
    @address_utxos_blueprint.response(
        200,
        AddressUtxosResponse(many=True),
        description="Returns the utxos for one or more addresses.",
    )
    @address_utxos_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Could not fetch utxos.",
                "Incorrect message format for UTXO data.",
            ]
        },
    )
    def get(self, args):
        address_caching_server = current_app.extensions["address_caching_server"]
        cache = current_app.extensions["cache"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        addresses = args["addresses"]
        logger.info(f"get_utxos({addresses})")

        address_utxos = []
        for address in addresses:
            # Send a tracking request to the address caching server
            try:
                request = {"method": "track", "addresses": [address], "id": 2}
                address_caching_server.send_request(request)
            except ConnectionRefusedError:
                logger.warning("Could not send request to address caching server")
                try:
                    address_caching_server.recreate_socket()
                    address_caching_server.send_request(request)
                except ConnectionRefusedError:
                    logger.warning(
                        "Could not recreate socket, trying again next request"
                    )

            # Fetch UTXOs from cache
            utxos = cache.get(f"{address}_utxos")
            if utxos:
                address_utxos.append(
                    {
                        "address": address,
                        "utxos": utxos,
                    }
                )
                logger.info(f"Found UTXOs for {address} in cache")
                continue

            # Or from a Witnet node
            logger.info(f"Did not find UTXOs for {address} in cache")
            utxos = witnet_node.get_utxos(address)
            if "result" in utxos:
                address_utxos.append(
                    {
                        "address": address,
                        "utxos": utxos["result"]["utxos"],
                    }
                )
            else:
                logger.error(f"Could not fetch UTXOs for {address}: {utxos}")
                abort(404, message=f"Could not fetch utxos for {address}.")

        try:
            AddressUtxosResponse(many=True).load(address_utxos)
        except ValidationError as err_info:
            logger.error(
                f"Incorrect message format for UTXO data for {address}: {err_info}"
            )
            abort(404, message=f"Incorrect message format for UTXO data for {address}.")

        return address_utxos
