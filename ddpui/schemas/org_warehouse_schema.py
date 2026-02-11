from ninja import Schema


class OrgWarehouseSchema(Schema):
    """payload to register an organization's data warehouse"""

    wtype: str
    name: str
    destinationDefId: str
    airbyteConfig: dict
