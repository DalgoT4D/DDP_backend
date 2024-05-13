"""Tests for the Prefect Service"""

from uuid import uuid4
from ddpui.ddpprefect import prefect_service, schema


class TestAirbyteServer:
    """tests creation and retrieval of an airbyte server block"""

    @staticmethod
    def test_create_serverblock():
        """creates a server block"""
        TestAirbyteServer.BLOCK_NAME = "test-" + str(uuid4())
        block_id, block_name = prefect_service.create_airbyte_server_block(
            TestAirbyteServer.BLOCK_NAME
        )
        assert block_id is not None
        assert block_name is not None
        TestAirbyteServer.block_id = block_id

    @staticmethod
    def test_fetch_serverblock():
        """fetches the id of the block we just created, verifies that it is what we saved"""
        block_id = prefect_service.get_airbyte_server_block_id(
            TestAirbyteServer.BLOCK_NAME
        )
        assert block_id == TestAirbyteServer.block_id

    @staticmethod
    def test_delete_serverblock():
        """cleans up"""
        prefect_service.delete_airbyte_server_block(TestAirbyteServer.block_id)


class TestAirbyteConnection:
    """tests the creation and retrieval of airbyte connection blocks"""

    @staticmethod
    def test_start():
        """sets up an airbyte server block"""
        TestAirbyteConnection.server_block_name = "test-srvr-" + str(uuid4())
        TestAirbyteConnection.airbyte_connection_id = str(uuid4())
        TestAirbyteConnection.connection_block_name = "test-conn-" + str(uuid4())
        TestAirbyteConnection.connection_block_id = None
        TestAirbyteConnection.server_block_id = (
            prefect_service.create_airbyte_server_block(
                TestAirbyteConnection.server_block_name
            )[0]
        )

    @staticmethod
    def test_end():
        """cleans up"""
        prefect_service.delete_airbyte_server_block(
            TestAirbyteConnection.server_block_id
        )
