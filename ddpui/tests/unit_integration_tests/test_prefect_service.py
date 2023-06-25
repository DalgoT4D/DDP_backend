"""Tests for the Prefect Service"""
from uuid import uuid4
from ddpui.ddpprefect import prefect_service, schema


class TestAirbyteServer:
    """tests creation and retrieval of an airbyte server block"""

    @staticmethod
    def test_create_serverblock():
        """creates a server block"""
        TestAirbyteServer.BLOCK_NAME = "test-" + str(uuid4())
        block_id = prefect_service.create_airbyte_server_block(
            TestAirbyteServer.BLOCK_NAME
        )
        assert block_id is not None
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
            )
        )

    @staticmethod
    def test_create_connectionblock():
        """creates a connection block in prerfect"""
        conninfo = schema.PrefectAirbyteConnectionSetup(
            serverBlockName=TestAirbyteConnection.server_block_name,
            connectionBlockName=TestAirbyteConnection.connection_block_name,
            connectionId=TestAirbyteConnection.airbyte_connection_id,
        )
        TestAirbyteConnection.connection_block_id = (
            prefect_service.create_airbyte_connection_block(conninfo)
        )
        assert TestAirbyteConnection.connection_block_id is not None

    @staticmethod
    def test_get_connectionblockid():
        """looks up the prefect connection block by name and returns the block id"""
        block_id = prefect_service.get_airbyte_connection_block_id(
            TestAirbyteConnection.connection_block_name
        )
        assert block_id == TestAirbyteConnection.connection_block_id

    @staticmethod
    def test_get_block_by_id():
        """retreives the connection block by block id"""
        block = prefect_service.get_airbyte_connection_block_by_id(
            TestAirbyteConnection.connection_block_id
        )
        assert block is not None

    @staticmethod
    def test_end():
        """cleans up"""
        prefect_service.delete_airbyte_server_block(
            TestAirbyteConnection.server_block_id
        )


class TestShellBlock:
    """tests the creation and retrieval of shell blocks"""

    @staticmethod
    def test_create_shellblock():
        """creates a shell block"""
        TestShellBlock.block_name = "test-shell-" + str(uuid4())
        shell = schema.PrefectShellSetup(
            blockname=TestShellBlock.block_name,
            commands=["cmd1", "cmd2"],
            workingDir="/tmp",
            env={},
        )
        TestShellBlock.block_id = prefect_service.create_shell_block(shell)

    @staticmethod
    def test_get_shell_block():
        """retrieves the shell block by name"""
        block_id = prefect_service.get_shell_block_id(TestShellBlock.block_name)
        assert block_id == TestShellBlock.block_id

    @staticmethod
    def test_delete_shell_block():
        """deletes the shell block"""
        prefect_service.delete_shell_block(TestShellBlock.block_id)
