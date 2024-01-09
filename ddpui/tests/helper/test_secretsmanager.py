from unittest.mock import patch, Mock
from ddpui.utils.secretsmanager import (
    Org,
    generate_github_token_name,
    generate_warehouse_credentials_name,
    generate_superset_credentials_name,
    save_github_token,
    retrieve_github_token,
    delete_github_token,
    save_warehouse_credentials,
    update_warehouse_credentials,
    retrieve_warehouse_credentials,
    delete_warehouse_credentials,
    save_superset_usage_dashboard_credentials,
    retrieve_superset_usage_dashboard_credentials,
)


@patch("ddpui.utils.secretsmanager.uuid4", Mock(return_value="unique-val"))
def test_generate_github_token_name():
    org = Org(name="temporg", slug="temporg")
    response = generate_github_token_name(org)
    assert response == "gitrepoAccessToken-temporg-unique-val"


@patch("ddpui.utils.secretsmanager.uuid4", Mock(return_value="unique-val"))
def test_generate_warehouse_credentials_name():
    org = Org(name="temporg", slug="temporg")
    response = generate_warehouse_credentials_name(org)
    assert response == "warehouseCreds-temporg-unique-val"


@patch("ddpui.utils.secretsmanager.uuid4", Mock(return_value="unique-val"))
def test_generate_superset_credentials_name():
    response = generate_superset_credentials_name()
    assert response == "supersetCreds-unique-val"
    assert response.startswith("supersetCreds")


@patch(
    "ddpui.utils.secretsmanager.generate_github_token_name",
    Mock(return_value="secretname"),
)
@patch("ddpui.utils.secretsmanager.get_client")
def test_save_github_token(mock_getclient: Mock):
    createsecret_mock = Mock(return_value={"Name": "secretname"})
    mock_getclient.return_value = Mock(create_secret=createsecret_mock)
    org = Mock(
        slug="orgslug", dbt=Mock(gitrepo_access_token_secret="oldval", save=Mock())
    )
    save_github_token(org, "newtoken")
    createsecret_mock.assert_called_once_with(
        Name="secretname", SecretString="newtoken"
    )
    assert org.dbt.gitrepo_access_token_secret == "secretname"


@patch("ddpui.utils.secretsmanager.get_client")
def test_retrieve_github_token(mock_getclient: Mock):
    get_secret_value_mock = Mock(return_value={"SecretString": "secret-string"})
    mock_getclient.return_value = Mock(get_secret_value=get_secret_value_mock)
    orgdbt = Mock(gitrepo_access_token_secret="access-token")
    response = retrieve_github_token(orgdbt)
    get_secret_value_mock.assert_called_once_with(SecretId="access-token")
    assert response == "secret-string"


@patch("ddpui.utils.secretsmanager.get_client")
def test_delete_github_token(mock_getclient: Mock):
    delete_secret_mock = Mock()
    save_mock = Mock()
    mock_getclient.return_value = Mock(delete_secret=delete_secret_mock)
    org = Mock(dbt=Mock(gitrepo_access_token_secret="access-token", save=save_mock))
    delete_github_token(org)
    delete_secret_mock.assert_called_once_with(SecretId="access-token")
    save_mock.assert_called_once()


@patch(
    "ddpui.utils.secretsmanager.generate_warehouse_credentials_name",
    Mock(return_value="warehousecreds"),
)
@patch("ddpui.utils.secretsmanager.get_client")
def test_save_warehouse_credentials(mock_getclient: Mock):
    create_secret_mock = Mock(return_value={"Name": "warehousecreds"})
    mock_getclient.return_value = Mock(create_secret=create_secret_mock)
    warehouse = Mock(org=Mock())
    response = save_warehouse_credentials(warehouse, {"credkey": "credval"})
    assert response == "warehousecreds"
    create_secret_mock.assert_called_once_with(
        Name="warehousecreds", SecretString='{"credkey": "credval"}'
    )


@patch("ddpui.utils.secretsmanager.get_client")
def test_update_warehouse_credentials(mock_getclient: Mock):
    update_secret = Mock(return_value={"Name": "warehousecreds"})
    mock_getclient.return_value = Mock(update_secret=update_secret)
    warehouse = Mock(credentials="credentialskey")
    update_warehouse_credentials(warehouse, {"credkey": "credval"})
    update_secret.assert_called_once_with(
        SecretId="credentialskey", SecretString='{"credkey": "credval"}'
    )


@patch("ddpui.utils.secretsmanager.get_client")
def test_retrieve_warehouse_credentials(
    mock_getclient: Mock,
):
    get_secret_value = Mock(return_value={"SecretString": '{"credkey": "credval"}'})
    mock_getclient.return_value = Mock(get_secret_value=get_secret_value)
    warehouse = Mock(credentials="credentialskey")
    retrieve_warehouse_credentials(warehouse)
    get_secret_value.assert_called_once_with(SecretId="credentialskey")


@patch("ddpui.utils.secretsmanager.get_client")
def test_delete_warehouse_credentials(
    mock_getclient: Mock,
):
    delete_secret = Mock()
    mock_getclient.return_value = Mock(delete_secret=delete_secret)
    warehouse = Mock(credentials="credentialskey")
    delete_warehouse_credentials(warehouse)
    delete_secret.assert_called_once_with(SecretId="credentialskey")


@patch(
    "ddpui.utils.secretsmanager.generate_superset_credentials_name",
    Mock(return_value="supersetcreds"),
)
@patch("ddpui.utils.secretsmanager.get_client")
def test_save_superset_usage_dashboard_credentials(mock_getclient: Mock):
    create_secret_mock = Mock(return_value={"Name": "supersetcreds"})
    mock_getclient.return_value = Mock(create_secret=create_secret_mock)
    response = save_superset_usage_dashboard_credentials({"credkey": "credval"})
    assert response == "supersetcreds"
    create_secret_mock.assert_called_once_with(
        Name="supersetcreds", SecretString='{"credkey": "credval"}'
    )


@patch("ddpui.utils.secretsmanager.get_client")
def test_retrieve_superset_usage_dashboard_credentials(
    mock_getclient: Mock,
):
    get_secret_value = Mock(return_value={"SecretString": '{"credkey": "credval"}'})
    mock_getclient.return_value = Mock(get_secret_value=get_secret_value)
    retrieve_superset_usage_dashboard_credentials("credentialskey")
    get_secret_value.assert_called_once_with(SecretId="credentialskey")
