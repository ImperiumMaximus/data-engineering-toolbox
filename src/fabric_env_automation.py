import subprocess
import sys


def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])


install("requests-toolbelt")

import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException

import requests
from requests.auth import HTTPBasicAuth
import os
import time
from bs4 import BeautifulSoup
from requests_toolbelt.multipart.encoder import MultipartEncoder


class UploadPackageToEnvironment:
    """
    Class to upload a custom package to Microsift Fabric Environment
    Attributes:
        environment: The environment to be created
        workspace_name: The workspace in which to create the environnment
        fabric_access_token: Needed to be able to use the ENV API and upload a package
        is_devops: Whether or not the package to be uploaded is in Azure DevOps
        package_name: The name of the package from Azure. If package not from azure, keep default None
        package_version: The version of the package to be used from Azure DevOps. If package not from azure, keep default None
        devops_pat: Azure DevOps Personal Access Token avec Read package permission. If package not from azure, keep default None
        whl_url: The url of the .whl file if it is not from Azure DevOps
    """

    def __init__(
        self,
        environment,
        workspace_name,
        fabric_access_token,
        is_devops=True,
        package_name=None,
        package_version=None,
        devops_pat=None,
        whl_url=None,
    ):
        self.environment = environment
        self.fabric_client = fabric.FabricRestClient()
        self.workspace_name = workspace_name
        self.fabric_access_token = fabric_access_token
        self.is_devops = is_devops
        self.package_name = package_name
        self.package_version = package_version
        self.devops_pat = devops_pat
        self.whl_url = whl_url
        self.payload = {
            "displayName": self.environment,
            "type": "Environment",
            "description": "Default environment for DWH settings in Microsoft Fabric",
        }
        self.headers = {'Authorization': f'Bearer {self.fabric_access_token}'}
        self.base_fabric_url = "https://api.fabric.microsoft.com/v1"

    def create_and_upload(self):
        """
        Main function to create an ENV and upload python package in it
        Params:
            is_devops (bool) Indicate whether or not the package to download is in Azure DevOps Artifact
        """
        environment_id, workspace_id = self.create_fabric_item(self.payload, self.workspace_name)

        filename = (
            self.download_package_from_azure_devops(
                self.devops_pat,
                package_name=self.package_name,
                package_version=self.package_version,
            )
            if self.is_devops
            else self.get_package_whl(self.whl_url, self.is_devops)
        )

        self.upload_package_to_fabric(
            workspace_id, environment_id, filename, self.fabric_access_token
        )
        metadata = self.get_environment_metadata(workspace_id, environment_id)
        if metadata and metadata.get("properties").get('publishDetails', {}).get('state') != 'running':
            # Review staging changes
            staging_libraries = self.get_staging_libraries(workspace_id, environment_id)
            if staging_libraries:                
                # Publish the environment
                publish_result = self.publish_environment(workspace_id, environment_id)
                if publish_result:
                    print("Publish operation started. Waiting for completion...")
                    if self.wait_for_publish_completion(workspace_id, environment_id):
                        print("Environment published successfully")
                    else:
                        print("Failed to publish environment")
        else:
            print()

    def upload_package_to_fabric(
        self, workspace_id, environment_id, file_path, fabric_access_token
    ):
        """
        Upload a custom python package to fabric environment
        Params:
            workspace_id (str): The idea of the workspace where to uoload the library
            environment_id (str): The environment id within the workspace
            file_path (str): The downloaded .whl file from Atifact feeds or
        """
        url = f"{self.base_fabric_url}/workspaces/{workspace_id}/environments/{environment_id}/staging/libraries"

        with open(file_path, "rb") as file:
            file_content = file.read()
        m = MultipartEncoder(
            fields={
                "file": (
                    os.path.basename(file_path),
                    file_content,
                    "application/octet-stream",
                )
            }
        )

        headers = {
            "Content-Type": m.content_type,
            "Authorization": f"Bearer {fabric_access_token}",
        }

        response = requests.post(url, headers=headers, data=m)

        if response.status_code == 200:
            print("Package uploaded successfully to Fabric environment")
        else:
            print(
                f"Failed to upload package to Fabric. Status code: {response.status_code}"
            )
            print(f"Response: {response.text}")

    def get_package_whl(self, whl_url, auth=None, is_devops=False):
        """
        Function to dowload package .whl file
        Params:
            whl_url (str): The url of .whl ro be downloaded
            auth (str): Authentifacation token, required for private package. Default is None
            is_devops (bool): Whether the package to be downloaded is from Azure DevOps Artifact or not
        """
        filename = os.path.basename(whl_url)
        if is_devops:
            filename = filename.split("#")[0]
        response = requests.get(whl_url, auth=auth)
        if response.status_code == 200:
            with open(filename, "wb") as file:
                file.write(response.content)
                print(f"Package {filename} downloaded successfully")
            return filename
        else:
            print(f"Failed to download package. Status code: {response.status_code}")
            return

    def download_package_from_azure_devops(
        self, token, package_name, package_version
    ):
        """
        Function to get download .whl file from Azure Artifact Feed
        Params:
            token (str): Azure DevOps PAT
            package_name (str): Name of the package
            package_version (str): Package version
        """
        base_url = "https://pkgs.dev.azure.com/OrganizationName/ProjectName/_packaging/FeedName/pypi/simple"
        package_url = f"{base_url}/{package_name}/"
        auth = HTTPBasicAuth("", token)
        response = requests.get(package_url, auth=auth)
        soup = BeautifulSoup(response.text, "html.parser")
        download_url = soup.find_all("a", href=True)[0]["href"]
        filename = self.get_package_whl(download_url, auth=auth, is_devops=True)
        return filename

    def create_fabric_item(self, payload: dict, workspace=None):
        """
        Create a fabric item (lakehouse, env) using API POST request
        Params:
            payload (dict) : Request payload. Example payload = {"displayName": "TestingEnvAPI",
                            "type": "Environment",
                            description": "Default environment for DWH settings in Microsoft Fabric: custom library and required packages"}

            workspace (str): Name of the workspace where to create the fabric item
        """
        workspaceId = (
            fabric.get_workspace_id()
            if workspace is None
            else fabric.resolve_workspace_id(workspace)
        )
        environment_id = self.check_if_environment_exist(payload.get("displayName"), workspaceId)

        if environment_id:
            print(f"Environment {payload.get('displayName')} already exist. Moving to upload step")
            return environment_id, workspaceId

        try:
            response = self.fabric_client.post(
                f"/v1/workspaces/{workspaceId}/items", json=payload
            )

            if response.status_code != 201:
                raise FabricHTTPException(response)
            environment_id = response.json()["id"]
            workspace_id = response.json()["workspaceId"]
            print(f"Environment {payload.get('displayName')} succesfully created. Moving to upload step")
            return environment_id, workspace_id
        except WorkspaceNotFoundException as e:
            raise ("Caught a WorkspaceNotFoundException:", e)
        except FabricHTTPException as e:
            raise (
                "Caught a FabricHTTPException. Check the API endpoint, authentication."
            )
        return

    def check_if_environment_exist(self, display_name, workspace_id=None):
        """
        Function to check whether an envionment exist given its name and parent workspace
        Params:
            display_name (str): Name of the environment we want to get Id from
            workspace_id (str): The Id of the parent workspace: Default is None for current workspace
        """
        if not workspace_id:
            workspace_id = fabric.get_workspace_id()
        url = f"v1/workspaces/{workspace_id}/environments"
        result = self.fabric_client.get(url)
        if result.status_code == 200:
            for environment in result.json()['value']:
                if environment['displayName'] == display_name:
                    return environment['id']
            return

    def get_environment_metadata(self, workspace_id, environment_id):
        url = f"{self.base_fabric_url}/workspaces/{workspace_id}/environments/{environment_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to get environment metadata. Status code: {response.status_code}")
            return None

    def publish_environment(self, workspace_id, environment_id):
        url = f"{self.base_fabric_url}/workspaces/{workspace_id}/environments/{environment_id}/staging/publish"
        response = requests.post(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to publish environment. Status code: {response.status_code}")
            return None

    def wait_for_publish_completion(self, workspace_id, environment_id, timeout=1200):
        start_time = time.time()
        while time.time() - start_time < timeout:
            metadata = self.get_environment_metadata(workspace_id, environment_id)
            if metadata and metadata.get("properties").get('publishDetails', {}).get('state') != 'running':
                print("Environment published successfully")
                return True
            elif metadata.get("properties").get('publishDetails', {}).get('state') == 'failed':
                print("Environment publish failed")
                return False
            time.sleep(60)
        print("Publish operation timed out")
        return False

    def get_staging_libraries(self, workspace_id, environment_id):
        url = f"{self.base_fabric_url}/workspaces/{workspace_id}/environments/{environment_id}/staging/libraries"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to get staging libraries. Status code: {response.status_code}")
            return None
