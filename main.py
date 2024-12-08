#!/usr/bin/env python
import argparse
from src.fabric_env_automation import UploadPackageToEnvironment

parser = argparse.ArgumentParser(description="Fabric Environment Python Library Upload Automation")
parser.add_argument('--environment', type=str, help='Fabric Environment Name', required=True)
parser.add_argument('--workspace-name', type=str, help='Fabric Workspace Name', required=True)
parser.add_argument('--access-token', type=str, help='Fabric Access Token', required=True)
parser.add_argument('--is-devops', action=argparse.BooleanOptionalAction, help='Pass this flag if script is running in CI/CD pipeline', required=False)
parser.add_argument('--package-name', type=str, help='Python Package Name', required=True)
parser.add_argument('--package-version', type=str, help='Python Package Version', required=True)
parser.add_argument('--devops-pat', type=str, help='Azure DevOps PAT', required=True)
parser.add_argument('--organization-name', type=str, help='Azire DevOps Organization Name', required=True)
parser.add_argument('--project-name', type=str, help='Azure DevOps Project Name', required=True)
parser.add_argument('--feed-name', type=str, help='Azure DevOps Artifacts Feed Name', required=True)
parser.add_argument('--delete-whl', action=argparse.BooleanOptionalAction, help='Pass this flag to delete the whl file after environment has been published', required=False)

args = parser.parse_args()

if __name__ == '__main__':
    uploader = UploadPackageToEnvironment(
        environment=args.environment,
        workspace_name=args.workspace_name,
        fabric_access_token=args.access_token,
        is_devops=args.is_devops,
        package_name=args.package_name,
        package_version=args.package_version,
        devops_pat=args.devops_pat,
        organization_name=args.organization_name,
        project_name=args.project_name,
        feed_name=args.feed_name
    )
    
    uploader.create_and_upload(delete_whl=args.delete_whl)
