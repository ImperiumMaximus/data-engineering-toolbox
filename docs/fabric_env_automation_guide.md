First, we need to initialize our UploadPackageToEnvironment class with the necessary parameters:

```python
uploader = UploadPackageToEnvironment(
    environment="MyFabricEnv",
    workspace_name="MyWorkspace",
    fabric_access_token="your_fabric_token",
    is_devops=True,
    package_name="my_custom_package",
    package_version="1.0.0",
    devops_pat="your_devops_pat"
)
```

With all these pieces in place, creating or updating a Fabric environment with a custom package is as simple as:

```python
uploader.create_and_upload()
```
