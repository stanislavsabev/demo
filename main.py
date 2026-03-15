print("Hello, version 0.2.0!")


def UserDep(require_permissions: list[Permission]):
    
    return Annotated[User, Depends(require_permissions(require_permissions))]
