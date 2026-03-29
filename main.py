print("Hello, version 0.2.0!")

from typing import Annotated
from fastapi import Depends

def UserDep(require_permissions: list[Permission]):
    
    return Annotated[User, Depends(require_permissions(require_permissions))]
