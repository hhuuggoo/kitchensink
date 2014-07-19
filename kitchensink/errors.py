class KitchenSinkError(Exception):
    pass

class UnauthorizedAccess(KitchenSinkError):
    pass

class UnknownFunction(KitchenSinkError):
    pass

class WrappedError(KitchenSinkError):
    pass
