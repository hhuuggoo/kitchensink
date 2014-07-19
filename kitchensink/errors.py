class KitchenSinkError(Exception):

class UnauthorizedAccess(KitchenSinkError):
    pass

class UnknownFunction(KitchenSinkError):
    pass

class WrappedError(KitchenSinkError):
    pass
