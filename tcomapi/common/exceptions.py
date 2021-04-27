class NoBinsToParseTaxPayments(Exception):
    pass


class ExternalSourceError(Exception):
    pass


class BadDataType(Exception):
    pass


class ServerError(Exception):
    pass


class FtpDirectoryNotExists(Exception):
    pass
