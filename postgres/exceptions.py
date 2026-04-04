"""DUT exception hierarchy matching C++ dut::failure tree."""


class DutFailure(Exception):
    def __init__(self, msg: str = "", sqlstate: str = ""):
        super().__init__(msg)
        self.sqlstate = sqlstate


class DutBroken(DutFailure):
    pass


class DutTimeout(DutFailure):
    pass


class DutSyntax(DutFailure):
    pass
