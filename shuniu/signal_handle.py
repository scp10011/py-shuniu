class ExitError(Exception):
    pass


class UserTimeoutError(TimeoutError):
    pass


class UserKillError(Exception):
    pass


def exit_handle(signum, frame):
    raise ExitError("user exit")


def timeout_handle(signum, frame):
    raise UserTimeoutError("timeout")


def kill_handle(signum, frame):
    raise UserKillError("user kill")

