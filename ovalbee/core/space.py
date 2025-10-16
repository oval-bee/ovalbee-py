from ovalbee.core.collection import Collection


class Space:
    pass

    def collections(self):
        return [Collection()]


def get_space() -> Space:
    return Space()
