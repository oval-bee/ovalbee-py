from ovalbee.core.asset import Asset


class Collection:
    pass

    def assets(self):
        return [Asset()]


def get_collection() -> Collection:
    return Collection()
