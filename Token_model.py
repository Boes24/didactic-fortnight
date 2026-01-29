from sklearn.ensemble import RandomForestClassifier


class Token:
    name: str
    price_decimals: int
    quantity_decimals: int
    model: RandomForestClassifier | None = None
    free:float
    locked: float

    def __init__(self, name, price_decimals, amount_decimals, free, locked):
        self.name = name
        self.price_decimals = price_decimals
        self.quantity_decimals = amount_decimals
        self.free = free
        self.locked = locked