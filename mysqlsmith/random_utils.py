"""Random utilities: RNG and dice functions matching C++ sqlsmith ranges."""

import random as _random

rng = _random.Random()


def seed(s: int):
    rng.seed(s)


def d6() -> int:
    """C++ range: uniform_int_distribution<>(1, 3)"""
    return rng.randint(1, 3)


def d9() -> int:
    """C++ range: uniform_int_distribution<>(1, 4)"""
    return rng.randint(1, 4)


def d12() -> int:
    """C++ range: uniform_int_distribution<>(1, 6)"""
    return rng.randint(1, 6)


def d20() -> int:
    """C++ range: uniform_int_distribution<>(1, 10)"""
    return rng.randint(1, 10)


def d42() -> int:
    """C++ range: uniform_int_distribution<>(1, 42)"""
    return rng.randint(1, 42)


def d100() -> int:
    """C++ range: uniform_int_distribution<>(1, 50)"""
    return rng.randint(1, 50)


def random_pick(collection):
    """Pick a random element from a list/sequence. Raises on empty."""
    if not collection:
        raise RuntimeError("No candidates available")
    return rng.choice(collection)


def random_pick_iter(mapping, key):
    """Pick a random value from a multimap (dict[key] -> list) entry.
    Raises RuntimeError if key is absent or list is empty."""
    items = mapping.get(key, [])
    if not items:
        raise RuntimeError("No candidates available")
    return rng.choice(items)
