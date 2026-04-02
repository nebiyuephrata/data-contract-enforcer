from __future__ import annotations

from contracts.attributor import _confidence_from_blame


def test_attribution_confidence_penalizes_hops():
    fresh = _confidence_from_blame(4102444800, 1)
    older_more_hops = _confidence_from_blame(4102444800, 3)
    assert fresh > older_more_hops
