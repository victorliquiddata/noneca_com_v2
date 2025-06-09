# tests/test_orders_extractor.py
import pytest
from types import SimpleNamespace
from src.extractors.orders_extractor import extract_orders
from src.extractors.ml_api_client import MLClient, create_client


def test_extract_orders_pagination_and_limit(monkeypatch):
    """
    Test that extract_orders correctly paginates through multiple pages,
    and that max_records caps the total records.
    """
    # Prepare stub pages
    orders = [
        {"id": "o1"},
        {"id": "o2"},
        {"id": "o3"},
    ]
    pages = [
        {"results": orders[:2], "paging": {"offset": 0, "limit": 2}},
        {"results": orders[2:], "paging": {"offset": 2, "limit": 2}},
        {"results": [], "paging": {"offset": 4, "limit": 2}},
    ]
    call_count = {"idx": 0}

    class StubClient:
        def get_orders(
            self,
            token,
            seller_id,
            date_from=None,
            date_to=None,
            sort=None,
            limit=None,
            offset=None,
        ):
            idx = call_count["idx"]
            resp = pages[idx]
            call_count["idx"] += 1
            return resp

    # Monkeypatch create_client to return our stub and dummy token
    monkeypatch.setattr(
        "src.extractors.orders_extractor.create_client",
        lambda: (StubClient(), "dummy_token"),
    )

    # Without max_records: should return all 3 orders
    result = extract_orders(seller_id="seller123", limit=2)
    assert len(result) == 3
    assert result == orders

    # Reset call_count and test with max_records
    call_count["idx"] = 0
    result2 = extract_orders(seller_id="seller123", limit=2, max_records=2)
    assert len(result2) == 2
    assert result2 == orders[:2]


def test_extract_orders_empty_first_page(monkeypatch):
    """
    Test that when the first page is empty, extract_orders returns an empty list.
    """
    stub = SimpleNamespace(
        get_orders=lambda token, seller_id, **kwargs: {"results": [], "paging": {}}
    )
    monkeypatch.setattr(
        "src.extractors.orders_extractor.create_client", lambda: (stub, "dummy_token")
    )
    result = extract_orders(seller_id="seller123")
    assert result == []


def test_mlclient_get_orders_params_forwarding(monkeypatch):
    """
    Test that MLClient.get_orders forwards parameters correctly to _req.
    """
    client = MLClient()
    captured = {}

    def fake_req(method, endpoint, **kwargs):
        captured["method"] = method
        captured["endpoint"] = endpoint
        captured["params"] = kwargs.get("params", {})
        # Return empty results to end pagination
        return {"results": [], "paging": {}}

    monkeypatch.setattr(client, "_req", fake_req)

    # Call get_orders with specific parameters
    client.get_orders(
        token="tok",
        seller_id="seller123",
        date_from="2025-05-01",
        date_to="2025-05-31",
        sort="date_closed",
        limit=5,
        offset=10,
    )

    # Verify _req invocation
    assert captured["method"] == "GET"
    assert captured["endpoint"] == "/orders/search"
    assert captured["params"] == {
        "seller": "seller123",
        "limit": 5,
        "offset": 10,
        "sort": "date_closed",
        "order.date_created.from": "2025-05-01",
        "order.date_created.to": "2025-05-31",
    }


@pytest.mark.parametrize(
    "page, expected_offset",
    [
        ({"results": [{"id": "x"}], "paging": {}}, 100),
        ({"results": [{"id": "y"}], "paging": {"limit": 3}}, 3),
    ],
)
def test_extract_orders_missing_paging(monkeypatch, page, expected_offset):
    calls = {"idx": 0}
    pages = [page, {"results": [], "paging": {}}]

    # Mutable index for our stub closure
    idx = [0]
    recorded_offsets = []

    def stub_get_orders(
        token,
        seller_id,
        date_from=None,
        date_to=None,
        sort=None,
        limit=None,
        offset=None,
    ):
        # record the offset for this call
        recorded_offsets.append(offset)
        # grab the page for this call
        resp = pages[idx[0]]
        idx[0] += 1
        return resp

    # Monkey-patch the extractor to use our stub
    monkeypatch.setattr(
        "src.extractors.orders_extractor.create_client",
        lambda: (SimpleNamespace(get_orders=stub_get_orders), "tok"),
    )

    # Run with default page size=100
    extract_orders(seller_id="s", limit=100)

    # We should have two calls: one at offset=0, one at offset=expected_offset
    assert recorded_offsets == [0, expected_offset]
