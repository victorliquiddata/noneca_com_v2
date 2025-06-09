#!/usr/bin/env python3
# src/extractors/ml_api_client.py
"""Mercado Livre API client with OAuth 2.0 integration and pagination support."""
import os
import json
import requests
import typing
from typing import Dict, Any, Optional
import secrets
from datetime import datetime, timedelta
from config.config import cfg


class MLClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {"Accept": "application/json", "User-Agent": "MLExtractor/1.0"}
        )
        self._rate = {"calls": 0, "reset": datetime.now()}

    def _check_rate(self):
        now = datetime.now()
        if now - self._rate["reset"] > timedelta(minutes=1):
            self._rate = {"calls": 0, "reset": now}
        if self._rate["calls"] >= cfg.rate_limit:
            raise Exception("Rate limit exceeded")
        self._rate["calls"] += 1

    def _req(self, method, endpoint, **kwargs):
        self._check_rate()
        url = f"{cfg.api_url}{endpoint}"

        kwargs.setdefault("headers", {}).update(
            {"X-Request-ID": secrets.token_hex(8), "Cache-Control": "no-cache"}
        )

        try:
            resp = self.session.request(method, url, timeout=cfg.timeout, **kwargs)
            resp.raise_for_status()
            return {} if resp.status_code == 204 else resp.json()
        except requests.exceptions.HTTPError:
            status = resp.status_code
            try:
                err = resp.json()
            except ValueError:
                err = {"error": "Invalid JSON"}

            error_map = {
                401: f"Unauthorized: {err}",
                403: f"Forbidden: {err}",
                404: f"Not found: {endpoint}",
                429: "Rate limited",
            }
            raise Exception(error_map.get(status, f"HTTP {status}: {err}"))
        except requests.exceptions.Timeout:
            raise Exception("Request timeout")
        except Exception as e:
            raise Exception(f"Request failed: {str(e)}")

    def _auth(self, token):
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    # Core API methods
    def get_user(self, token, user_id="me", attrs=None):
        self._auth(token)
        params = {"attributes": attrs} if attrs else {}
        return self._req("GET", f"/users/{user_id}", params=params)

    def get_items(self, token, seller_id, limit=None, status="active"):
        """
        Extract items for a seller with pagination support.

        Args:
            token: Authentication token
            seller_id: The seller ID to extract items for
            limit: Maximum number of items to extract (None for all items)
            status: Item status filter (default: "active")

        Returns:
            List of item dictionaries with full details
        """
        self._auth(token)

        collected_items = []
        offset = 0
        page_size = 100  # Maximum items per API request

        while True:
            # Calculate how many items to request this round
            if limit is not None:
                remaining = limit - len(collected_items)
                if remaining <= 0:
                    break
                current_limit = min(page_size, remaining)
            else:
                current_limit = page_size

            params = {"limit": current_limit, "offset": offset, "status": status}

            try:
                result = self._req(
                    "GET", f"/users/{seller_id}/items/search", params=params
                )
                batch_ids = result.get("results", [])

                if not batch_ids:
                    break  # No more items

                # Fetch full item details for this batch
                batch_items = []
                for item_id in batch_ids:
                    try:
                        item_details = self.get_item(token, item_id)
                        if item_details:
                            batch_items.append(item_details)
                    except Exception as e:
                        print(f"Warning: Failed to get details for item {item_id}: {e}")
                        continue

                collected_items.extend(batch_items)
                offset += len(batch_ids)

                # Stop if we got fewer items than requested (end of data)
                if len(batch_ids) < current_limit:
                    break

            except Exception as e:
                print(f"Error fetching items batch at offset {offset}: {e}")
                break

        return collected_items[:limit] if limit else collected_items

    def get_item(self, token, item_id, attrs=None):
        self._auth(token)
        params = {"attributes": attrs} if attrs else {}
        return self._req("GET", f"/items/{item_id}", params=params)

    def get_desc(self, token, item_id):
        self._auth(token)
        try:
            return self._req("GET", f"/items/{item_id}/description")
        except Exception as e:
            return {"plain_text": "N/A", "error": str(e)}

    def get_reviews(self, token, item_id):
        self._auth(token)
        try:
            return self._req("GET", f"/reviews/item/{item_id}")
        except Exception as e:
            return {
                "rating_average": 0,
                "total_reviews": 0,
                "reviews": [],
                "error": str(e),
            }

    def get_questions(self, token, item_id, limit=10):
        self._auth(token)
        try:
            params = {"item_id": item_id, "limit": limit}
            return self._req("GET", "/questions/search", params=params)
        except Exception as e:
            return {"questions": [], "total": 0, "error": str(e)}

    def get_orders(
        self,
        token: str,
        seller_id: str,
        *,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        sort: str = "date_created",
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Hits /orders/search with full filter/sort/pagination support.
        Returns the raw JSON (including `results`, `paging`, etc.) for callers to
        page through as needed.
        """
        # Ensure we have a valid bearer token on the session
        self._auth(token)

        # Build query parameters
        params: Dict[str, Any] = {
            "seller": seller_id,
            "limit": limit,
            "offset": offset,
            "sort": "date_asc" if sort == "date_created" else sort,
        }
        if date_from:
            params["order.date_created.from"] = date_from
        if date_to:
            params["order.date_created.to"] = date_to

        # Delegate to the shared request method
        response = self._req("GET", "/orders/search", params=params)

        # response is expected to include both "results" and "paging"
        return response

    def get_listing_types(self, token, site_id):
        self._auth(token)
        return self._req("GET", f"/sites/{site_id}/listing_types")

    def get_listing_exposures(self, token, site_id):
        self._auth(token)
        return self._req("GET", f"/sites/{site_id}/listing_exposures")

    def search(
        self,
        token,
        site_id,
        query=None,
        seller_id=None,
        category=None,
        limit=50,
        offset=0,
    ):
        # Authenticate immediately
        self._auth(token)

        # Always build the same param dictionary
        params = {"limit": limit, "offset": offset}
        if query:
            params["q"] = query
        if seller_id:
            params["seller_id"] = seller_id
        if category:
            params["category"] = category

        # 1) Try the generic /sites/{site_id}/search
        try:
            return self._req("GET", f"/sites/{site_id}/search", params=params)
        except Exception as e:
            # If it fails with 403 or 401, fall back to "items by seller" if we can
            msg = str(e).lower()
            if "403" in msg or "401" in msg:
                # If caller already passed seller_id, just re‐raise (nothing else to try)
                if seller_id:
                    raise

                # Otherwise, fetch the user's own ID and do /users/{id}/items/search
                user = self.get_user(token)  # returns JSON with "id" field
                fallback_seller = user["id"]
                return self.get_items(token, fallback_seller, limit=limit)
            # For any other exception, re‐raise
            raise

    def get_categories(self, token, site_id):
        try:
            return self._req("GET", f"/sites/{site_id}/categories")
        except Exception:
            return self._req("GET", f"/sites/{site_id}/categories")

    def get_category(self, token, category_id):
        try:
            return self._req("GET", f"/categories/{category_id}")
        except Exception:
            return self._req("GET", f"/categories/{category_id}")

    def get_trends(self, token, site_id, category_id=None):
        endpoint = f"/trends/{site_id}"
        if category_id:
            endpoint += f"/{category_id}"
        return self._req("GET", endpoint, headers={"Authorization": f"Bearer {token}"})

    def validate_item(self, token, item_data):
        self._auth(token)
        try:
            resp = self.session.post(
                f"{cfg.api_url}/items/validate",
                json=item_data,
                headers=self.session.headers,
                timeout=cfg.timeout,
            )
            return {
                "valid": resp.status_code == 204,
                "errors": resp.json() if resp.status_code != 204 else None,
            }
        except Exception as e:
            return {"valid": False, "errors": str(e)}


# Token management
def save_tokens(tokens):
    tokens["expires_at"] = (
        datetime.now() + timedelta(seconds=tokens["expires_in"])
    ).isoformat()
    with open(cfg.token_file, "w") as f:
        json.dump(tokens, f)


def load_tokens():
    if os.path.exists(cfg.token_file):
        with open(cfg.token_file) as f:
            return json.load(f)
    return {
        "access_token": cfg.fallback_access,
        "token_type": "Bearer",
        "expires_in": 21600,
        "refresh_token": cfg.fallback_refresh,
        "expires_at": cfg.fallback_expires,
    }


def is_valid(tokens):
    if not tokens:
        return False
    expires_at = datetime.fromisoformat(tokens["expires_at"])
    return datetime.now() < expires_at - timedelta(minutes=5)


def refresh_token(refresh_token):
    resp = requests.post(
        f"{cfg.api_url}/oauth/token",
        data={
            "grant_type": "refresh_token",
            "client_id": cfg.client_id,
            "client_secret": cfg.client_secret,
            "refresh_token": refresh_token,
        },
        timeout=cfg.timeout,
    )
    resp.raise_for_status()
    return resp.json()


def get_token():
    tokens = load_tokens()

    if tokens and is_valid(tokens):
        return tokens["access_token"]

    if tokens and "refresh_token" in tokens:
        try:
            new_tokens = refresh_token(tokens["refresh_token"])
            save_tokens(new_tokens)
            return new_tokens["access_token"]
        except Exception:
            pass

    return load_tokens()["access_token"]


def create_client():
    return MLClient(), get_token()
