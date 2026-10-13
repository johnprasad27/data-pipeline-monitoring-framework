"""
API client for fetching and normalizing sales data from a REST API.

This client supports two modes:
 - Real API mode: fetches JSON from `config.API_BASE_URL` using `requests`.
 - Mock mode: when `API_BASE_URL` is empty, loads `data/json/sample_api_response.json`.

Special handling is provided for the DummyJSON carts API (https://dummyjson.com/carts):
it converts each cart's products into separate sales records and generates
customer names like "Customer {cart_id}". The returned DataFrame follows the
`sales_data` table schema: customer_name, product_name, quantity, amount, sale_date.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests

import config

logger = logging.getLogger(__name__)


class APIError(Exception):
    """Exception raised for API related errors."""


class APIClient:
    """
    API client to fetch JSON data and convert it into a DataFrame
    matching the `sales_data` schema.

    Methods
    -------
    fetch_data() -> Any
        Retrieve raw JSON payload from the API or mock file.

    get_dataframe(payload) -> pd.DataFrame
        Normalize raw payload into a DataFrame with columns:
        `customer_name, product_name, quantity, amount, sale_date`.
    """

    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = base_url if base_url is not None else config.API_BASE_URL
        self.mock_file = Path("data/json/sample_api_response.json")

    def fetch_data(self) -> Any:
        """
        Fetch raw JSON from the configured API or from mock file when
        `API_BASE_URL` is empty.

        Returns
        -------
        Any
            Parsed JSON payload (dict or list) from the source.

        Raises
        ------
        APIError
            On network errors, timeouts, HTTP errors or invalid JSON.
        """
        # Mock mode
        if not self.base_url:
            logger.info("API_BASE_URL not configured; loading mock data: %s", self.mock_file)
            try:
                with self.mock_file.open("r", encoding="utf-8") as fh:
                    payload = json.load(fh)
                logger.debug("Loaded mock payload (type=%s)", type(payload))
                return payload
            except FileNotFoundError as e:
                logger.error("Mock file not found: %s", e)
                raise APIError(f"Mock file not found: {e}") from e
            except json.JSONDecodeError as e:
                logger.error("Invalid JSON in mock file: %s", e)
                raise APIError(f"Invalid JSON in mock file: {e}") from e
            except Exception as e:
                logger.exception("Unexpected error loading mock data")
                raise APIError(f"Unexpected error loading mock data: {e}") from e

        # Real API mode
        try:
            logger.info("Fetching data from API: %s", self.base_url)
            resp = requests.get(self.base_url, timeout=15)
            resp.raise_for_status()
            payload = resp.json()
            logger.debug("Received payload from API (type=%s)", type(payload))
            return payload
        except requests.Timeout as e:
            logger.error("API request timed out: %s", e)
            raise APIError(f"API request timed out: {e}") from e
        except requests.ConnectionError as e:
            logger.error("Connection error when calling API: %s", e)
            raise APIError(f"Connection error: {e}") from e
        except requests.HTTPError as e:
            logger.error("HTTP error from API: %s", e)
            raise APIError(f"HTTP error: {e}") from e
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON received from API: %s", e)
            raise APIError(f"Invalid JSON from API: {e}") from e
        except Exception as e:
            logger.exception("Unexpected error during API fetch")
            raise APIError(f"Unexpected error during API fetch: {e}") from e

    def get_dataframe(self, payload: Any) -> pd.DataFrame:
        """
        Normalize various API payload shapes into a DataFrame compatible with
        the `sales_data` table.

        Supported payloads:
        - DummyJSON carts API response (dict with 'carts' list)
        - List of sales records (mock file format)

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        APIError
            If the payload cannot be normalized.
        """
        try:
            records: List[Dict[str, Any]] = []

            # Case 1: DummyJSON /carts -> payload is dict with 'carts'
            if isinstance(payload, dict) and "carts" in payload and isinstance(payload["carts"], list):
                logger.info("Normalizing DummyJSON carts payload into sales records")
                for cart in payload["carts"]:
                    cart_id = cart.get("id")
                    customer_name = f"Customer {cart_id}"
                    cart_products = cart.get("products", [])
                    for prod in cart_products:
                        records.append({
                            "customer_name": customer_name,
                            "product_name": prod.get("title") or prod.get("product") or "",
                            "quantity": prod.get("quantity", 0),
                            "amount": prod.get("price", 0.0),
                            "sale_date": date.today().isoformat(),
                        })

            # Case 2: DummyJSON /products or dict wrapping 'products', 'items', 'data', or 'results'
            elif isinstance(payload, dict) and any(
                k in payload and isinstance(payload[k], list)
                for k in ("products", "items", "data", "results")
            ):
                list_key = next(
                    k for k in ("products", "items", "data", "results")
                    if k in payload and isinstance(payload[k], list)
                )
                logger.info("Normalizing payload list under '%s' into sales records", list_key)
                for index, item in enumerate(payload[list_key], start=1):
                    if isinstance(item, dict) and all(k in item for k in ("customer_name", "product_name", "quantity", "amount", "sale_date")):
                        records.append({
                            "customer_name": item["customer_name"],
                            "product_name": item["product_name"],
                            "quantity": item["quantity"],
                            "amount": item["amount"],
                            "sale_date": item["sale_date"],
                        })
                    elif isinstance(item, dict):
                        item_id = item.get("id", index)
                        records.append({
                            "customer_name": item.get("customer_name") or f"Customer {item_id}",
                            "product_name": item.get("title") or item.get("product_name") or item.get("product") or item.get("name") or "",
                            "quantity": max(1, int(item.get("quantity") or item.get("minimumOrderQuantity") or 1)),
                            "amount": max(0.0, float(item.get("price") if item.get("price") is not None else item.get("amount") or 0.0)),
                            "sale_date": item.get("sale_date") or date.today().isoformat(),
                        })

            # Case 3: Payload is already a list of sales-like records (mock file)
            elif isinstance(payload, list):
                logger.info("Payload is a list; attempting to coerce to sales schema")
                for item in payload:
                    # If item already has required keys, use them directly
                    if all(k in item for k in ("customer_name", "product_name", "quantity", "amount", "sale_date")):
                        records.append({
                            "customer_name": item["customer_name"],
                            "product_name": item["product_name"],
                            "quantity": item["quantity"],
                            "amount": item["amount"],
                            "sale_date": item["sale_date"],
                        })
                    else:
                        # Try to map common fields
                        records.append({
                            "customer_name": item.get("customer_name", "Customer 0"),
                            "product_name": item.get("product_name") or item.get("title") or "",
                            "quantity": item.get("quantity", 0),
                            "amount": item.get("amount") or item.get("price", 0.0),
                            "sale_date": item.get("sale_date", date.today().isoformat()),
                        })

            else:
                logger.error("Unsupported payload shape: %s", type(payload))
                raise APIError("Unsupported payload shape for conversion to DataFrame")

            df = pd.DataFrame.from_records(records)
            logger.info("Normalized data into DataFrame (%d rows, %d cols)", df.shape[0], df.shape[1])
            return df

        except Exception as e:
            logger.exception("Failed to convert API payload to DataFrame")
            raise APIError(f"Failed to convert API payload to DataFrame: {e}") from e
