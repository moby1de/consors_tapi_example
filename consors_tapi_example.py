#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Consorsbank ActiveTrader/TAPI – minimal Python example
======================================================

Example by Markus Jurina – LeverageLens
https://www.jurina.biz

Copyright (c) 2026 Markus Jurina
SPDX-License-Identifier: MIT

Unofficial community example. Not affiliated with or endorsed by Consorsbank.

Purpose
-------
This standalone example demonstrates a deliberately small TAPI workflow:

    ISIN + quantity
      -> login
      -> trading-account selection
      -> SecurityInfo
      -> OTC/direct-trading route selection
      -> QuoteRequest
      -> TOTAL_COSTS_ONLY
      -> optional LIVE AcceptQuote with WITHOUT_VALIDATION
      -> read-only order/depot reconciliation (when LIVE)

LeverageLens-specific strategy logic, product selection, leverage selection,
position sizing, signal handling and exit management are intentionally NOT part
of this example.

OCO / recovery design note (documentation only; NOT implemented here)
--------------------------------------------------------------------
For a stateful trading bot, persist the account/depot, product ISIN, order
number and the current state BEFORE/AFTER every order-changing RPC. After an
uncertain AddOrder/CancelOrder/AcceptQuote response, first reconcile current
holdings and orders directly with the broker; NEVER assume the request failed
and blindly send it again. Distinguish an actual second OCO order number from a
single order appearing twice in a snapshot. If two genuinely active equivalent
OCO orders exist, and their SL/TP/ISIN/quantity are truly equivalent, the
older order may be the one to cancel, BUT only after confirming that the other
still protects the entire position. Check the broker again after cancellation.
A broker may auto-remove an obsolete OCO following a fill; verify rather than
assume. An uncertain cancel must never trigger a blind second mutation.
Likewise, do not block an explicit user-requested exit solely to avoid a
suspected duplicate OCO: bypass stale *local* sync flags if necessary, while
still requiring fresh broker/account/position/order checks before any sale.
A working OCO recovery manager needs durable state, order-model fields and
broker-specific reconciliation; deliberately omitted from this minimal demo.

SAFETY
------
* Default mode does NOT send a live order.
* A live order is sent only when BOTH --live and --confirm-live are supplied.
* Immediately before a live AcceptQuote, a fresh quote is requested.
* A live AcceptQuote is NEVER retried automatically after a timeout or transport
  error. In that situation, check the broker account/order list before doing
  anything else. Blind retries can create duplicate orders.
* LIVE SELL is refused unless a fresh broker depot snapshot confirms sufficient
  holdings, and a fresh orders snapshot reveals no conflicting/unclear SELL order.
  This example NEVER cancels an existing protective order automatically.
* The immediate AcceptQuote reply is not sufficient proof of an executed fill:
  the broker may report EXECUTED with executed_amount=0/execution_quote=NaN.
  LIVE orders are followed by a read-only account order/depot check, including
  a comparison with the pre-trade inventory. Concurrent trades of the same
  instrument need a more sophisticated reconciliation than this example.
* A snapshot timeout or an ambiguous status is NOT permission to retry a LIVE
  order. Verify manually in ActiveTrader if reconciliation stays uncertain.
* The TAPI secret is read from CONSORS_TAPI_SECRET or requested via a hidden
  prompt. It is never written to disk by this script.
* Real trading can cause financial loss. This is technical example code, not
  investment advice. Use only after reviewing the code and the broker's current
  documentation/terms.

Typical use
-----------
Safe test (quote + MiFID-II total-cost request; no order):

    python consors_tapi_example.py --isin DE000ABC1234 --qty 1

Explicit live order:

    python consors_tapi_example.py --isin DE000ABC1234 --qty 1 \\
        --live --confirm-live

Dependencies:

    pip install grpcio protobuf

The local ActiveTrader/TAPI service must be running. Place the broker-provided
roots.pem next to this script or pass --cert PATH.
"""

from __future__ import annotations

import argparse
import getpass
import math
import os
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import grpc
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
except Exception as exc:  # pragma: no cover - user-facing dependency error
    raise SystemExit(
        "Missing dependency. Install with: pip install grpcio protobuf\n"
        f"Import error: {exc}"
    )


PACKAGE = "com.consorsbank.module.tapi.grpc"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 40443
DEFAULT_EXCHANGE = "OTC"
DEFAULT_TIMEOUT = 5.0

# TAPI Validation enum values used by this example.
WITHOUT_VALIDATION = 0
TOTAL_COSTS_ONLY = 4

# TAPI OrderType enum values used by this example.
BUY = 1
SELL = 2


# -----------------------------------------------------------------------------
# Minimal dynamic protobuf schema
# -----------------------------------------------------------------------------
# This example intentionally contains only the message fields needed for the
# workflow above. Unknown protobuf fields sent by the backend are ignored.


class TapiSchema:
    def __init__(self) -> None:
        self.pool = descriptor_pool.DescriptorPool()
        self.classes: Dict[str, Any] = {}
        self._build()

    @staticmethod
    def _enum(container: Any, name: str, values: List[Tuple[str, int]]) -> Any:
        enum = container.enum_type.add()
        enum.name = name
        for item_name, item_value in values:
            value = enum.value.add()
            value.name = item_name
            value.number = int(item_value)
        return enum

    @staticmethod
    def _message(fd: Any, name: str) -> Any:
        msg = fd.message_type.add()
        msg.name = name
        return msg

    @staticmethod
    def _field(
        msg: Any,
        name: str,
        number: int,
        field_type: int,
        *,
        repeated: bool = False,
        type_name: Optional[str] = None,
    ) -> None:
        field = msg.field.add()
        field.name = name
        field.number = int(number)
        field.type = int(field_type)
        field.label = 3 if repeated else 1  # LABEL_REPEATED / LABEL_OPTIONAL
        if type_name:
            field.type_name = type_name

    def _build(self) -> None:
        FDP = descriptor_pb2.FieldDescriptorProto
        fd = descriptor_pb2.FileDescriptorProto()
        fd.name = "consors_tapi_example.proto"
        fd.package = PACKAGE
        fd.syntax = "proto3"
        P = f".{PACKAGE}."

        self._enum(
            fd,
            "SecurityCodeType",
            [
                ("NO_CODE_TYPE", 0),
                ("WKN", 1),
                ("ISIN", 2),
                ("ID_NOTATION", 3),
                ("ID_OSI", 4),
                ("ID_INSTRUMENT", 5),
                ("MNEMONIC", 6),
                ("MNEMONIC_US", 7),
            ],
        )
        self._enum(
            fd,
            "LimitToken",
            [("LIMIT_AND_QUOTE", 0), ("QUOTE_ONLY", 1), ("LIMIT_ONLY", 2)],
        )
        self._enum(
            fd,
            "OrderType",
            [
                ("NO_ORDER_TYPE", 0),
                ("BUY", 1),
                ("SELL", 2),
                ("SHORT_SELL", 3),
                ("SHORT_COVER", 4),
                ("FORCED_COVER", 5),
            ],
        )
        self._enum(
            fd,
            "Validation",
            [
                ("WITHOUT_VALIDATION", 0),
                ("VALIDATE_ONLY", 1),
                ("VALIDATE_WITH_TOTAL_COSTS", 2),
                ("VALIDATE_WITH_DETAIL_COSTS", 3),
                ("TOTAL_COSTS_ONLY", 4),
            ],
        )
        self._enum(
            fd,
            "OrderStatus",
            [
                ("NO_ORDER_STATUS", 0),
                ("NEW", 1),
                ("OPEN", 2),
                ("EXECUTED", 3),
                ("PARTIALLY_EXECUTED", 4),
                ("CANCELED", 5),
                ("CANCELED_FORCED", 6),
                ("CANCELED_NOTED", 7),
                ("CANCELED_TIMEOUT", 8),
                ("CHANGED", 9),
                ("CHANGING_NOTED", 10),
                ("INACTIVE", 11),
                ("INACTIVE_NOTED", 12),
                ("STORNO", 13),
            ],
        )

        # Generic error
        m = self._message(fd, "Error")
        self._field(m, "code", 1, FDP.TYPE_STRING)
        self._field(m, "message", 2, FDP.TYPE_STRING)

        # Authentication
        m = self._message(fd, "AccessTokenRequest")
        self._field(m, "access_token", 1, FDP.TYPE_STRING)

        m = self._message(fd, "LoginRequest")
        self._field(m, "secret", 1, FDP.TYPE_STRING)

        m = self._message(fd, "LoginReply")
        self._field(m, "access_token", 1, FDP.TYPE_STRING)
        self._field(m, "error", 1000, FDP.TYPE_MESSAGE, type_name=P + "Error")

        m = self._message(fd, "LogoutRequest")
        self._field(m, "access_token", 1, FDP.TYPE_STRING)

        m = self._message(fd, "LogoutReply")
        self._field(m, "error", 1000, FDP.TYPE_MESSAGE, type_name=P + "Error")

        # Security / exchange
        m = self._message(fd, "StockExchange")
        self._field(m, "id", 1, FDP.TYPE_STRING)
        self._field(m, "issuer", 2, FDP.TYPE_STRING)

        m = self._message(fd, "SecurityCode")
        self._field(m, "code", 1, FDP.TYPE_STRING)
        self._field(m, "code_type", 2, FDP.TYPE_ENUM, type_name=P + "SecurityCodeType")

        m = self._message(fd, "SecurityWithStockExchange")
        self._field(m, "security_code", 1, FDP.TYPE_MESSAGE, type_name=P + "SecurityCode")
        self._field(m, "stock_exchange", 2, FDP.TYPE_MESSAGE, type_name=P + "StockExchange")

        m = self._message(fd, "SecurityStockExchangeInfo")
        self._field(m, "stock_exchange", 1, FDP.TYPE_MESSAGE, type_name=P + "StockExchange")
        self._field(m, "buy_limit_token", 2, FDP.TYPE_ENUM, type_name=P + "LimitToken")
        self._field(m, "sell_limit_token", 3, FDP.TYPE_ENUM, type_name=P + "LimitToken")

        m = self._message(fd, "SecurityInfoRequest")
        self._field(m, "access_token", 1, FDP.TYPE_STRING)
        self._field(m, "security_code", 2, FDP.TYPE_MESSAGE, type_name=P + "SecurityCode")

        m = self._message(fd, "SecurityInfoReply")
        self._field(m, "name", 1, FDP.TYPE_STRING)
        self._field(m, "security_codes", 3, FDP.TYPE_MESSAGE, repeated=True, type_name=P + "SecurityCode")
        self._field(
            m,
            "stock_exchange_infos",
            4,
            FDP.TYPE_MESSAGE,
            repeated=True,
            type_name=P + "SecurityStockExchangeInfo",
        )
        self._field(m, "error", 1000, FDP.TYPE_MESSAGE, type_name=P + "Error")

        # Accounts
        self._message(fd, "Empty")
        m = self._message(fd, "TradingAccount")
        self._field(m, "account_number", 1, FDP.TYPE_STRING)
        self._field(m, "depot_number", 2, FDP.TYPE_STRING)
        self._field(m, "name", 3, FDP.TYPE_STRING)
        self._field(m, "tradable", 4, FDP.TYPE_BOOL)

        m = self._message(fd, "TradingAccounts")
        self._field(m, "accounts", 1, FDP.TYPE_MESSAGE, repeated=True, type_name=P + "TradingAccount")
        self._field(m, "error", 1000, FDP.TYPE_MESSAGE, type_name=P + "Error")

        m = self._message(fd, "TradingAccountRequest")
        self._field(m, "access_token", 1, FDP.TYPE_STRING)
        self._field(m, "trading_account", 2, FDP.TYPE_MESSAGE, type_name=P + "TradingAccount")

        # Read-only depot/order snapshots. Field numbers match the TAPI messages
        # used by the production bridge; the demo needs only a subset.
        m = self._message(fd, "DepotEntry")
        self._field(m, "security_code", 1, FDP.TYPE_MESSAGE, type_name=P + "SecurityCode")
        self._field(m, "total_amount", 5, FDP.TYPE_DOUBLE)
        self._field(m, "sell_possible", 6, FDP.TYPE_BOOL)
        self._field(m, "blocked", 10, FDP.TYPE_BOOL)
        self._field(m, "open_sales", 14, FDP.TYPE_DOUBLE)

        m = self._message(fd, "DepotEntries")
        self._field(m, "account", 1, FDP.TYPE_MESSAGE, type_name=P + "TradingAccount")
        self._field(m, "entries", 2, FDP.TYPE_MESSAGE, repeated=True, type_name=P + "DepotEntry")
        self._field(m, "error", 1000, FDP.TYPE_MESSAGE, type_name=P + "Error")

        # Order reply (only fields useful for this example)
        m = self._message(fd, "Order")
        self._field(m, "security_with_stockexchange", 1, FDP.TYPE_MESSAGE, type_name=P + "SecurityWithStockExchange")
        self._field(m, "order_type", 2, FDP.TYPE_ENUM, type_name=P + "OrderType")
        self._field(m, "order_number", 3, FDP.TYPE_STRING)
        self._field(m, "amount", 4, FDP.TYPE_DOUBLE)
        self._field(m, "executed_amount", 8, FDP.TYPE_DOUBLE)
        self._field(m, "order_status", 9, FDP.TYPE_ENUM, type_name=P + "OrderStatus")
        self._field(m, "execution_quote", 20, FDP.TYPE_DOUBLE)
        self._field(m, "unique_id", 21, FDP.TYPE_STRING)

        # The backend may return detailed nested cost fields. They are deliberately
        # not decoded here because the example only needs to know whether the
        # TOTAL_COSTS_ONLY request succeeded.
        self._message(fd, "OrderCosts")

        m = self._message(fd, "OrderReply")
        self._field(m, "account", 1, FDP.TYPE_MESSAGE, type_name=P + "TradingAccount")
        self._field(m, "order", 2, FDP.TYPE_MESSAGE, type_name=P + "Order")
        self._field(m, "order_costs", 3, FDP.TYPE_MESSAGE, type_name=P + "OrderCosts")
        self._field(m, "error", 1000, FDP.TYPE_MESSAGE, type_name=P + "Error")

        m = self._message(fd, "Orders")
        self._field(m, "account", 1, FDP.TYPE_MESSAGE, type_name=P + "TradingAccount")
        self._field(m, "orders", 2, FDP.TYPE_MESSAGE, repeated=True, type_name=P + "Order")
        self._field(m, "error", 9000, FDP.TYPE_MESSAGE, type_name=P + "Error")

        # Quote
        m = self._message(fd, "QuoteRequest")
        self._field(m, "access_token", 1, FDP.TYPE_STRING)
        self._field(m, "security_code", 2, FDP.TYPE_MESSAGE, type_name=P + "SecurityCode")
        self._field(m, "order_type", 3, FDP.TYPE_ENUM, type_name=P + "OrderType")
        self._field(m, "amount", 4, FDP.TYPE_DOUBLE)
        self._field(m, "stock_exchanges", 5, FDP.TYPE_MESSAGE, repeated=True, type_name=P + "StockExchange")

        m = self._message(fd, "QuoteEntry")
        self._field(m, "stock_exchange", 1, FDP.TYPE_MESSAGE, type_name=P + "StockExchange")
        self._field(m, "buy_price", 2, FDP.TYPE_DOUBLE)
        self._field(m, "buy_volume", 3, FDP.TYPE_DOUBLE)
        self._field(m, "sell_price", 4, FDP.TYPE_DOUBLE)
        self._field(m, "sell_volume", 5, FDP.TYPE_DOUBLE)
        self._field(m, "currency", 9, FDP.TYPE_STRING)
        self._field(m, "quote_reference", 10, FDP.TYPE_STRING)
        self._field(m, "order_type", 11, FDP.TYPE_ENUM, type_name=P + "OrderType")
        self._field(m, "error", 1000, FDP.TYPE_MESSAGE, type_name=P + "Error")

        m = self._message(fd, "QuoteReply")
        self._field(m, "security_code", 1, FDP.TYPE_MESSAGE, type_name=P + "SecurityCode")
        self._field(m, "order_type", 2, FDP.TYPE_ENUM, type_name=P + "OrderType")
        self._field(m, "price_entries", 3, FDP.TYPE_MESSAGE, repeated=True, type_name=P + "QuoteEntry")
        self._field(m, "error", 1000, FDP.TYPE_MESSAGE, type_name=P + "Error")

        # AcceptQuote
        m = self._message(fd, "AcceptQuoteRequest")
        self._field(m, "access_token", 1, FDP.TYPE_STRING)
        self._field(m, "account_number", 2, FDP.TYPE_STRING)
        self._field(m, "security_with_stockexchange", 3, FDP.TYPE_MESSAGE, type_name=P + "SecurityWithStockExchange")
        self._field(m, "order_type", 4, FDP.TYPE_ENUM, type_name=P + "OrderType")
        self._field(m, "amount", 5, FDP.TYPE_DOUBLE)
        self._field(m, "limit", 6, FDP.TYPE_DOUBLE)
        self._field(m, "quote_reference", 7, FDP.TYPE_STRING)
        self._field(m, "validation", 8, FDP.TYPE_ENUM, type_name=P + "Validation")
        self._field(m, "risk_class_override", 9, FDP.TYPE_BOOL)
        self._field(m, "target_market_override", 10, FDP.TYPE_BOOL)
        self._field(m, "tax_nontransparent_override", 11, FDP.TYPE_BOOL)
        self._field(m, "accept_additional_fees", 12, FDP.TYPE_BOOL)

        self.pool.Add(fd)
        for name in (
            "Empty",
            "Error",
            "AccessTokenRequest",
            "LoginRequest",
            "LoginReply",
            "LogoutRequest",
            "LogoutReply",
            "StockExchange",
            "SecurityCode",
            "SecurityWithStockExchange",
            "SecurityInfoRequest",
            "SecurityInfoReply",
            "TradingAccount",
            "TradingAccounts",
            "TradingAccountRequest",
            "DepotEntry",
            "DepotEntries",
            "Order",
            "OrderCosts",
            "OrderReply",
            "Orders",
            "QuoteRequest",
            "QuoteReply",
            "AcceptQuoteRequest",
        ):
            descriptor = self.pool.FindMessageTypeByName(f"{PACKAGE}.{name}")
            self.classes[name] = message_factory.GetMessageClass(descriptor)

    def cls(self, name: str) -> Any:
        return self.classes[name]


SCHEMA = TapiSchema()


def error_text(message: Any) -> str:
    try:
        err = message.error
        code = str(getattr(err, "code", "") or "").strip()
        text = str(getattr(err, "message", "") or "").strip()
        return f"{code}: {text}" if code and text else code or text
    except Exception:
        return ""


def enum_name(message: Any, field_name: str) -> str:
    try:
        field = message.DESCRIPTOR.fields_by_name[field_name]
        number = int(getattr(message, field_name))
        value = field.enum_type.values_by_number.get(number)
        return value.name if value else str(number)
    except Exception:
        return "?"


def finite_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except Exception:
        return None
    return number if math.isfinite(number) else None


def normalize_isin(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def valid_isin(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}[0-9]", normalize_isin(value)))


def mask_account(value: str) -> str:
    value = str(value or "")
    if len(value) <= 4:
        return value
    return "*" * max(0, len(value) - 4) + value[-4:]


class ConsorsTapi:
    def __init__(self, host: str, port: int, cert_file: Path, secret: str, timeout: float) -> None:
        self.host = host
        self.port = int(port)
        self.cert_file = Path(cert_file)
        self.secret = secret
        self.timeout = float(timeout)
        self.channel: Optional[Any] = None
        self.access_token = ""

    def _rpc(self, service: str, method: str, request: Any, response_name: str) -> Any:
        if self.channel is None:
            raise RuntimeError("TAPI connection is not open")
        response_cls = SCHEMA.cls(response_name)
        call = self.channel.unary_unary(
            f"/{PACKAGE}.{service}/{method}",
            request_serializer=lambda msg: msg.SerializeToString(),
            response_deserializer=lambda data: response_cls.FromString(data),
        )
        return call(request, timeout=self.timeout)

    def connect(self) -> None:
        if not self.cert_file.is_file():
            raise FileNotFoundError(f"Certificate not found: {self.cert_file}")
        if not self.secret:
            raise RuntimeError("TAPI secret is empty")

        roots = self.cert_file.read_bytes()
        credentials = grpc.ssl_channel_credentials(root_certificates=roots)
        target = f"{self.host}:{self.port}"
        self.channel = grpc.secure_channel(target, credentials)
        grpc.channel_ready_future(self.channel).result(timeout=self.timeout)

        M = SCHEMA.cls
        request = M("LoginRequest")()
        request.secret = self.secret
        reply = self._rpc("AccessService", "Login", request, "LoginReply")
        err = error_text(reply)
        if err:
            raise RuntimeError(f"Login failed: {err}")
        self.access_token = str(reply.access_token or "")
        if not self.access_token:
            raise RuntimeError("Login returned no access token")

    def close(self) -> None:
        if self.channel is not None and self.access_token:
            try:
                M = SCHEMA.cls
                request = M("LogoutRequest")()
                request.access_token = self.access_token
                self._rpc("AccessService", "Logout", request, "LogoutReply")
            except Exception:
                pass
        try:
            if self.channel is not None:
                self.channel.close()
        finally:
            self.channel = None
            self.access_token = ""

    def get_accounts(self) -> List[Dict[str, Any]]:
        M = SCHEMA.cls
        request = M("AccessTokenRequest")()
        request.access_token = self.access_token
        reply = self._rpc("AccountService", "GetTradingAccounts", request, "TradingAccounts")
        err = error_text(reply)
        if err:
            raise RuntimeError(f"GetTradingAccounts: {err}")
        return [
            {
                "account_number": str(item.account_number or ""),
                "depot_number": str(item.depot_number or ""),
                "name": str(item.name or ""),
                "tradable": bool(item.tradable),
            }
            for item in reply.accounts
        ]

    def _trading_account_request(self, account: Dict[str, Any]) -> Any:
        M = SCHEMA.cls
        request = M("TradingAccountRequest")()
        request.access_token = self.access_token
        ta = M("TradingAccount")()
        for field in ("account_number", "depot_number", "name"):
            setattr(ta, field, str(account.get(field) or ""))
        ta.tradable = bool(account.get("tradable"))
        request.trading_account.CopyFrom(ta)
        return request

    def _fresh_snapshot(self, account: Dict[str, Any], *, kind: str) -> Any:
        """One fresh read-only TAPI push snapshot (Stream* + Update*).

        A first initial push is not considered proof of a new Update* response.
        If the stream had no prior update, establish an initial sequence and
        then request a second, newer snapshot. The two Update calls are READ-ONLY.
        """
        if self.channel is None:
            raise RuntimeError("TAPI connection is not open")
        if kind == "depot":
            service, stream_name, update_name, response_name = (
                "DepotService", "StreamDepot", "UpdateDepot", "DepotEntries"
            )
        elif kind == "orders":
            service, stream_name, update_name, response_name = (
                "OrderService", "StreamOrders", "UpdateOrders", "Orders"
            )
        else:
            raise ValueError(f"Unknown snapshot type: {kind}")

        req = self._trading_account_request(account)
        cls = SCHEMA.cls(response_name)
        call = self.channel.unary_stream(
            f"/{PACKAGE}.{service}/{stream_name}",
            request_serializer=lambda msg: msg.SerializeToString(),
            response_deserializer=lambda data: cls.FromString(data),
        )
        stream = call(req, timeout=max(15.0, self.timeout * 4))
        changed = threading.Event()
        received: List[Any] = []
        errors: List[BaseException] = []
        lock = threading.Lock()

        def reader() -> None:
            try:
                for message in stream:
                    with lock:
                        received.append(message)
                    changed.set()
            except BaseException as exc:
                with lock:
                    errors.append(exc)
                changed.set()

        threading.Thread(target=reader, name=f"tapi-{kind}-snapshot", daemon=True).start()

        def await_newer(index: int) -> Tuple[Any, int]:
            until = time.monotonic() + self.timeout
            while True:
                with lock:
                    if errors:
                        raise RuntimeError(f"{kind} stream failed: {errors[-1]}")
                    if len(received) > index:
                        return received[-1], len(received)
                    changed.clear()
                remaining = until - time.monotonic()
                if remaining <= 0 or not changed.wait(remaining):
                    raise TimeoutError(f"{update_name} did not return a fresh broker snapshot")

        try:
            with lock:
                index = len(received)
            if index == 0:
                self._rpc(service, update_name, req, "Empty")
                _, index = await_newer(index)
            self._rpc(service, update_name, req, "Empty")
            reply, _ = await_newer(index)
            err = error_text(reply)
            if err:
                raise RuntimeError(f"{update_name}: {err}")
            reported_account = str(reply.account.account_number or "")
            if reported_account != str(account["account_number"]):
                raise RuntimeError(
                    f"{update_name} returned no matching trading account; refusing ambiguous snapshot"
                )
            return reply
        finally:
            stream.cancel()

    def depot_snapshot(self, account: Dict[str, Any]) -> Any:
        return self._fresh_snapshot(account, kind="depot")

    def orders_snapshot(self, account: Dict[str, Any]) -> Any:
        return self._fresh_snapshot(account, kind="orders")

    def security_info(self, isin: str) -> Any:
        M = SCHEMA.cls
        security_code = M("SecurityCode")()
        security_code.code = isin
        security_code.code_type = 2  # ISIN
        request = M("SecurityInfoRequest")()
        request.access_token = self.access_token
        request.security_code.CopyFrom(security_code)
        reply = self._rpc("SecurityService", "GetSecurityInfo", request, "SecurityInfoReply")
        err = error_text(reply)
        if err:
            raise RuntimeError(f"GetSecurityInfo({isin}): {err}")
        return reply

    def quote(
        self,
        isin: str,
        order_type: int,
        amount: int,
        exchange_id: str,
        issuer: str,
    ) -> Any:
        M = SCHEMA.cls
        security_code = M("SecurityCode")()
        security_code.code = isin
        security_code.code_type = 2  # ISIN

        exchange = M("StockExchange")()
        exchange.id = exchange_id
        exchange.issuer = issuer

        request = M("QuoteRequest")()
        request.access_token = self.access_token
        request.security_code.CopyFrom(security_code)
        request.order_type = int(order_type)
        request.amount = float(amount)
        request.stock_exchanges.add().CopyFrom(exchange)

        reply = self._rpc("OrderService", "GetQuote", request, "QuoteReply")
        err = error_text(reply)
        if err:
            raise RuntimeError(f"GetQuote({isin}): {err}")
        if not reply.price_entries:
            raise RuntimeError(f"GetQuote({isin}): no price entries returned")

        for entry in reply.price_entries:
            same_exchange = str(entry.stock_exchange.id or "").upper() == exchange_id.upper()
            same_issuer = (not issuer) or str(entry.stock_exchange.issuer or "").upper() == issuer.upper()
            if same_exchange and same_issuer:
                entry_error = error_text(entry)
                if entry_error:
                    raise RuntimeError(f"GetQuote({isin}) route error: {entry_error}")
                if not str(entry.quote_reference or ""):
                    raise RuntimeError("Quote has no quote_reference")
                return entry

        raise RuntimeError(f"GetQuote({isin}): no exact route match for {exchange_id}/{issuer or '-'}")

    def accept_quote(
        self,
        *,
        account_number: str,
        isin: str,
        order_type: int,
        amount: int,
        exchange_id: str,
        issuer: str,
        quote_entry: Any,
        validation: int,
        risk_class_override: bool = False,
        target_market_override: bool = False,
        tax_nontransparent_override: bool = False,
        accept_additional_fees: bool = False,
    ) -> Any:
        M = SCHEMA.cls
        security_code = M("SecurityCode")()
        security_code.code = isin
        security_code.code_type = 2

        exchange = M("StockExchange")()
        exchange.id = exchange_id
        exchange.issuer = issuer

        security_with_exchange = M("SecurityWithStockExchange")()
        security_with_exchange.security_code.CopyFrom(security_code)
        security_with_exchange.stock_exchange.CopyFrom(exchange)

        request = M("AcceptQuoteRequest")()
        request.access_token = self.access_token
        request.account_number = account_number
        request.security_with_stockexchange.CopyFrom(security_with_exchange)
        request.order_type = int(order_type)
        request.amount = float(amount)
        request.quote_reference = str(quote_entry.quote_reference or "")
        request.validation = int(validation)
        request.risk_class_override = bool(risk_class_override)
        request.target_market_override = bool(target_market_override)
        request.tax_nontransparent_override = bool(tax_nontransparent_override)
        request.accept_additional_fees = bool(accept_additional_fees)

        price = quote_entry.buy_price if int(order_type) == BUY else quote_entry.sell_price
        request.limit = float(price)

        reply = self._rpc("OrderService", "AcceptQuote", request, "OrderReply")
        err = error_text(reply)
        if err:
            mode = "TOTAL_COSTS_ONLY" if validation == TOTAL_COSTS_ONLY else "WITHOUT_VALIDATION"
            raise RuntimeError(f"AcceptQuote {mode}: {err}")
        return reply


def select_account(accounts: List[Dict[str, Any]], requested: str) -> Dict[str, Any]:
    tradable = [item for item in accounts if item.get("tradable")]
    if not tradable:
        raise RuntimeError("No tradable TAPI account returned")

    requested = str(requested or "").strip()
    if requested:
        for item in tradable:
            if item["account_number"] == requested:
                return item
        raise RuntimeError(f"Requested account {requested} is not available/tradable")

    if len(tradable) == 1:
        return tradable[0]

    masked = ", ".join(mask_account(item["account_number"]) for item in tradable)
    raise RuntimeError(
        "More than one tradable account is available. "
        f"Select one explicitly with --account. Available (masked): {masked}"
    )


def select_route(info: Any, exchange_id: str, issuer_preference: str, order_type: int) -> Dict[str, Any]:
    exchange_id = str(exchange_id or DEFAULT_EXCHANGE).strip().upper()
    issuer_preference = str(issuer_preference or "").strip().upper()
    candidates: List[Dict[str, Any]] = []

    for item in getattr(info, "stock_exchange_infos", []):
        exchange = item.stock_exchange
        current_id = str(exchange.id or "").strip().upper()
        issuer = str(exchange.issuer or "").strip().upper()
        if current_id != exchange_id:
            continue
        # --issuer is a strict constraint, not a fuzzy ranking preference.
        if issuer_preference and issuer != issuer_preference:
            continue
        token = int(item.buy_limit_token if order_type == BUY else item.sell_limit_token)
        if token in (0, 1):  # LIMIT_AND_QUOTE / QUOTE_ONLY
            candidate = {"id": current_id, "issuer": issuer, "limit_token": token}
            if candidate not in candidates:
                candidates.append(candidate)

    if not candidates:
        detail = f" / issuer {issuer_preference}" if issuer_preference else ""
        raise RuntimeError(f"No quote-capable exact route for {exchange_id}{detail}")
    distinct = {(row["id"], row["issuer"]) for row in candidates}
    if len(distinct) > 1:
        available = ", ".join(sorted(issuer or "(unnamed)" for _, issuer in distinct))
        raise RuntimeError(
            "Multiple quote-capable issuers for this exchange. Select --issuer "
            f"explicitly (available: {available})"
        )
    return candidates[0]


def find_isin(info: Any) -> str:
    for code in getattr(info, "security_codes", []):
        if int(code.code_type) == 2:
            return normalize_isin(str(code.code or ""))
    return ""


def security_code_matches(code: Any, info: Any, isin: str) -> bool:
    key = (int(code.code_type), normalize_isin(str(code.code or "")))
    accepted = {(2, isin)}
    accepted.update(
        (int(entry.code_type), normalize_isin(str(entry.code or "")))
        for entry in getattr(info, "security_codes", [])
    )
    return bool(key[1]) and key in accepted


def _depot_entry(depot: Any, info: Any, isin: str) -> Optional[Any]:
    matches = [
        entry for entry in depot.entries
        if security_code_matches(entry.security_code, info, isin)
    ]
    if len(matches) > 1:
        raise RuntimeError("Multiple matching depot entries; cannot determine unique holding")
    return matches[0] if matches else None


def _whole_quantity(value: Any, label: str) -> int:
    number = finite_float(value)
    if number is None or number < 0 or abs(number - round(number)) > 1e-6:
        raise RuntimeError(f"Invalid {label} from broker: {value!r}")
    return int(round(number))


def confirm_sell_is_safe(client: ConsorsTapi, account: Dict[str, Any],
                         info: Any, isin: str, qty: int) -> int:
    """Refuse SELL when holdings or outstanding SELL orders are ambiguous.

    This example cannot automatically handle OCO cancellation. If any existing
    sell/protective order might conflict, handle it manually in ActiveTrader.
    """
    depot = client.depot_snapshot(account)
    orders = client.orders_snapshot(account)
    entry = _depot_entry(depot, info, isin)
    if entry is None:
        raise RuntimeError("LIVE SELL refused: ISIN not present in the broker depot")
    held = _whole_quantity(entry.total_amount, "depot quantity")
    if held < qty or not entry.sell_possible or entry.blocked:
        raise RuntimeError(
            f"LIVE SELL refused: broker shows {held} pieces "
            f"(sell_possible={entry.sell_possible}, blocked={entry.blocked}); need {qty}"
        )
    if (finite_float(entry.open_sales) or 0.0) > 0:
        raise RuntimeError("LIVE SELL refused: broker reports open sales; inspect protective orders")

    for order in orders.orders:
        if int(order.order_type) != SELL:
            continue
        if not security_code_matches(order.security_with_stockexchange.security_code, info, isin):
            continue
        status = int(order.order_status)
        # Includes inactive/noted and uncertain-cancel states; no automatic
        # order mutation until the status is settled in the broker UI.
        if status not in (3, 5, 6, 13):
            number = str(order.order_number or "-")
            raise RuntimeError(
                f"LIVE SELL refused: existing SELL/order protection {number} "
                f"has unresolved status {enum_name(order, 'order_status')}"
            )
    print(f"SELL preflight: broker confirms {held} pieces; no conflicting SELL order")
    return held


def reconcile_after_live(client: ConsorsTapi, account: Dict[str, Any], info: Any,
                         isin: str, reply: Any, *, baseline_qty: int,
                         requested_qty: int, order_type: int) -> bool:
    """Read-only confirmation of order number AND expected inventory movement.

    No automatic resubmission or cancellation. This deliberately requires both
    a broker order with status EXECUTED and a changed depot amount. Concurrent
    trades of the same ISIN require more extensive reconciliation than this demo.
    """
    number = str(reply.order.order_number or "").strip()
    if not number:
        print("RECONCILIATION UNKNOWN: AcceptQuote supplied no order number.", file=sys.stderr)
        return False

    for attempt in range(1, 4):
        try:
            orders = client.orders_snapshot(account)
            depot = client.depot_snapshot(account)
            matches = [order for order in orders.orders if str(order.order_number or "") == number]
            if len(matches) != 1:
                raise RuntimeError(f"Expected one broker order {number}, found {len(matches)}")
            order = matches[0]
            if not security_code_matches(order.security_with_stockexchange.security_code, info, isin):
                raise RuntimeError("Broker order ISIN does not match the requested product")
            entry = _depot_entry(depot, info, isin)
            held = _whole_quantity(entry.total_amount, "depot quantity") if entry else 0
            status = enum_name(order, "order_status")
            expected = baseline_qty + requested_qty if order_type == BUY else baseline_qty - requested_qty
            print(f"Broker check: order {number}, status={status}, "
                  f"depot {baseline_qty} -> {held} pieces (expected {expected})")
            if status == "EXECUTED" and held == expected:
                if finite_float(order.executed_amount) in (None, 0.0) or finite_float(order.execution_quote) in (None, 0.0):
                    print("Note: fill details are 0/NaN in the order reply; depot movement is independently confirmed.")
                print("LIVE broker reconciliation: order EXECUTED and expected depot movement confirmed")
                return True
            print("Order exists, but execution and expected depot movement are not yet both confirmed.")
        except (RuntimeError, TimeoutError, grpc.RpcError) as exc:
            print(f"Read-only broker reconciliation {attempt}/3: {exc}", file=sys.stderr)
            if attempt < 3:
                time.sleep(0.5)
    print("RECONCILIATION UNKNOWN: inspect the order in ActiveTrader. DO NOT retry.", file=sys.stderr)
    return False


def quote_price(quote: Any, order_type: int) -> float:
    value = quote.buy_price if order_type == BUY else quote.sell_price
    price = finite_float(value)
    if price is None or price <= 0:
        raise RuntimeError("Quote returned an invalid price")
    return price


def print_order_reply(reply: Any) -> None:
    order = reply.order
    number = str(getattr(order, "order_number", "") or "")
    status = enum_name(order, "order_status")
    amount = finite_float(getattr(order, "amount", 0.0))
    executed = finite_float(getattr(order, "executed_amount", 0.0))
    execution_quote = finite_float(getattr(order, "execution_quote", 0.0))
    unique_id = str(getattr(order, "unique_id", "") or "")

    print("LIVE response received:")
    print(f"  order_number:    {number or '-'}")
    print(f"  status:          {status}")
    print(f"  amount:          {amount if amount is not None else '-'}")
    print(f"  executed_amount: {executed if executed is not None else '-'}")
    print(f"  execution_quote: {execution_quote if execution_quote not in (None, 0.0) else '-'}")
    print(f"  unique_id:       {unique_id or '-'}")
    print("  Note: this reply alone is NOT a verified fill; a broker check follows.")


def default_cert_path() -> Path:
    return Path(__file__).resolve().with_name("roots.pem")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Unofficial Consorsbank ActiveTrader/TAPI Quote + optional LIVE AcceptQuote example"
    )
    parser.add_argument("--isin", required=True, help="12-character ISIN")
    parser.add_argument("--qty", required=True, type=int, help="Quantity/pieces; chosen explicitly by the user")
    parser.add_argument("--side", choices=("BUY", "SELL"), default="BUY", help="Order side (default: BUY)")
    parser.add_argument("--account", default="", help="Exact trading account number if more than one is available")
    parser.add_argument("--exchange", default=DEFAULT_EXCHANGE, help=f"Exchange id (default: {DEFAULT_EXCHANGE})")
    parser.add_argument("--issuer", default="", help="Exact issuer id; required if multiple quote-capable issuers exist")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"TAPI host (default: {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"TAPI port (default: {DEFAULT_PORT})")
    parser.add_argument("--cert", type=Path, default=default_cert_path(), help="Path to roots.pem")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="RPC timeout in seconds")

    parser.add_argument(
        "--live",
        action="store_true",
        help="Enable the LIVE path. Still requires --confirm-live before an order can be sent.",
    )
    parser.add_argument(
        "--confirm-live",
        action="store_true",
        help="Explicitly confirm that a real order may be sent. Has no effect without --live.",
    )

    # Explicit opt-in override flags. Nothing is silently accepted.
    parser.add_argument("--risk-class-override", action="store_true")
    parser.add_argument("--target-market-override", action="store_true")
    parser.add_argument("--tax-nontransparent-override", action="store_true")
    parser.add_argument("--accept-additional-fees", action="store_true")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    isin = normalize_isin(args.isin)
    if not valid_isin(isin):
        raise SystemExit("Invalid ISIN format")
    if args.qty <= 0:
        raise SystemExit("--qty must be a positive integer")
    if args.port <= 0 or args.port > 65535:
        raise SystemExit("--port must be between 1 and 65535")
    if args.timeout <= 0:
        raise SystemExit("--timeout must be > 0")

    if args.live and not args.confirm_live:
        raise SystemExit(
            "LIVE mode requested but not confirmed. No order was sent.\n"
            "If you really intend to send a real order, add --confirm-live."
        )

    secret = str(os.getenv("CONSORS_TAPI_SECRET", "") or "")
    if not secret:
        secret = getpass.getpass("TAPI secret (hidden; not stored): ").strip()
    if not secret:
        raise SystemExit("No TAPI secret provided")

    order_type = BUY if args.side == "BUY" else SELL
    client = ConsorsTapi(args.host, args.port, args.cert, secret, args.timeout)

    try:
        print(f"Connecting to TAPI at {args.host}:{args.port} ...")
        client.connect()
        print("Connection: OK")

        accounts = client.get_accounts()
        account = select_account(accounts, args.account)
        print(f"Account:    {mask_account(account['account_number'])} (tradable)")

        info = client.security_info(isin)
        returned_isin = find_isin(info)
        if returned_isin and returned_isin != isin:
            raise RuntimeError(f"SecurityInfo returned a different ISIN: {returned_isin}")
        print(f"Security:   {str(info.name or '-')} | {isin}")

        route = select_route(info, args.exchange, args.issuer, order_type)
        print(f"Route:      {route['id']}/{route['issuer'] or '-'} | quote-capable")
        print(f"Side/qty:   {args.side} x {args.qty}")

        # 1) Quote used only for display + TOTAL_COSTS_ONLY.
        quote = client.quote(isin, order_type, args.qty, route["id"], route["issuer"])
        price = quote_price(quote, order_type)
        currency = str(getattr(quote, "currency", "") or "")
        print(f"Quote:      {price:.4f} {currency}".rstrip())
        print(f"Approx.:    {price * args.qty:.2f} {currency}".rstrip())

        print("Preflight:   TOTAL_COSTS_ONLY ...")
        client.accept_quote(
            account_number=account["account_number"],
            isin=isin,
            order_type=order_type,
            amount=args.qty,
            exchange_id=route["id"],
            issuer=route["issuer"],
            quote_entry=quote,
            validation=TOTAL_COSTS_ONLY,
            risk_class_override=args.risk_class_override,
            target_market_override=args.target_market_override,
            tax_nontransparent_override=args.tax_nontransparent_override,
            accept_additional_fees=args.accept_additional_fees,
        )
        print("Preflight:   OK (no order sent)")

        if not args.live:
            print("Result:      SAFE TEST COMPLETE – no live order sent")
            return 0

        # 2) LIVE: before SELL, verify actual holdings + orders at the broker.
        # An active OCO cannot be auto-cancelled by this standalone example.
        if order_type == SELL:
            print("SELL preflight: checking live broker holdings and SELL orders ...")
            baseline_qty = confirm_sell_is_safe(client, account, info, isin, args.qty)
        else:
            # Compare the holdings before and after the BUY: the order reply
            # alone can say EXECUTED while fill amounts are still 0/NaN.
            depot_before = client.depot_snapshot(account)
            entry_before = _depot_entry(depot_before, info, isin)
            baseline_qty = _whole_quantity(entry_before.total_amount, "depot quantity") if entry_before else 0
            print(f"BUY preflight: broker holds {baseline_qty} pieces before this order")

        # Never reuse the preflight quote. Fetch a fresh quote and submit
        # exactly once with WITHOUT_VALIDATION.
        print("LIVE:        fetching a fresh quote ...")
        live_quote = client.quote(isin, order_type, args.qty, route["id"], route["issuer"])
        live_price = quote_price(live_quote, order_type)
        live_currency = str(getattr(live_quote, "currency", "") or "")
        print(f"LIVE quote:  {live_price:.4f} {live_currency}".rstrip())
        print("LIVE:        sending ONE AcceptQuote with WITHOUT_VALIDATION")

        try:
            reply = client.accept_quote(
                account_number=account["account_number"],
                isin=isin,
                order_type=order_type,
                amount=args.qty,
                exchange_id=route["id"],
                issuer=route["issuer"],
                quote_entry=live_quote,
                validation=WITHOUT_VALIDATION,
                risk_class_override=args.risk_class_override,
                target_market_override=args.target_market_override,
                tax_nontransparent_override=args.tax_nontransparent_override,
                accept_additional_fees=args.accept_additional_fees,
            )
        except (grpc.RpcError, TimeoutError) as exc:
            print(
                "\nIMPORTANT: The LIVE RPC ended with a transport/timeout error.\n"
                "The script will NOT retry automatically because the broker may have\n"
                "received the order. Check ActiveTrader/account orders before any\n"
                "manual retry.\n",
                file=sys.stderr,
            )
            raise RuntimeError(f"LIVE AcceptQuote transport/timeout error: {exc}") from exc

        print_order_reply(reply)
        # A broker order snapshot verifies that an order with this number exists;
        # it is not a final execution/fill guarantee. No automatic order retries.
        return 0 if reconcile_after_live(
            client, account, info, isin, reply,
            baseline_qty=baseline_qty, requested_qty=args.qty, order_type=order_type
        ) else 2

    except KeyboardInterrupt:
        print("Interrupted. No automatic retry will be performed.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        client.close()
        # Best effort: remove the local reference as soon as the session is closed.
        secret = ""


if __name__ == "__main__":
    raise SystemExit(main())
