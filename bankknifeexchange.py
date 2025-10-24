#!/usr/bin/env python3
"""
bankknifeexchange.py
-------------------------------------------------------------
Modes:
  --mode client   (default) CLI client for mock interchange Resource & AuthZ APIs
  --mode server   Run as mock interchange Resource + AuthZ server

# Note: very experimental & pro forma, best not to use yet.
"""

import argparse
import json
import os
import sys
import urllib.request
import urllib.error
import urllib.parse
import hashlib
import base64
import secrets
import webbrowser
import threading
import time
import uuid
import sqlite3
import uvicorn
from fastapi import FastAPI, HTTPException, Request

BASE_URL = "http://localhost:9090"
AUTHZ_URL = "http://localhost:8080"

# ---------------------------------------------------------------------
# HTTP Client Helper
# ---------------------------------------------------------------------

def make_request(method, base, path, headers=None, data=None, form_encoded=False, fdx_headers=False):
    """Perform an HTTP request and return parsed JSON."""
    url = f"{base}{path}"
    hdrs = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    if fdx_headers:
        hdrs.update({
            "x-fdx-api-version": "6.0",
            "x-fapi-interaction-id": str(uuid.uuid4()),
            "x-fapi-customer-ip-address": "127.0.0.1"
        })
    if form_encoded:
        hdrs["Content-Type"] = "application/x-www-form-urlencoded"
    if headers:
        hdrs.update(headers)
    body = None
    if data is not None:
        if form_encoded:
            body = urllib.parse.urlencode(data).encode("utf-8")
        else:
            body = json.dumps(data).encode("utf-8")

    req = urllib.request.Request(url, data=body, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read().decode("utf-8")
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return {"raw": raw}
    except urllib.error.HTTPError as e:
        try:
            err = e.read().decode("utf-8")
            return {"error": e.code, "body": json.loads(err)}
        except Exception:
            return {"error": e.code, "body": e.reason}
    except urllib.error.URLError as e:
        return {"error": "Network", "body": str(e.reason)}


def print_json(obj):
    print(json.dumps(obj, indent=2, ensure_ascii=False))


# ---------------------------------------------------------------------
# PKCE Utilities
# ---------------------------------------------------------------------

def generate_pkce_pair():
    code_verifier = base64.urlsafe_b64encode(os.urandom(40)).decode("utf-8").rstrip("=")
    code_challenge = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode("utf-8")).digest()
    ).decode("utf-8").rstrip("=")
    return code_verifier, code_challenge


# ---------------------------------------------------------------------
# Resource Client Commands
# ---------------------------------------------------------------------

def user_create(args):
    payload = {"userId": args.userId, "password": args.password}
    res = make_request("POST", args.base_url, "/user", {"Authorization": args.token}, payload, fdx_headers=args.fdx_headers)
    print_json(res)

def user_get(args):
    res = make_request("GET", args.base_url, f"/user/{args.userId}", {"Authorization": args.token}, fdx_headers=args.fdx_headers)
    print_json(res)

def consent_create(args):
    payload = {"userId": args.userId, "accountIds": args.accountIds, "consentShareDurationSeconds": args.duration}
    res = make_request("POST", args.base_url, "/consent", {"Authorization": args.token}, payload, fdx_headers=args.fdx_headers)
    print_json(res)

def consent_get(args):
    res = make_request("GET", args.base_url, f"/consent/{args.consentId}", {"Authorization": args.token}, fdx_headers=args.fdx_headers)
    print_json(res)

def consent_update(args):
    payload = {"userId": args.userId, "accountIds": args.accountIds, "consentShareDurationSeconds": args.duration}
    res = make_request("PUT", args.base_url, f"/consent/{args.consentId}", {"Authorization": args.token}, payload, fdx_headers=args.fdx_headers)
    print_json(res)

def accounts_list(args):
    res = make_request("GET", args.base_url, "/fdx/v6/accounts", {"Authorization": args.token}, fdx_headers=args.fdx_headers)
    print_json(res)

def account_details(args):
    res = make_request("GET", args.base_url, f"/fdx/v6/accounts/{args.accountId}", {"Authorization": args.token}, fdx_headers=args.fdx_headers)
    print_json(res)

def transactions_list(args):
    res = make_request("GET", args.base_url, f"/fdx/v6/accounts/{args.accountId}/transactions", {"Authorization": args.token}, fdx_headers=args.fdx_headers)
    print_json(res)

def upload_accounts(args):
    with open(args.file, "r", encoding="utf-8") as f:
        data = json.load(f)
    res = make_request("POST", args.base_url, f"/upload/addAccount?userId={args.userId}", data=data, fdx_headers=args.fdx_headers)
    print_json(res)

def upload_transactions(args):
    with open(args.file, "r", encoding="utf-8") as f:
        data = json.load(f)
    res = make_request("POST", args.base_url, "/upload/addTransactions", data=data, fdx_headers=args.fdx_headers)
    print_json(res)


# ---------------------------------------------------------------------
# Authorization Client Commands
# ---------------------------------------------------------------------

def authz_register(args):
    with open(args.file, "r", encoding="utf-8") as f:
        payload = json.load(f)
    res = make_request("POST", args.authz_base, "/fdx/v6/register", data=payload, fdx_headers=args.fdx_headers)
    print_json(res)

def authz_token(args):
    payload = {
        "client_id": args.client_id,
        "grant_type": args.grant_type,
        "scope": args.scope,
    }
    if args.client_assertion:
        payload["client_assertion"] = args.client_assertion
        payload["client_assertion_type"] = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
    if args.redirect_uri:
        payload["redirect_uri"] = args.redirect_uri
    if args.code:
        payload["code"] = args.code
        payload["code_verifier"] = args.code_verifier
    if args.refresh_token:
        payload["refresh_token"] = args.refresh_token
    res = make_request("POST", args.authz_base, "/oauth2/token", data=payload, fdx_headers=args.fdx_headers)
    print_json(res)

def authz_introspect(args):
    form = {
        "client_id": args.client_id,
        "client_assertion": args.client_assertion,
        "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        "token": args.token_value,
        "token_type_hint": args.token_type_hint,
    }
    res = make_request("POST", args.authz_base, "/oauth2/introspect", data=form, form_encoded=True, fdx_headers=args.fdx_headers)
    print_json(res)

def authz_par(args):
    form = {
        "authorization_details": args.authorization_details,
        "client_assertion": args.client_assertion,
        "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        "client_id": args.client_id,
        "grant_type": args.grant_type,
        "request": args.request,
        "response_type": args.response_type,
    }
    res = make_request("POST", args.authz_base, "/oauth2/par", data=form, form_encoded=True, fdx_headers=args.fdx_headers)
    print_json(res)

def authz_token_exchange(base, client_id, code, code_verifier, redirect_uri, fdx_headers=False):
    payload = {
        "client_id": client_id,
        "grant_type": "authorization_code",
        "code": code,
        "code_verifier": code_verifier,
        "redirect_uri": redirect_uri,
    }
    print("\n--- Exchanging authorization code for token ---")
    res = make_request("POST", base, "/oauth2/token", data=payload, fdx_headers=fdx_headers)
    print_json(res)
    return res

def authz_authorize(args):
    code_verifier, code_challenge = generate_pkce_pair()
    redirect_uri = args.redirect_uri or (
        "http://localhost:8085/callback" if args.redirect_mode == "auto"
        else "https://oauth.pstmn.io/v1/browser-callback"
    )
    params = {
        "client_id": args.client_id,
        "response_type": "code",
        "scope": args.scope,
        "redirect_uri": redirect_uri,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "state": secrets.token_hex(8),
    }
    auth_url = f"{args.authz_base}/oauth2/authorize?" + urllib.parse.urlencode(params)
    print("\n=== Authorization Request ===")
    print(f"Auth URL:\n{auth_url}\n")
    print(f"Code Verifier: {code_verifier}")
    print(f"Redirect Mode: {args.redirect_mode}")
    print("=============================\n")

    if args.redirect_mode == "manual":
        print("Open the above URL in your browser and paste the 'code':")
        code = input("Enter authorization code: ").strip()
        print_json({"authorization_code": code, "code_verifier": code_verifier})
        if args.auto_exchange:
            authz_token_exchange(args.authz_base, args.client_id, code, code_verifier, redirect_uri, fdx_headers=args.fdx_headers)
        return

    try:
        from fastapi import FastAPI, Request
        import uvicorn
    except ImportError:
        print("FastAPI + Uvicorn required for auto mode. Install with: pip install fastapi uvicorn")
        sys.exit(1)

    app = FastAPI()
    received = {}

    @app.get("/callback")
    async def callback(req: Request):
        code = req.query_params.get("code")
        received["code"] = code
        return {"status": "received", "code": code}

    def run_server():
        uvicorn.run(app, host="0.0.0.0", port=8085, log_level="error")

    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()

    time.sleep(1)
    webbrowser.open(auth_url)
    print("Waiting for redirect to capture code...")
    while "code" not in received:
        time.sleep(0.3)
    code = received["code"]
    print_json({"authorization_code": code, "code_verifier": code_verifier})
    if args.auto_exchange:
        authz_token_exchange(args.authz_base, args.client_id, code, code_verifier, redirect_uri, fdx_headers=args.fdx_headers)


# ---------------------------------------------------------------------
# SERVER MODE
# ---------------------------------------------------------------------

def build_fdx_app(data_source="sqlite:///fdx.db"):
    """
    Builds the interchange mock server app based on the specified data source
    (SQLite, JSON, or in-memory).
    """
    app = FastAPI(title="Financial Interchange Mock Server", version="6.0")

    # Handle SQLite data source
    if data_source.startswith("sqlite:///"):
        db_path = data_source.replace("sqlite:///", "")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE IF NOT EXISTS accounts (id TEXT PRIMARY KEY, name TEXT, type TEXT, balance REAL)")
        conn.execute("CREATE TABLE IF NOT EXISTS transactions (id TEXT PRIMARY KEY, account_id TEXT, amount REAL, description TEXT)")
        conn.commit()

        def load_accounts():
            cur = conn.cursor()
            cur.execute("SELECT id,name,type,balance FROM accounts")
            return [{"id": i, "name": n, "type": t, "balance": b} for (i, n, t, b) in cur.fetchall()]

        def save_accounts(accs):
            cur = conn.cursor()
            for a in accs:
                cur.execute("INSERT OR REPLACE INTO accounts VALUES (?,?,?,?)", (a["id"], a["name"], a["type"], a["balance"]))
            conn.commit()

        def load_transactions(account_id=None):
            cur = conn.cursor()
            if account_id:
                cur.execute("SELECT id,account_id,amount,description FROM transactions WHERE account_id=?", (account_id,))
            else:
                cur.execute("SELECT id,account_id,amount,description FROM transactions")
            return [{"id": i, "account_id": a, "amount": am, "description": d} for (i, a, am, d) in cur.fetchall()]

        def save_transactions(txs):
            cur = conn.cursor()
            for t in txs:
                cur.execute("INSERT OR REPLACE INTO transactions VALUES (?,?,?,?)", (t["id"], t["account_id"], t["amount"], t["description"]))
            conn.commit()

    # Handle JSON data source (using an in-memory store or file-based JSON)
    elif data_source.startswith("json://"):
        json_file_path = data_source.replace("json://", "")
        if os.path.exists(json_file_path):
            with open(json_file_path, 'r') as f:
                store = json.load(f)
        else:
            store = {"accounts": [], "transactions": []}

        def load_accounts():
            return store.get("accounts", [])

        def save_accounts(accs):
            store["accounts"] = accs
            with open(json_file_path, 'w') as f:
                json.dump(store, f)

        def load_transactions(account_id=None):
            return [tx for tx in store.get("transactions", []) if tx["account_id"] == account_id] if account_id else store.get("transactions", [])

        def save_transactions(txs):
            store["transactions"] = txs
            with open(json_file_path, 'w') as f:
                json.dump(store, f)

    # Handle in-memory data source
    else:
        store = {"accounts": [], "transactions": []}

        def load_accounts(): return store["accounts"]
        def save_accounts(accs): store["accounts"] = accs
        def load_transactions(aid=None): return [t for t in store["transactions"] if t["account_id"] == aid] if aid else store["transactions"]
        def save_transactions(txs): store["transactions"] = txs

    # Define the API routes as per your original logic
    @app.get("/fdx/v6/accounts")
    async def get_accounts():
        return {"accounts": load_accounts()}

    @app.get("/fdx/v6/accounts/{aid}")
    async def get_account(aid: str):
        for a in load_accounts():
            if a["id"] == aid: return a
        raise HTTPException(status_code=404, detail="Account not found")

    @app.get("/fdx/v6/accounts/{aid}/transactions")
    async def get_txs(aid: str):
        return {"transactions": load_transactions(aid)}

    @app.post("/upload/addAccount")
    async def up_acc(req: Request):
        data = await req.json()
        accs = load_accounts()
        accs.extend(data if isinstance(data, list) else [data])
        save_accounts(accs)
        return {"status": "ok", "count": len(data)}

    @app.post("/upload/addTransactions")
    async def up_tx(req: Request):
        data = await req.json()
        txs = load_transactions()
        txs.extend(data if isinstance(data, list) else [data])
        save_transactions(txs)
        return {"status": "ok", "count": len(data)}

    @app.post("/oauth2/token")
    async def token(req: Request):
        body = await req.json()
        return {"access_token": "mock-access-token-" + secrets.token_hex(8), "token_type": "Bearer", "expires_in": 3600, "scope": body.get("scope", "")}

    @app.post("/oauth2/introspect")
    async def introspect(req: Request):
        form = await req.form()
        token = form.get("token")
        return {"active": bool(token and token.startswith("mock-access-token"))}

    @app.get("/")
    async def root(): 
        return {"status": "FDX 6.0 mock server running", "data_source": data_source}

    return app

def run_server(data_source="sqlite:///fdx.db", host="0.0.0.0", port=9090):
    app = build_fdx_app(data_source)
    uvicorn.run(app, host=host, port=port)

def get_app(data_source="sqlite:///fdx.db"):
    return build_fdx_app(data_source)

# ---------------------------------------------------------------------
# CLI Parser
# ---------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Financial Interchange Combined CLI + Server")
    parser.add_argument("--mode", choices=["client", "server"], default="client")
    parser.add_argument("--data-source", default="sqlite:///fdx.db", help="Data source for the server (e.g., sqlite:///path/to/db, json:///path/to/data.json, or in-memory)")
    parser.add_argument("--port", type=int, default=9090)
    parser.add_argument("--base-url", default=BASE_URL)
    parser.add_argument("--authz-base", default=AUTHZ_URL)
    parser.add_argument("--fdx-headers", action="store_true")
    args = parser.parse_args()

    if args.mode == "server":
        run_server(data_source=args.data_source, port=args.port)
        return

    # Full client CLI parser
    parser = argparse.ArgumentParser(description="Financial Interchange Combined CLI Client")
    parser.add_argument("--base-url", default=BASE_URL)
    parser.add_argument("--authz-base", default=AUTHZ_URL)
    parser.add_argument("--fdx-headers", action="store_true", help="Add interchange headers to requests")
    sub = parser.add_subparsers(dest="command", required=True)


if __name__ == "__main__":
    main()


