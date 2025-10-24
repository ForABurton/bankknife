#!/usr/bin/env python3
"""
bankknife.py
--------------------------------------------------
Convert pseudo CSV (such as BofA credit card or account, but they even vary within a bank customer to customer), QIF, QFX and other data into Quicken-compatible CSV, plus more. Originally written for BofA files and a Quicken-like spec as of early Oct '25, a disruptive age of changing institutional OFX connector validation and authentication capability.

"""

import argparse
import csv
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from statistics import mean
import gzip

# ----------------- CONSTANTS -----------------
QUICKEN_COLUMNS = [
    "Date", "Payee", "FI Payee", "Amount", "Debit/Credit",
    "Category", "Account", "Tag", "Memo", "Chknum"
]
REQUIRED_COLUMNS = ["Date", "Payee", "Amount", "Account"]

DEFAULT_RULES = {
    "category": [
        {"pattern": r"(?i)\bGROCERY\s*STORE|SUPERMARKET|FARMERS\s*MARKET|DISCOUNT\s*STORE|RETAIL\s*GROCERY", "value": "Groceries"},
        {"pattern": r"(?i)\bGAS\s*STATION|FUEL|PETROL|CAR\s*SUPPLIES|MOTOR\s*OIL|VEHICLE\s*MAINTENANCE", "value": "Auto:Fuel"},
        {"pattern": r"(?i)\bRIDE\s*SHARE|CAR\s*SERVICE|ON\s*DEMAND\s*TRANSPORT|TAXI|PUBLIC\s*TRANSPORT", "value": "Transportation:Rideshare"},\
        {"pattern": r"(?i)\bELECTRICITY|WATER\s*BILL|GAS\s*BILL|TELECOMMUNICATIONS|INTERNET\s*SERVICE|UTILITY\s*BILL", "value": "Utilities"},
        {"pattern": r"(?i)\bONLINE\s*STORE|E-COMMERCE|RETAIL\s*STORE|TECH\s*STORE|HOME\s*IMPROVEMENT|FURNITURE\s*STORE", "value": "Shopping:Online"},
        {"pattern": r"(?i)\bRESTAURANT|FAST\s*FOOD|DINING\s*OUT|TAKEOUT|FOOD\s*DELIVERY|CATERING", "value": "Dining:Restaurants"},
        {"pattern": r"(?i)\bGYM|FITNESS\s*CENTER|HEALTH\s*CLUB|SPORTS\s*SUPPLIES|WELLNESS\s*SERVICE|PHARMACY", "value": "Health:Fitness"},
        {"pattern": r"(?i)\bAIRLINE|FLIGHT|HOTEL|VACATION\s*BOOKING|CAR\s*RENTAL|TRAVEL\s*SERVICE", "value": "Travel:FlightsAndHotels"},
        {"pattern": r"(?i)\bTAX\s*SERVICE|TAX\s*PREPARATION|FINANCIAL\s*ADVISOR|ACCOUNTING\s*SERVICE|FINANCIAL\s*PLANNER", "value": "Taxes:TaxPrep"},
        {"pattern": r"(?i)\bONLINE\s*LEARNING|EDUCATION\s*PLATFORM|COURSE\s*SUBSCRIPTION|TUTORING|CERTIFICATION\s*COURSES", "value": "Education:Learning"},
        {"pattern": r"(?i)\bSOFTWARE\s*SUBSCRIPTION|OFFICE\s*SUPPLIES|CLOUD\s*STORAGE|COLLABORATION\s*TOOLS|DOCUMENT\s*SHARING", "value": "Office:Subscriptions"},
        {"pattern": r"(?i)\bHOME\s*IMPROVEMENT|HARDWARE\s*STORE|PAINT\s*SUPPLIES|PLUMBING|ELECTRICAL\s*SUPPLIES|GARDENING\s*SUPPLIES", "value": "Home:Improvement"},
        {"pattern": r"(?i)\bDEPARTMENT\s*STORE|CLOTHING\s*STORE|CONSUMER\s*ELECTRONICS|HOUSEHOLD\s*ITEMS|CLEANING\s*SUPPLIES", "value": "Shopping:DepartmentStores"},
        {"pattern": r"(?i)\bPET\s*SUPPLIES|PET\s*STORE|ANIMAL\s*CARE|PET\s*FOOD|PET\s*ACCESSORIES", "value": "Pets:PetSupplies"},
        {"pattern": r"(?i)\bBANK|CREDIT\s*UNION|FINANCIAL\s*INSTITUTION|CHECKING\s*ACCOUNT|SAVINGS\s*ACCOUNT", "value": "Banking:Deposits"},
    ],
    
    "tag": [
        {"pattern": r"(?i)\bSUBSCRIPTION|MONTHLY|AUTOPAY|RECURRING\s*CHARGES", "value": "recurring"},
        {"pattern": r"(?i)\bREFUND|CREDIT|REBATE|RETURN", "value": "refund"},
        {"pattern": r"(?i)\bGIFT\s*CARD|LOYALTY\s*POINTS|REWARD\s*POINTS", "value": "gift"},
        {"pattern": r"(?i)\bPAYMENT\s*PLAN|INSTALLMENT\s*PAYMENT", "value": "payment_plan"},
    ],
    
    "filter": [  # 🧹 default filter rule
        {"pattern": r"(?i)\bBALANCE\s+AS\s+OF\b"},
        {"pattern": r"(?i)\bFEE\b"},  # filter out miscellaneous fee-related transactions
        {"pattern": r"(?i)\bTRANSFER\b"},  # avoid showing internal transfers
    ],
}


# ----------------- UTILITIES -----------------

import io, sys

import contextlib
import io
import sys
from typing import Iterator, Union, TextIO

_stdin_cache = None


def peek_stdin():
    """Return a reusable, seekable buffer for stdin."""
    global _stdin_cache
    if _stdin_cache is None:
        data = sys.stdin.read()
        _stdin_cache = io.StringIO(data)
    else:
        _stdin_cache.seek(0)
    return _stdin_cache

def log_verbose(enabled, *args):
    """Print only when verbose mode is active."""
    if enabled:
        print("[DEBUG]", *args)


def sniff_input_type(text: str, verbose: bool = False) -> str:
    """
    Heuristically guess input type from a text sample.
    Returns one of: 'csv', 'qif', 'ofx', 'qfx', 'qbo', 'msmoney', 'txf', 'beancount', 'gnucash', 'camt053'.
    This is robust against BofA CSVs and other quasi-text inputs.
    """
    sample = text[:8192].strip()
    lower = sample.lower()

    # Optional debug
    if verbose:
        print(f"[DEBUG sniff] Sample length: {len(sample)}")
        print(f"[DEBUG sniff] First 120 chars: {sample[:120]!r}")

    # --- QIF (Quicken Interchange Format) ---
    if re.match(r"^!type:\w+", sample, flags=re.I):
        if verbose: print("[DEBUG sniff] Matched explicit !Type: header → qif")
        return "qif"
    if (
        re.search(r"(?m)^D\d{1,2}/\d{1,2}/\d{2,4}", sample)
        and re.search(r"(?m)^T[-+]?\d", sample)
        and "^" in sample
    ):
        if verbose: print("[DEBUG sniff] Found D/T/^ record triplet → qif")
        return "qif"

    # --- TXF (Tax eXchange Format) ---
    if (
        sample.startswith("V")
        and "^" in sample
        and re.search(r"(?m)^\$", sample)
        and "tdate" in lower
    ):
        if verbose: print("[DEBUG sniff] Found TXF markers (V,^,$,Tdate) → txf")
        return "txf"

    # --- OFX/QFX/QBO (SGML/XML-based) ---
    if "<ofx>" in lower or "<?xml" in lower:
        if "intuit" in lower or "webconnect" in lower:
            if verbose: print("[DEBUG sniff] Found Intuit/WebConnect markers → qbo")
            return "qbo"
        if "money" in lower or "microsoft" in lower:
            if verbose: print("[DEBUG sniff] Found 'money' markers → msmoney")
            return "msmoney"
        if verbose: print("[DEBUG sniff] Found <OFX> XML root → ofx")
        return "ofx"
    if any(tag in lower for tag in ("<stmttrn>", "<banktranlist>", "<trnlist>")):
        if verbose: print("[DEBUG sniff] Found OFX transaction tags → ofx")
        return "ofx"

    # --- MSMoney plain file variants ---
    if "microsoft money" in lower or "msmoney" in lower:
        if verbose: print("[DEBUG sniff] Found 'microsoft money' text → msmoney")
        return "msmoney"

    # --- CSV or tabular text ---
    if ("," in sample or "\t" in sample) and (
        re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", sample)
        or re.search(r"\b(date|posted|running balance)\b", lower)
    ):
        if verbose: print("[DEBUG sniff] Found comma/tab-delimited rows → csv")
        return "csv"
        
    # --- BEANCOUNT journal detection ---
    if re.search(r"^\d{4}-\d{2}-\d{2}\s+[*!]", sample, re.M) and re.search(r"(?m)^\s{2,}\S", sample):
        if verbose: print("[DEBUG sniff] Found Beancount-style postings → beancount")
        return "beancount"

    # --- GnuCash XML detection ---
    # GnuCash files are XML files, so we check for the XML declaration or specific tags (like gnc:transaction)
    if "<gnc:transaction" in lower or "<?xml" in lower:
        if verbose: print("[DEBUG sniff] Found GnuCash XML tags → gnucash")
        return "gnucash"

    # --- CAMT.053 XML detection ---
    # CAMT.053 files are XML-based and have the "camt.053" XML namespace.
    if "<Document" in lower and "camt.053" in lower:
        if verbose: print("[DEBUG sniff] Found CAMT.053 XML markers → camt053")
        return "camt053"

    # --- Fallback ---
    if verbose: print("[DEBUG sniff] Fallback to CSV (no strong markers found)")
    return "csv"




@contextlib.contextmanager
def open_maybe_stdin(path: str, mode: str = "r", *args, **kwargs):
    """
    Context-manager wrapper around open() that transparently handles
    '-', '/dev/stdin' (for reading) and '-', '/dev/stdout' (for writing).
    Safe for repeated reads of stdin using peek_stdin().
    Prints user-friendly errors instead of traceback.
    """
    is_read = "r" in mode or "+" in mode
    is_write = any(m in mode for m in ("w", "a", "x"))

    try:
        # ---- READ MODES ----
        if is_read and path in ("-", "/dev/stdin"):
            buf = peek_stdin()
            try:
                yield buf
            finally:
                buf.seek(0)
            return

        # ---- WRITE MODES ----
        if is_write and path in ("-", "/dev/stdout"):
            try:
                yield sys.stdout
            finally:
                sys.stdout.flush()
            return

        # ---- NORMAL FILE ----
        f = open(path, mode, *args, **kwargs)
        try:
            yield f
        finally:
            f.close()

    except FileNotFoundError:
        print(f"Error: File not found — {path}", file=sys.stderr)
        sys.exit(1)
    except PermissionError:
        print(f"Error: Permission denied — {path}", file=sys.stderr)
        sys.exit(1)
    except OSError as e:
        print(f"Error: Cannot open {path}: {e}", file=sys.stderr)
        sys.exit(1)



def detect_delimiter(sample_lines):
    tab_count = sum(line.count("\t") for line in sample_lines)
    comma_count = sum(line.count(",") for line in sample_lines)
    return "\t" if tab_count > comma_count else ","

def load_csv_rows(file_path, explicit_delim=None, return_text=False):
    import io

    with open_maybe_stdin(file_path, "r", encoding="utf-8-sig", errors="ignore") as f:
        sample = []
        for _ in range(10):
            line = f.readline()
            if not line:
                break
            sample.append(line)

        if hasattr(f, "seek") and f.seekable():
            f.seek(0)
            reader_source = f
            text_buffer = None
        else:
            buffered = "".join(sample) + f.read()
            reader_source = io.StringIO(buffered)
            text_buffer = buffered

        delimiter = explicit_delim or detect_delimiter(sample)
        reader = csv.reader(reader_source, delimiter=delimiter, quotechar='"', skipinitialspace=True)

        rows = []
        for row in reader:
            cleaned = [c.strip() for c in row]
            if any(cleaned):
                rows.append(cleaned)

        if reader_source is not f:
            reader_source.close()

    if return_text:
        return rows, delimiter, text_buffer or "".join(sample)
    return rows, delimiter


def normalize_date(date_str):
    date_str = date_str.strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
        try:
            d = datetime.strptime(date_str, fmt)
            return f"{d.month}/{d.day}/{d.year}"
        except ValueError:
            continue
    return date_str

def normalize_amount(value):
    v = str(value).strip().replace(",", "")
    if v.startswith("(") and v.endswith(")"):
        v = "-" + v[1:-1]
    return v

def cosine_similarity(vec1, vec2):
    dot = sum(a * b for a, b in zip(vec1, vec2))
    norm1 = sum(a * a for a in vec1) ** 0.5
    norm2 = sum(b * b for b in vec2) ** 0.5
    return dot / (norm1 * norm2 + 1e-10)

# ----------------- OFX FIXER -----------------
def fix_ofx_to_xml(data):
    """
    Convert loose OFX/QFX SGML syntax to well-formed XML.
    Handles dotted tags like <INTU.BID>, adds missing end tags, escapes '&'.
    """
    fixed_lines = []
    tag_pattern = re.compile(r"^<([A-Za-z0-9_.-]+)>([^<]+)$")

    for raw_line in data.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        # Escape & not already part of an entity
        line = re.sub(r"&(?!(amp|lt|gt|apos|quot);)", "&amp;", line)

        # Match <TAG>value (no closing)
        m = tag_pattern.match(line)
        if m:
            tag, val = m.groups()
            val = val.strip()
            line = f"<{tag}>{val}</{tag}>"

        fixed_lines.append(line)

    return "\n".join(fixed_lines)
    
    
def parse_beancount(file_path, account_name=None, verbose=False):
    """
    Parse Beancount journal into normalized transaction rows.

    Recognizes postings like:
        2024-12-05 * "SAFEWAY"
          Assets:Bank:Checking   -45.23 USD
          Expenses:Groceries
    """
    import re

    with open_maybe_stdin(file_path, "r", encoding="utf-8-sig", errors="ignore") as f:
        text = f.read()

    txn_pattern = re.compile(
        r"(?m)^(?P<date>\d{4}-\d{2}-\d{2})\s+[*!]\s+\"?(?P<payee>[^\n\"]*)\"?.*?\n((?:\s{2,}[^\n]+\n)+)",
        re.S,
    )
    postings_pattern = re.compile(
        r"^\s{2,}(?P<account>[A-Za-z0-9:\-]+)\s+(?P<amount>[-+]?\d[\d,\.]*)?\s*(?P<currency>[A-Z]{3})?",
        re.M,
    )

    rows = []
    for m in txn_pattern.finditer(text):
        date, payee, postings_text = m.group("date"), m.group("payee").strip(), m.group(3)
        postings = postings_pattern.findall(postings_text)
        if not postings:
            continue

        # Find asset/liability and expense/income postings
        asset = next((a for a in postings if a[0].startswith(("Assets", "Liabilities"))), postings[0])
        expense = next((a for a in postings if a[0].startswith(("Expenses", "Income"))), postings[-1])
        amt_str = asset[1] or expense[1] or "0"
        cur = asset[2] or expense[2] or "USD"

        try:
            amt_val = float(amt_str.replace(",", ""))
        except ValueError:
            amt_val = 0.0

        rows.append({
            "Date": datetime.strptime(date, "%Y-%m-%d").strftime("%m/%d/%Y"),
            "Payee": payee,
            "FI Payee": "",
            "Amount": f"{amt_val:.2f}",
            "Debit/Credit": "Debit" if amt_val < 0 else "Credit",
            "Category": expense[0],
            "Account": asset[0] if account_name is None else account_name,
            "Tag": "",
            "Memo": "",
            "Chknum": "",
        })

    if verbose:
        print(f"🪄 Parsed {len(rows)} transactions from Beancount journal.")

    return rows

# ----------------- GNUCASH PARSER -------------#


import gzip
import xml.etree.ElementTree as ET
from datetime import datetime
from fractions import Fraction

def parse_gnucash(file_path, account_name):
    """
    Parse a GnuCash XML file (.gnucash, possibly gzipped) and return a list
    of transactions for Quicken export. The passed-in account_name is imposed,
    not filtered, but amounts are preserved from split values.
    """
    # Read file (gzip-safe)
    if file_path.endswith(".gnucash"):
        with gzip.open(file_path, "rt", encoding="utf-8", errors="ignore") as f:
            data = f.read()
    else:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            data = f.read()

    root = ET.fromstring(data)

    NS = {
        "gnc": "http://www.gnucash.org/XML/gnc",
        "act": "http://www.gnucash.org/XML/act",
        "trn": "http://www.gnucash.org/XML/trn",
        "ts":  "http://www.gnucash.org/XML/ts",
        "split": "http://www.gnucash.org/XML/split",
    }

    def parse_gnc_date(s):
        s = (s or "").strip()
        if not s:
            return ""
        for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S"):
            try:
                d = datetime.strptime(s, fmt)
                return f"{d.month}/{d.day}/{d.year}"
            except ValueError:
                continue
        return s  # fallback

    txns = []

    for txn in root.findall(".//gnc:transaction", NS):
        desc = txn.findtext("trn:description", default="", namespaces=NS).strip()
        posted_raw = txn.findtext("trn:date-posted/ts:date", default="", namespaces=NS)
        date_fmt = parse_gnc_date(posted_raw)

        # Grab the first valid split's value (usually there are two, one +, one -)
        amount_val = None
        for sp in txn.findall(".//trn:splits/trn:split", NS):
            val_raw = sp.findtext("split:value", default="0", namespaces=NS).strip()
            try:
                amount_val = float(Fraction(val_raw))
                # pick the first nonzero split and stop
                if abs(amount_val) > 0:
                    break
            except Exception:
                continue

        if amount_val is None:
            amount_val = 0.0

        dc = "DBIT" if amount_val < 0 else "CRDT"
        out_amount = abs(amount_val)

        txns.append({
            "Date": date_fmt,
            "Payee": desc or "Unknown",
            "FI Payee": "",
            "Amount": f"{out_amount:.2f}",
            "Debit/Credit": dc,
            "Category": "",
            "Account": account_name,  # imposed externally
            "Tag": "",
            "Memo": desc,
            "Chknum": "",
            "Reconciled": "",
        })

    if not txns:
        raise ValueError("No transactions found in the GnuCash file.")

    return txns




# ----------------- CAMT 53 PARSER -------------#
# Note: This is largely underdeveloped at present, and almost as idiosyncratic as the abused-"TXF" parser.

def parse_camt053(file_path, account_name):
    """Parse a CAMT.053 XML statement into normalized transaction dicts."""

    from xml.etree import ElementTree as ET
    from datetime import datetime

    # Read the input (file or stdin)
    with open_maybe_stdin(file_path, "r", encoding="utf-8", errors="ignore") as f:
        data = f.read()

    if "<Document" not in data or "camt.053" not in data:
        raise ValueError("Not a valid CAMT.053 XML file (missing <Document> with camt.053).")

    # Attempt to parse XML
    try:
        root = ET.fromstring(data)
    except ET.ParseError as e:
        print(f"❌ ParseError at line {e.position[0]}, col {e.position[1]}: {e}")
        context = data.splitlines()
        around = range(max(0, e.position[0] - 3), min(len(context), e.position[0] + 3))
        for i in around:
            print(f"{i+1:4}: {context[i]}")
        raise ValueError("Failed to parse CAMT.053 XML; file may be truncated or malformed.") from e

    # Extract namespace dynamically
    if root.tag.startswith("{"):
        namespace = root.tag.split("}")[0][1:]
        NS = {"camt": namespace}
        prefix = "camt:"
    else:
        # Namespace-free fallback
        NS = {}
        prefix = ""

    txns = []

    # Iterate over each <Stmt> section (may contain multiple accounts)
    for stmt in root.findall(f".//{prefix}Stmt", NS):
        currency = None
        acct_elem = stmt.find(f"{prefix}Acct", NS)
        if acct_elem is not None:
            acct_id = (
                acct_elem.findtext(f"{prefix}Id/{prefix}Othr/{prefix}Id", "", NS)
                or account_name
            )
            currency = acct_elem.findtext(f"{prefix}Ccy", "", NS)
        else:
            acct_id = account_name

        # Loop over transactions
        for entry in stmt.findall(f"{prefix}Ntry", NS):
            amt_elem = entry.find(f"{prefix}Amt", NS)
            amt_text = amt_elem.text.strip() if amt_elem is not None and amt_elem.text else ""
            ccy = (
                amt_elem.attrib.get("Ccy", "")
                if amt_elem is not None
                else (currency or "")
            )
            cdt_dbt = (entry.findtext(f"{prefix}CdtDbtInd", "", NS) or "").upper()

            # Choose best date: ValDt > BookgDt
            date_str = (
                entry.findtext(f"{prefix}ValDt/{prefix}Dt", "", NS)
                or entry.findtext(f"{prefix}BookgDt/{prefix}Dt", "", NS)
                or entry.findtext(f"{prefix}BookgDt/{prefix}DtTm", "", NS)
            )

            # Parse human-friendly date
            date_fmt = date_str
            if date_str:
                try:
                    d = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    date_fmt = d.strftime("%m/%d/%Y")
                except Exception:
                    pass

            # Extract payee / memo / ref info
            payee = (
                entry.findtext(f".//{prefix}RltdPties/{prefix}Dbtr/{prefix}Pty/{prefix}Nm", "", NS)
                or entry.findtext(f".//{prefix}RltdPties/{prefix}Cdtr/{prefix}Pty/{prefix}Nm", "", NS)
                or ""
            )
            memo = (
                entry.findtext(f".//{prefix}AddtlTxInf", "", NS)
                or entry.findtext(f".//{prefix}RmtInf/{prefix}Ustrd", "", NS)
                or ""
            )
            ref = entry.findtext(f"{prefix}NtryRef", "", NS)
            tx_type = (
                entry.findtext(f"{prefix}BkTxCd/{prefix}Prtry/{prefix}Cd", "", NS)
                or entry.findtext(f"{prefix}BkTxCd/{prefix}Domn/{prefix}Fmly/{prefix}Cd", "", NS)
                or ""
            )

            # Normalize amount
            try:
                amount_val = normalize_amount(amt_text)
            except Exception:
                amount_val = amt_text

            # Apply credit/debit sign
            if cdt_dbt == "DBIT" and isinstance(amount_val, (int, float)):
                amount_val = -abs(amount_val)
            elif cdt_dbt == "DBIT" and isinstance(amount_val, str):
                amount_val = f"-{amount_val}" if not amount_val.startswith("-") else amount_val

            # Build Quicken-style transaction record
            txns.append({
                "Date": date_fmt or "",
                "Payee": (payee or tx_type).strip(),
                "FI Payee": "",
                "Amount": amount_val,
                "Debit/Credit": cdt_dbt,
                "Currency": ccy,
                "Category": "",
                "Account": acct_id or account_name or "",
                "Tag": "",
                "Memo": memo.strip(),
                "Chknum": ref or "",
            })

    if not txns:
        raise ValueError("No transactions found in CAMT.053 file (possibly due to namespaces).")

    return txns


# ----------------- QIF PARSER -----------------
def parse_qif(file_path, account_name):
    rows, tx, qif_type = [], {}, None
    with open_maybe_stdin(file_path, "r", encoding="utf-8-sig", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith("!Type:"):
                qif_type = line[6:].strip()
                continue
            if line == "^":
                if "Date" in tx and "Amount" in tx:
                    for k in QUICKEN_COLUMNS:
                        tx.setdefault(k, "")
                    tx["Account"] = account_name
                    rows.append(tx)
                tx = {}
                continue
            if line.startswith("D"):
                raw_date = line[1:].strip()
                for fmt in ("%m/%d/%y", "%m/%d/%Y"):
                    try:
                        d = datetime.strptime(raw_date, fmt)
                        tx["Date"] = f"{d.month}/{d.day}/{d.year}"
                        break
                    except ValueError:
                        continue
                else:
                    tx["Date"] = raw_date
            elif line.startswith("T"):
                tx["Amount"] = normalize_amount(line[1:].strip())
            elif line.startswith("P"):
                tx["Payee"] = line[1:].strip()
            elif line.startswith("M"):
                tx["Memo"] = line[1:].strip()
            elif line.startswith("L"):
                tx["Category"] = line[1:].strip()
            elif line.startswith("N"):
                tx["Chknum"] = line[1:].strip()
    return rows

# ----------------- OFX/QFX PARSER -----------------
def parse_ofx(file_path, account_name):
    with open_maybe_stdin(file_path, "r", encoding="utf-8", errors="ignore") as f:
        data = f.read()
    start = data.find("<OFX>")
    if start == -1:
        raise ValueError("Not a valid OFX/QFX file (no <OFX> tag found).")

    xml_data = fix_ofx_to_xml(data[start:])
    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as e:
        print(f"❌ ParseError at line {e.position[0]}, col {e.position[1]}: {e}")
        lines = xml_data.splitlines()
        around = range(max(0, e.position[0]-3), min(len(lines), e.position[0]+3))
        for i in around:
            print(f"{i+1:4}: {lines[i]}")
        raise

    txns = []
    for banklist in root.findall(".//BANKTRANLIST"):
        for trn in banklist.findall(".//STMTTRN"):
            date_str = trn.findtext("DTPOSTED", "").strip()
            amt = trn.findtext("TRNAMT", "").strip()
            name = trn.findtext("NAME", "").strip()
            ttype = trn.findtext("TRNTYPE", "").strip().upper()
            date = date_str[:8]
            try:
                d = datetime.strptime(date, "%Y%m%d")
                date_fmt = f"{d.month}/{d.day}/{d.year}"
            except ValueError:
                date_fmt = date_str
            txns.append({
                "Date": date_fmt,
                "Payee": name or ttype,
                "FI Payee": "",
                "Amount": normalize_amount(amt),
                "Debit/Credit": "",
                "Category": "",
                "Account": account_name,
                "Tag": "",
                "Memo": "",
                "Chknum": "",
            })
    if not txns:
        raise ValueError("No transactions found in OFX/QFX file.")
    return txns

# ----------------- CSV PARSERS -----------------
def detect_format(rows, verbose=False):
    """Detect whether CSV is bank, credit, or Quicken style."""
    if not rows:
        raise ValueError("Empty CSV input")

    header = " ".join(rows[0]).lower()
    sample = " ".join([" ".join(r).lower() for r in rows[:8]])

    log_verbose(verbose, "Header line:", rows[0])
    log_verbose(verbose, "Sample text:", sample[:200])

    if "fi payee" in header and "debit/credit" in header:
        log_verbose(verbose, "Detected Quicken CSV format")
        return "quicken"
    if "posted date" in header or "reference number" in header:
        log_verbose(verbose, "Detected BofA CREDIT CSV format")
        return "credit"
    if "running bal" in header or "running balance" in sample or "beginning balance" in sample:
        log_verbose(verbose, "Detected BofA BANK CSV format")
        return "bank"

    log_verbose(verbose, "Could not detect CSV type — header didn't match known patterns.")
    raise ValueError("Could not detect statement type (bank, credit, or quicken)")


def parse_bank(rows, account, verbose=False):
    out, in_table = [], False
    for idx, parts in enumerate(rows):
        if not any(parts):
            continue
        joined = " ".join(parts).lower()

        if re.search(r"^date[, ]+description", joined):
            in_table = True
            log_verbose(verbose, f"Found table start at line {idx}: {parts}")
            continue
        if not in_table:
            log_verbose(verbose, f"Skipping pre-table line {idx}: {parts}")
            continue

        # skip totals and headers
        if "running bal" in joined or "balance as of" in joined or "total" in joined:
            log_verbose(verbose, f"Skipping summary/header line {idx}: {parts}")
            continue

        # detect transaction line
        if len(parts) >= 2 and re.match(r"\d{1,2}/\d{1,2}/\d{2,4}", parts[0]):
            date = normalize_date(parts[0])
            desc = parts[1].strip() if len(parts) > 1 else ""
            amount = parts[-1].strip() if len(parts) >= 2 else "0"
            log_verbose(verbose, f"Parsed txn at line {idx}: {date=} {desc=} {amount=}")
            out.append({
                "Date": date,
                "Payee": desc,
                "FI Payee": "",
                "Amount": normalize_amount(amount),
                "Debit/Credit": "",
                "Category": "",
                "Account": account,
                "Tag": "",
                "Memo": "",
                "Chknum": ""
            })
        else:
            log_verbose(verbose, f"Unrecognized row {idx}: {parts}")

    log_verbose(verbose, f"Extracted {len(out)} bank transactions.")
    return out


def parse_credit(rows, account, verbose=False):
    out, header_seen = [], False
    for idx, parts in enumerate(rows):
        if not any(parts):
            continue
        joined = " ".join(parts).lower()

        if not header_seen and ("posted date" in joined or "reference number" in joined):
            header_seen = True
            log_verbose(verbose, f"Found header row at {idx}: {parts}")
            continue
        if not header_seen:
            log_verbose(verbose, f"Skipping pre-header line {idx}: {parts}")
            continue

        if len(parts) < 3:
            log_verbose(verbose, f"Skipping short line {idx}: {parts}")
            continue
        if not re.match(r"\d{1,2}/\d{1,2}/\d{2,4}", parts[0]):
            log_verbose(verbose, f"Skipping non-date row {idx}: {parts}")
            continue

        date = normalize_date(parts[0])
        payee = parts[2].strip() if len(parts) >= 3 else ""
        amount = parts[-1].strip()
        log_verbose(verbose, f"Parsed credit txn at line {idx}: {date=} {payee=} {amount=}")

        out.append({
            "Date": date,
            "Payee": payee,
            "FI Payee": "",
            "Amount": normalize_amount(amount),
            "Debit/Credit": "",
            "Category": "",
            "Account": account,
            "Tag": "",
            "Memo": "",
            "Chknum": ""
        })

    log_verbose(verbose, f"Extracted {len(out)} credit transactions.")
    return out


# ----------------- RECONCILE FEATURE -----------------
class Reconciler:
    """
    Assists reconciliation by computing running balance and comparing with
    an expected closing balance or external register file.

    Usage:
        r = Reconciler(target_balance=1234.56)
        r.run(rows)
    """

    def __init__(self, target_balance=None, external_file=None, verbose=False):
        self.target_balance = target_balance
        self.external_file = external_file
        self.verbose = verbose

    def _load_external(self):
        if not self.external_file:
            return []
        import csv
        with open_maybe_stdin(self.external_file, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        for r in rows:
            r["Amount"] = float(str(r.get("Amount", "0")).replace(",", ""))
        return rows

    def _sum(self, rows):
        total = 0.0
        for r in rows:
            try:
                total += float(str(r.get("Amount", "0")).replace(",", ""))
            except ValueError:
                continue
        return total

    def run(self, rows):
        print("🧾 Reconciling imported transactions...")
        imported_total = self._sum(rows)
        print(f"   Imported total: {imported_total:,.2f}")

        if self.target_balance is not None:
            diff = self.target_balance - imported_total
            print(f"   Target ending balance: {self.target_balance:,.2f}")
            print(f"   Difference: {diff:+,.2f}")
            if abs(diff) < 0.01:
                print("✅ Reconciled perfectly.")
            else:
                print("⚠️ Not reconciled — difference detected.")

        if self.external_file:
            ext = self._load_external()
            external_total = self._sum(ext)
            diff2 = external_total - imported_total
            print(f"   External register total: {external_total:,.2f}")
            print(f"   Difference vs imported: {diff2:+,.2f}")

            # Look for missing matches by Payee+Amount pairs
            missing = []
            imported_keys = {(r.get("Payee", "").strip().upper(), round(float(r.get("Amount", 0)), 2)) for r in rows}
            for e in ext:
                key = (e.get("Payee", "").strip().upper(), round(float(e.get("Amount", 0)), 2))
                if key not in imported_keys:
                    missing.append(e)

            if missing:
                print(f"❗ {len(missing)} transactions in register not found in import:")
                for m in missing[:10]:
                    print(f"   {m.get('Date','?'):<10} {m.get('Payee','')[:40]:40} {m.get('Amount',''):>10}")
                if len(missing) > 10:
                    print(f"   ... and {len(missing)-10} more")

        print("🧮 Reconciliation complete.\n")


# ----------------- TAX PARSERS -----------------


class TXFImporter:
    """
    Parses TXF (Tax eXchange Format) files used by TurboTax, Quicken, and brokers VERY nonstandardly.
    Note that this is obviously NOT sensible in the main (tax data is not tx data), 
    but a very few have misused these in ways that make souping them into generic forms useful.
    Supports header + record formats 0–6, and fixed-width X detail parsing.
    Returns list of Quicken-compatible row dicts.
    """

    def __init__(self, account_name):
        self.account = account_name

    @staticmethod
    def _normalize_date(date_str):
        date_str = date_str.strip()
        if not date_str or date_str.lower() == "various":
            return ""
        for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
            try:
                d = datetime.strptime(date_str, fmt)
                return f"{d.month}/{d.day}/{d.year}"
            except ValueError:
                continue
        return date_str

    @staticmethod
    def _normalize_amount(value):
        v = str(value).strip().replace(",", "")
        if v.startswith("(") and v.endswith(")"):
            v = "-" + v[1:-1]
        return v

    @staticmethod
    def _parse_x_field(x_line):
        """Parse fixed-width X detail line into dict fields."""
        data = {
            "Date": x_line[1:9].strip(),
            "AccountName": x_line[10:40].strip(),
            "CheckNumber": x_line[41:47].strip(),
            "Payee": x_line[48:88].strip(),
            "Memo": x_line[89:129].strip(),
            "Category": x_line[130:145].strip(),
        }
        return data

    def _detect_format(self, fields):
        num_d = len(fields.get("D", []))
        num_DOLLAR = len(fields.get("$", []))
        has_p = "P" in fields
        has_x = "X" in fields
        if has_x:
            return 6 if "P" in fields and "D" in fields else 0
        if num_d == 2 and num_DOLLAR == 2:
            return 4
        if num_d == 2 and num_DOLLAR == 3:
            return 5
        if has_p and num_DOLLAR == 1:
            return 3
        if has_p and not num_DOLLAR:
            return 2
        if num_DOLLAR == 1:
            return 1
        return 0

    def parse(self, file_path):
        with open_maybe_stdin(file_path, "r", encoding="utf-8-sig", errors="ignore") as f:
            content = f.read()

        records_raw = [r.strip() for r in content.split("^") if r.strip()]
        rows, header_seen = [], False

        for rec in records_raw:
            lines = [l.strip() for l in rec.splitlines() if l.strip()]
            if not lines:
                continue

            fields = {}
            for line in lines:
                key, val = line[0], line[1:].strip()
                fields.setdefault(key, []).append(val)

            # Skip header (V, A, D)
            if "V" in fields and not header_seen:
                header_seen = True
                continue

            fmt = self._detect_format(fields)
            row = self._record_to_row(fields, fmt)
            if row:
                rows.append(row)

        return rows

    def _record_to_row(self, f, fmt):
        """Convert parsed TXF record fields into a normalized Quicken-style dict."""
        amount = self._normalize_amount(f.get("$", [""])[0]) if "$" in f else ""
        desc = f.get("P", [""])[0] if "P" in f else ""
        dfields = f.get("D", [])
        date_acquired = self._normalize_date(dfields[0]) if len(dfields) >= 1 else ""
        date_sold = self._normalize_date(dfields[1]) if len(dfields) >= 2 else ""
        x_data = self._parse_x_field(f["X"][0]) if "X" in f else {}

        # Default date
        date = date_sold or date_acquired or x_data.get("Date", "")

        # Pick payee from priority: X.Payee > P.Description > fallback
        payee = x_data.get("Payee") or desc or f"TXF {f.get('N', [''])[0]}"

        row = {
            "Date": date,
            "Payee": payee,
            "FI Payee": "",
            "Amount": amount,
            "Debit/Credit": "",
            "Category": x_data.get("Category", ""),
            "Account": self.account,
            "Tag": "",
            "Memo": x_data.get("Memo", "") or f"TXF Ref {f.get('N', [''])[0]}",
            "Chknum": x_data.get("CheckNumber", ""),
        }

        return row

def parse_txf(file_path, account_name):
    importer = TXFImporter(account_name)
    return importer.parse(file_path)

# ----------------- QBO PARSER -----------------
def parse_qbo(file_path, account_name):
    """
    Parse QuickBooks .QBO Web Connect files.
    Structurally identical to OFX/QFX, but with Intuit-specific headers.
    """
    with open_maybe_stdin(file_path, "r", encoding="utf-8", errors="ignore") as f:
        data = f.read()

    start = data.find("<OFX>")
    if start == -1:
        raise ValueError("Not a valid QBO file (no <OFX> tag found).")

    xml_data = fix_ofx_to_xml(data[start:])
    root = ET.fromstring(xml_data)
    txns = []
    for banklist in root.findall(".//BANKTRANLIST"):
        for trn in banklist.findall(".//STMTTRN"):
            date_str = trn.findtext("DTPOSTED", "").strip()
            amt = trn.findtext("TRNAMT", "").strip()
            name = trn.findtext("NAME", "").strip()
            memo = trn.findtext("MEMO", "").strip()
            ttype = trn.findtext("TRNTYPE", "").strip().upper()
            date = date_str[:8]
            try:
                d = datetime.strptime(date, "%Y%m%d")
                date_fmt = f"{d.month}/{d.day}/{d.year}"
            except ValueError:
                date_fmt = date_str
            txns.append({
                "Date": date_fmt,
                "Payee": name or ttype,
                "FI Payee": "",
                "Amount": normalize_amount(amt),
                "Debit/Credit": "",
                "Category": "",
                "Account": account_name,
                "Tag": "",
                "Memo": memo,
                "Chknum": "",
            })
    if not txns:
        raise ValueError("No transactions found in QBO file.")
    return txns


# ----------------- MICROSOFT MONEY PARSER -----------------
def parse_msmoney(file_path, account_name):
    """
    Parse Microsoft Money OFX files (Money 97–Money Plus Sunset).
    Based on OFX 1.02 with no Intuit extensions.
    """
    with open_maybe_stdin(file_path, "r", encoding="utf-8", errors="ignore") as f:
        data = f.read()

    start = data.find("<OFX>")
    if start == -1:
        raise ValueError("Not a valid Microsoft Money OFX file (no <OFX> tag found).")

    xml_data = fix_ofx_to_xml(data[start:])
    root = ET.fromstring(xml_data)
    txns = []
    for banklist in root.findall(".//BANKTRANLIST"):
        for trn in banklist.findall(".//STMTTRN"):
            date_str = trn.findtext("DTPOSTED", "").strip()
            amt = trn.findtext("TRNAMT", "").strip()
            name = trn.findtext("NAME", "").strip()
            memo = trn.findtext("MEMO", "").strip()
            ttype = trn.findtext("TRNTYPE", "").strip().upper()
            date = date_str[:8]
            try:
                d = datetime.strptime(date, "%Y%m%d")
                date_fmt = f"{d.month}/{d.day}/{d.year}"
            except ValueError:
                date_fmt = date_str
            txns.append({
                "Date": date_fmt,
                "Payee": name or ttype,
                "FI Payee": "",
                "Amount": normalize_amount(amt),
                "Debit/Credit": "",
                "Category": "",
                "Account": account_name,
                "Tag": "",
                "Memo": memo,
                "Chknum": "",
            })
    if not txns:
        raise ValueError("No transactions found in Microsoft Money OFX file.")
    return txns


# ----------------- TRANSACTION ENRICHER -----------------
class TransactionEnricher:
    def __init__(self, rules=None, fi_payee=None, memo_template=None,
                 auto_dc=False, force_signed=False, disable_defaults=False,
                 autocat=False, autocat_backend="sentence",
                 autocat_model="all-MiniLM-L6-v2", fasttext_model=None,
                 autocat_labels=None, autocat_threshold=0.35,
                 autocat_report=None):
        self.rules = rules or {}
        self.fi_payee = fi_payee or ""
        self.memo_template = memo_template or ""
        self.auto_dc = auto_dc
        self.force_signed = force_signed
        self.disable_defaults = disable_defaults
        self.autocat = autocat
        self.backend = autocat_backend
        self.autocat_model_name = autocat_model
        self.fasttext_model_path = fasttext_model
        self.autocat_labels_file = autocat_labels
        self.autocat_threshold = autocat_threshold
        self.autocat_report = autocat_report
        self.model = None
        self.util = None
        self.cat_labels, self.cat_embs, self._autocat_results = [], [], []
        if not disable_defaults:
            self._merge_defaults(DEFAULT_RULES)
        if self.autocat:
            self._setup_autocat()

    def _merge_defaults(self, defaults):
        for k, v in defaults.items():
            if k not in self.rules:
                self.rules[k] = []
            self.rules[k].extend(v)

    def _setup_autocat(self):
        print(f"🔍 Autocat enabled (backend={self.backend})")
        base_cats = {
            "Groceries": ["grocery store", "supermarket", "trader joe's", "whole foods"],
            "Auto:Fuel": ["gas station", "fuel", "chevron", "shell"],
            "Transportation:Rideshare": ["uber", "lyft", "taxi"],
            "Entertainment:Subscriptions": ["netflix", "hulu", "spotify", "apple music"],
            "Utilities": ["pg&e", "comcast", "t-mobile", "verizon"],
            "Shopping:Online": ["amazon", "ebay", "online purchase"],
            "Dining": ["restaurant", "meal", "grill", "bar", "dining"],
        }
        if self.autocat_labels_file:
            with open_maybe_stdin(self.autocat_labels_file, "r", encoding="utf-8") as f:
                if self.autocat_labels_file.lower().endswith(".json"):
                    import json; user_labels = json.load(f)
                else:
                    import yaml; user_labels = yaml.safe_load(f)
            base_cats.update(user_labels)
        if self.backend == "sentence":
            from sentence_transformers import SentenceTransformer, util
            self.util, self.model = util, SentenceTransformer(self.autocat_model_name)
            for cat, examples in base_cats.items():
                for ex in examples:
                    self.cat_labels.append(cat)
                    self.cat_embs.append(self.model.encode(ex, convert_to_tensor=True))
        elif self.backend == "fasttext":
            import fasttext
            if not self.fasttext_model_path:
                raise SystemExit("❌ Provide --fasttext-model for backend=fasttext")
            self.model = fasttext.load_model(self.fasttext_model_path)
            for cat, examples in base_cats.items():
                for ex in examples:
                    self.cat_labels.append(cat)
                    self.cat_embs.append(self._fasttext_vec(ex))
        print(f"✅ Loaded {len(base_cats)} category groups")

    def _fasttext_vec(self, text):
        words = re.findall(r"\b\w+\b", text.lower())
        if not words:
            return [0.0] * self.model.get_dimension()
        vecs = [self.model.get_word_vector(w) for w in words]
        dim = len(vecs[0])
        return [mean(x[i] for x in vecs) for i in range(dim)]

    def _autocat_infer(self, desc):
        if self.backend == "sentence":
            desc_emb = self.model.encode(desc, convert_to_tensor=True)
            cos = self.util.cos_sim(desc_emb, self.cat_embs)
            best_idx, score = cos.argmax().item(), cos.max().item()
            return score, self.cat_labels[best_idx]
        else:
            v = self._fasttext_vec(desc)
            sims = [cosine_similarity(v, c) for c in self.cat_embs]
            best_idx = max(range(len(sims)), key=lambda i: sims[i])
            return sims[best_idx], self.cat_labels[best_idx]

    def apply_rules(self, row):
        desc, amt = row.get("Payee", ""), row.get("Amount", "").strip()

        # --- FILTER RULES ---
        for rule in self.rules.get("filter", []):
            if re.search(rule["pattern"], desc):
                return None  # skip this row

        row["FI Payee"] = self.fi_payee or row.get("FI Payee", "")
        # category
        for rule in self.rules.get("category", []):
            if re.search(rule["pattern"], desc):
                row["Category"] = rule["value"]
                break
        # tag
        tags = [rule["value"] for rule in self.rules.get("tag", []) if re.search(rule["pattern"], desc)]
        row["Tag"] = ":".join(tags)
        # memo
        row["Memo"] = self.memo_template.format(**row) if self.memo_template else ""
        # debit/credit logic
        if self.force_signed:
            if not amt.startswith(("+", "-")) and amt.replace(".", "", 1).isdigit():
                row["Amount"], row["Debit/Credit"] = f"+{amt}", ""
            else:
                row["Amount"], row["Debit/Credit"] = amt, ""
        elif self.auto_dc:
            val = amt.lstrip("+-")
            if amt.startswith("-"):
                row["Amount"], row["Debit/Credit"] = val, "Debit"
            else:
                row["Amount"], row["Debit/Credit"] = val, "Credit"
        else:
            row["Amount"], row["Debit/Credit"] = amt, ""
        # ML autocat
        if self.autocat and not row.get("Category"):
            try:
                score, best_cat = self._autocat_infer(desc)
                if score >= self.autocat_threshold:
                    row["Category"] = best_cat
                    self._autocat_results.append((desc, best_cat, score))
            except Exception as e:
                print(f"⚠️ Autocat failed for '{desc}': {e}")
        return row

    def write_autocat_report(self):
        if not self.autocat_report or not self._autocat_results:
            return
        with open_maybe_stdin(self.autocat_report, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows([("Description","Category","Similarity"), *self._autocat_results])
        print(f"📊 Autocat report written to {self.autocat_report}")
        
# ----------------- RULE LEARNER -----------------


class RuleLearner:
    """
    Learns regex-based category rules from a Quicken-compatible CSV.
    Prints discovered rules to stdout — never writes or mutates anything.
    """
    def __init__(self, min_support=2, fuzzy_threshold=0.75):
        self.min_support = min_support
        self.fuzzy_threshold = fuzzy_threshold

    def _normalize(self, name):
        n = re.sub(r"[\d#]+", "", name)
        n = re.sub(r"[^A-Za-z\s&]", " ", n)
        n = re.sub(r"\s+", " ", n).strip().upper()
        return n

    def _cluster(self, names):
        import difflib  # dynamically imported only if learning is invoked
        clusters = []
        for name in names:
            placed = False
            for cluster in clusters:
                if any(difflib.SequenceMatcher(None, name, c).ratio() > self.fuzzy_threshold for c in cluster):
                    cluster.append(name)
                    placed = True
                    break
            if not placed:
                clusters.append([name])
        return clusters

    def learn(self, csv_path):
        print(f"🧠 Learning from {csv_path}")
        with open_maybe_stdin(csv_path, "r", encoding="utf-8-sig", errors="ignore") as f:
            reader = csv.DictReader(f)
            from collections import defaultdict
            groups = defaultdict(list)
            for row in reader:
                cat = row.get("Category", "").strip()
                payee = row.get("Payee", "").strip()
                if cat and payee:
                    groups[cat].append(self._normalize(payee))

        rules = []
        for cat, payees in groups.items():
            clusters = self._cluster(payees)
            for cluster in clusters:
                if len(cluster) >= self.min_support:

                    common = set(cluster[0].split())
                    for c in cluster[1:]:
                        common &= set(c.split())

                    # prefer multi-word patterns or tokens >2 chars
                    tokens = [t for t in sorted(common, key=len, reverse=True) if len(t) > 2]
                    if tokens:
                        base = re.escape(tokens[0])
                        pattern = fr"(?i)\b{base}(?:\b|[- ])?"
                        rules.append({"pattern": pattern, "value": cat})

        if not rules:
            print("⚠️  No rules learned — ensure CSV has 'Payee' and 'Category' columns.")
            return

        import json
        print("\n📘 Suggested category rules:\n")
        for r in rules:
            print(json.dumps(r, ensure_ascii=False))
        print(f"\n✅{len(rules)} rules suggested.\n")

        # 🧭 CLI Guidance Section
        print("💡 You can apply these rules in several ways:")
        print("--------------------------------------------------")
        print("1. Inline via --category-rule:")
        for r in rules:
            print(f'   --category-rule "{r["pattern"]}={r["value"]}"')
        print("\n2. Save to JSON:")
        import json
        json_rules = json.dumps({"category": rules}, indent=2, ensure_ascii=False)
        escaped = json_rules.replace("'", "'\\''")
        print(f"   echo '{escaped}' > my_rules.json")
        print("\n   Then use:")
        print('   bofa_to_quicken.py stmt.qfx -a "BOFA Acct" --rules my_rules.json')
        print("\n3.Combine with inline rules:")
        print('   bofa_to_quicken.py stmt.qfx -a "BOFA Acct" --rules my_rules.json \\')
        print('       --tag-rule "(?i)\\bREFUND=refund"')
        print("--------------------------------------------------")
        print("Tip: Use --no-default-rules if you want to start from a clean rule set.\n")


import hashlib
from datetime import timedelta

class TransactionConsolidator:
    """
    Handles multi-file merging and transaction deduplication.
    Designed to operate before enrichment. Safe defaults preserve all records
    unless explicitly told to deduplicate.
    """

    def __init__(self, dedupe=False, strategy="strict", max_date_drift=2, verbose=False):
        self.dedupe = dedupe
        self.strategy = strategy
        self.max_date_drift = max_date_drift
        self.verbose = verbose

    # ----------------- Utility Methods -----------------
    @staticmethod
    def _txn_key(row):
        """Generate a stable hash key for strict deduplication."""
        date = row.get("Date", "").strip()
        payee = re.sub(r"[^A-Za-z0-9]+", "", row.get("Payee", "").upper())
        amt = row.get("Amount", "").replace("+", "").replace("-", "")
        acct = row.get("Account", "")
        base = f"{date}|{payee}|{amt}|{acct}"
        return hashlib.md5(base.encode()).hexdigest()

    @staticmethod
    def _normalize_payee(p):
        return re.sub(r"[^A-Za-z]+", "", p).upper()

    # ----------------- Core Operations -----------------
    def merge(self, list_of_row_lists, source_names=None):
        """
        Flatten multiple parsed lists into one unified list with source tags.
        Prints individual counts and totals.
        """
        merged = []
        if not source_names:
            source_names = [f"input_{i+1}" for i in range(len(list_of_row_lists))]

        total = 0
        print("📦 Merge summary:")
        for idx, (rows, src) in enumerate(zip(list_of_row_lists, source_names), start=1):
            count = len(rows)
            total += count
            if self.verbose:
                print(f"   • {src}: {count} transactions")
            for r in rows:
                r.setdefault("SourceFile", src)
                merged.append(r)

        print(f"📊 Total merged: {total} transactions from {len(list_of_row_lists)} sources\n")
        return merged

    def dedupe(self, rows):
        """Perform deduplication depending on chosen strategy."""
        if not self.dedupe or not rows:
            return rows

        seen, unique = set(), []
        strategy = self.strategy.lower()
        drift = self.max_date_drift

        for r in rows:
            key = self._txn_key(r)
            if strategy == "strict":
                if key not in seen:
                    seen.add(key)
                    unique.append(r)
            else:
                # Fuzzy deduplication using date/amount/payee heuristic
                amt = float(r.get("Amount", "0").replace("+", "").replace("-", "") or 0)
                payee = self._normalize_payee(r.get("Payee", ""))
                try:
                    d = datetime.strptime(r.get("Date", ""), "%m/%d/%Y")
                except Exception:
                    d = None

                is_dupe = False
                for u in unique[-200:]:  # local window scan for performance
                    if abs(amt - float(u.get("Amount", "0").replace("+", "").replace("-", "") or 0)) < 0.01:
                        if self._normalize_payee(u.get("Payee", "")) == payee:
                            try:
                                du = datetime.strptime(u.get("Date", ""), "%m/%d/%Y")
                                if d and du and abs((d - du).days) <= drift:
                                    is_dupe = True
                                    break
                            except Exception:
                                continue
                if not is_dupe:
                    seen.add(key)
                    unique.append(r)

        print(f"🧮 Deduplication: {len(rows)} → {len(unique)} (removed {len(rows) - len(unique)})")
        return unique
        
        
# ----------------- BUDGET VISUALIZER -----------------
class BudgetVisualizer:
    """
    Optional visualization layer for financial summaries.
    Generates pie, bar, and line charts using matplotlib if available.
    """

    def __init__(self, df, verbose=False, pd=None):
        self.df = df
        self.verbose = verbose
        self.pd = pd

    def _try_import_matplotlib(self):
        try:
            import matplotlib.pyplot as plt
            from matplotlib.backends.backend_pdf import PdfPages
            return plt, PdfPages
        except ImportError:
            raise SystemExit(
                "⚠️  Visualization requires matplotlib. Install with `pip install matplotlib`."
            )

    def render(self, output_path=None):
        plt, PdfPages = self._try_import_matplotlib()

        df = self.df.copy()
        # Ensure numeric and date conversions
        df["Amount"] = self.pd.to_numeric(df["Amount"], errors="coerce").fillna(0)
        try:
            df["Date"] = self.pd.to_datetime(df["Date"], errors="coerce")
        except Exception:
            pass

        figs = []

        # ---------- Pie Chart: Spending by Category ----------
        cat = (
            df.groupby("Category")["Amount"]
            .sum()
            .sort_values(ascending=False)
        )
        if df["Amount"].isna().all():
            print("⚠️ No valid numeric Amount column — skipping visualization.")
            return

        # ---------- Pie Chart: Spending by Category ----------
        # Signed Amount is the most reliable indicator: negatives = outflows.
        spend_df = df[df["Amount"] < 0].copy()

        # Fallback: if all Amounts are positive (some exports use Debit/Credit instead)
        if spend_df.empty and "Debit/Credit" in df.columns:
            spend_df = df[df["Debit/Credit"].str.lower().eq("debit")].copy()

        if not spend_df.empty:
            spend_df["Spending"] = spend_df["Amount"].abs()
            cat = (
                spend_df.groupby("Category")["Spending"]
                .sum()
                .sort_values(ascending=False)
            )
            fig1, ax1 = plt.subplots(figsize=(6, 6))
            cat.plot(
                kind="pie",
                autopct="%1.1f%%",
                ax=ax1,
                title="Spending by Category (Outflows Only)",
                startangle=90,
                label="",
            )
            figs.append(fig1)
            if self.verbose:
                print(f"✅ Added category pie chart ({len(cat)} categories).")
        else:
            if self.verbose:
                print("⚠️ No outflow (negative) transactions found for category pie chart.")


        # ---------- Monthly Inflows vs Outflows ----------
        if "Debit/Credit" in df.columns and df["Date"].notna().any():
            df_dc = df.copy()
            monthly = (
                df_dc.groupby([self.pd.Grouper(key="Date", freq="M"), "Debit/Credit"])["Amount"]
                .sum()
                .unstack(fill_value=0)
            )
            if not monthly.empty:
                fig2, ax2 = plt.subplots(figsize=(8, 4))
                monthly.plot(kind="bar", stacked=True, ax=ax2)
                ax2.set_title("Monthly Inflows vs Outflows")
                ax2.set_xlabel("Month")
                ax2.set_ylabel("Amount")
                figs.append(fig2)
                if self.verbose:
                    print("✅ Added monthly inflow/outflow bar chart.")

        # ---------- Balance Over Time ----------
        if df["Date"].notna().any():
            df_sorted = df.sort_values("Date")
            signed = df_sorted["Amount"].astype(float)
            # Debit as negative if marked
            if "Debit/Credit" in df_sorted:
                signed = signed.where(df_sorted["Debit/Credit"].str.lower() != "debit", -signed)
            df_sorted["Balance"] = signed.cumsum()

            fig3, ax3 = plt.subplots(figsize=(8, 4))
            ax3.plot(df_sorted["Date"], df_sorted["Balance"], linewidth=2)
            ax3.set_title("Account Balance Over Time")
            ax3.set_xlabel("Date")
            ax3.set_ylabel("Balance")
            figs.append(fig3)
            if self.verbose:
                print("✅ Added balance-over-time line chart.")

        # ---------- Output Handling ----------
        if not figs:
            print("⚠️ No data to visualize.")
            return

        if output_path:
            suffix = str(output_path).lower()
            if suffix.endswith(".pdf"):
                with PdfPages(output_path) as pdf:
                    for f in figs:
                        pdf.savefig(f, bbox_inches="tight")
                print(f"📊 BudgetViz report written to {output_path}")
            else:
                for i, f in enumerate(figs, 1):
                    f.savefig(f"{Path(output_path).stem}_{i}.png", bbox_inches="tight")
                print(f"📊 Charts saved as {Path(output_path).stem}_*.png")
        else:
            plt.show()




# ----------------- CSV WRITER -----------------
# ----------------- CSV WRITER -----------------
def write_quicken(rows, output_path, compliance="standard", verbose=False):
    """
    Write transactions to a Quicken-compatible CSV file.

    - Ignores extra keys like 'SourceFile'
    - Respects compliance level
    - Works with stdout and non-seekable outputs
    """
    fields = QUICKEN_COLUMNS if compliance != "loose" else REQUIRED_COLUMNS
    with open_maybe_stdin(output_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        count = 0
        for r in rows:
            # Ensure required fields exist
            for k in QUICKEN_COLUMNS:
                r.setdefault(k, "")

            if compliance == "loose":
                r = {k: r[k] for k in REQUIRED_COLUMNS}

            w.writerow(r)
            count += 1

        if verbose:
            print(f"📝 Wrote {count} transactions to {output_path}")


def write_qif4(rows, output_path, verbose=False):
    """
    Write transactions to a QIF 4-digit format file.
    The format includes:
    - Date in MM/DD/YYYY format
    - Payee, Memo, Amount, Category (limited to 4 digits)
    """
    with open_maybe_stdin(output_path, "w", newline="", encoding="utf-8") as f:
        # Write the QIF header
        f.write("!Type:Bank\n")

        count = 0
        for r in rows:
            date = r.get("Date", "")
            payee = r.get("Payee", "")
            amount = r.get("Amount", "0.00")
            category = r.get("Category", "Uncategorized")
            memo = r.get("Memo", "")

            # Format the date (MM/DD/YYYY)
            try:
                date_obj = datetime.strptime(date, "%m/%d/%Y")
                formatted_date = date_obj.strftime("%m/%d/%Y")
            except ValueError:
                formatted_date = date  # Fallback to raw value if invalid

            # Ensure amount is in a consistent format
            amount = normalize_amount(amount)

            # Write the QIF transaction
            f.write(f"D{formatted_date}\n")
            f.write(f"P{payee}\n")
            f.write(f"T{amount}\n")
            f.write(f"L{category}\n")
            f.write(f"M{memo}\n")
            f.write("^\n")
            count += 1

        if verbose:
            print(f"📝 Wrote {count} transactions to {output_path} in QIF 4-digit format.")


def write_beancount(rows, output_path, account_name, currency="USD", verbose=False):
    """
    Export transactions in Beancount journal format.

    Each transaction becomes:
        DATE * "Payee" "Memo"
          <source account>  <amount> USD
          <category account>

    The Category is treated as an expense/income account.
    The Account field is treated as the asset/liability source.
    """
    if not rows:
        print("⚠️ No transactions to write.")
        return

    # Normalize output path
    output_path = Path(output_path)

    with open_maybe_stdin(str(output_path), "w", encoding="utf-8") as f:
        # Optionally include account declarations
        seen_accounts = set()
        for r in rows:
            acct = r.get("Account", account_name)
            cat = r.get("Category", "Expenses:Uncategorized")
            for a in (acct, cat):
                if a and a not in seen_accounts:
                    seen_accounts.add(a)
                    f.write(f"1970-01-01 open {a} {currency}\n")
        f.write("\n")

        for r in rows:
            date = r.get("Date", "")
            payee = r.get("Payee", "").replace('"', "'")
            memo = r.get("Memo", "").replace('"', "'")
            cat = r.get("Category", "Expenses:Uncategorized")
            acct = r.get("Account", account_name)
            amt_str = r.get("Amount", "0").replace(",", "").strip()
            try:
                amt = float(amt_str)
            except ValueError:
                amt = 0.0

            # Determine polarity
            debit_credit = r.get("Debit/Credit", "").lower()
            if debit_credit == "debit" or amt < 0:
                src_acct = acct
                dst_acct = cat
                amt_disp = abs(amt)
                sign = "-"
            else:
                src_acct = cat
                dst_acct = acct
                amt_disp = abs(amt)
                sign = ""

            f.write(f"{date} * \"{payee}\" \"{memo}\"\n")
            f.write(f"  {src_acct:<40}  {sign}{amt_disp:.2f} {currency}\n")
            f.write(f"  {dst_acct}\n\n")

    if verbose:
        print(f"🧾 Wrote {len(rows)} Beancount transactions to {output_path}")



def run_exchange_server(enrichedData, config):
    import subprocess
    import sqlite3
    import json
    import os
    from pathlib import Path
    
    # Handle SQLite data source
    if config.get("data_source", "").startswith("sqlite://"):
        db_path = Path(config["data_source"].replace("sqlite://", ""))
        # Ensure the SQLite database path exists
        if not db_path.parent.exists():
            db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Assuming enrichedData is a list of transactions, save it to the database
        cursor.execute('''CREATE TABLE IF NOT EXISTS transactions (
                            id TEXT PRIMARY KEY, date TEXT, payee TEXT, amount REAL, category TEXT, account TEXT)''')
        
        for txn in enrichedData:
            cursor.execute('''INSERT OR REPLACE INTO transactions (id, date, payee, amount, category, account) 
                              VALUES (?, ?, ?, ?, ?, ?)''', 
                           (txn["Chknum"], txn["Date"], txn["Payee"], txn["Amount"], txn["Category"], txn["Account"]))

        conn.commit()  # Save changes
        conn.close()   # Close the connection
    
    # Handle JSON data source (file-based)
    elif config.get("data_source", "").startswith("json://"):
        json_file_path = config["data_source"].replace("json://", "")
        if not os.path.exists(json_file_path):
            store = {"accounts": [], "transactions": []}
        else:
            with open(json_file_path, 'r', encoding="utf-8") as f:
                store = json.load(f)

        # Store the enriched data in the JSON file
        store["transactions"].extend(enrichedData)
        
        with open(json_file_path, 'w', encoding="utf-8") as f:
            json.dump(store, f)
    
    # Handle in-memory data source
    elif config["data_source"] == "in-memory":
        store = {"accounts": [], "transactions": []}
        store["transactions"].extend(enrichedData)
        # For in-memory mode, we're simply storing the data in a dictionary

    else:
        print("Unsupported data source type")
        return

    # After saving the data, run the interchange server
    subprocess.run([sys.executable, "bankknifeexchange.py", 
                    "--mode", "server", 
                    "--data-source", config["data_source"], 
                    "--port", str(config["port"]),
                    "--base-url", config["base_url"], 
                    "--authz-base", config["authz_base"]])


def run_exchange_client(config, query):
    """
    Run the exchange client mode (query the interchange server)
    config: dictionary containing interchange base URLs and authorization info
    query: string that specifies the API query (e.g., accounts, transactions)
    """
    import subprocess
    import sys

    # Ensure the correct base URL and authorization base URL are passed
    base_url = config.get("base_url", "http://localhost:9090")  # interchange Resource API base URL
    authz_base = config.get("authz_base", "http://localhost:8080")  # interchange Authorization API base URL

    # Build the subprocess command to call bankknifeexchange.py in client mode
    command = [
        sys.executable, "bankknifeexchange.py",  # Path to the Python script
        "--mode", "client",                      # Run in client mode to query the interchange server
        "--base-url", base_url,                  # Set the base URL for the interchange Resource API
        "--authz-base", authz_base,              # Set the authorization URL
        query                                    # Pass the query to the client mode (e.g., "accounts" or "transactions")
    ]
    
    # Capture the output of the subprocess call
    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode == 0:
        # Successfully executed, return the stdout (response data)
        return result.stdout
    else:
        # Handle errors, printing stderr if the command fails
        print(f"Error occurred: {result.stderr}")
        return None



# ----------------- MAIN -----------------
def main():
    p = argparse.ArgumentParser(description="Convert BOFA CSV/QIF/OFX/QFX → Quicken CSV")
    p.add_argument("input_file")
    p.add_argument("-a", "--account", required=True)
    p.add_argument("-o", "--output")
    p.add_argument("--input-type", choices=["auto", "csv", "qif", "ofx", "qfx", "qbo", "msmoney", "txf"], default="auto")

    p.add_argument("--delimiter", choices=[",", "tab"])
    p.add_argument("--compliance", choices=["loose", "standard", "strict"], default="standard")
    p.add_argument("--auto-debitcredit", action="store_true")
    p.add_argument("--force-signed", action="store_true")
    p.add_argument("--rules")
    p.add_argument("--category-rule", action="append")
    p.add_argument("--tag-rule", action="append")
    p.add_argument("--fi-payee")
    p.add_argument("--memo-template")
    p.add_argument("--no-default-rules", action="store_true")
    
    # autocat
    p.add_argument("--autocat", action="store_true")
    p.add_argument("--autocat-backend", choices=["sentence", "fasttext"], default="sentence")
    p.add_argument("--autocat-model", default="all-MiniLM-L6-v2")
    p.add_argument("--fasttext-model")
    p.add_argument("--autocat-labels")
    p.add_argument("--autocat-threshold", type=float, default=0.35)
    p.add_argument("--autocat-report")
    p.add_argument("--learn-from", help="Learn regex category rules from a Quicken-compatible CSV (prints only).")
    
    # merger & deduper
    p.add_argument("--merge", nargs="*", help="Additional input files to merge before processing.")
    p.add_argument("--dedupe", action="store_true", help="Enable deduplication across merged inputs.")
    p.add_argument("--dedupe-strategy", choices=["strict", "fuzzy"], default="strict")
    p.add_argument("--max-date-drift", type=int, default=2)
    
    p.add_argument("-v", "--verbose", action="store_true", help="Enable verbose debugging output.")

    p.add_argument("--report-query", help="Run an SQL-like query over processed transactions in the Quicken import CSV target schema.")
    p.add_argument("--report-output", help="Write query results to this file (CSV or JSON).")
    p.add_argument("--report-format", choices=["table", "csv", "json"], default="table",
                   help="Output format for report-query results.")
                   
    p.add_argument("--reuseQueryAsNewEnrichedData", action="store_true", help="Direct the query processor to, when exiting, give the SQL-transformed data back to the data sink or export instead of exiting.")
                   
    p.add_argument("--reconcile", action="store_true",
                   help="Run reconciliation check after enrichment.")
    p.add_argument("--reconcile-balance", type=float,
                   help="Expected ending balance (e.g. from statement).")
    p.add_argument("--reconcile-against",
                   help="Optional CSV file (Quicken register export) to compare against.")
    p.add_argument("--output-format", choices=["csv", "beancount"], default="csv",
               help="Output format: Quicken CSV (default) or Beancount ledger.")

    p.add_argument(
        "--beancount",
        nargs="?",
        const=True,
        help="Output Beancount journal instead of CSV. Optionally specify output file (e.g. ledger.beancount)."
    )
    p.add_argument(
        "--beancount-currency",
        default="USD",
        help="Currency code for Beancount output (default: USD)."
    )
    
    p.add_argument(
        "--qif4", 
        nargs="?", 
        const=True, 
        help="Output transactions in QIF 4-digit format (instead of the default format)."
    )

                   
    p.add_argument(
        "--budgetviz",
        nargs="?",
        const=True,
        help="Generate visualizations (pie, inflows/outflows, balance). Optionally specify output file (e.g. report.pdf)."
    )
    
    
    p.add_argument("--exchange-server-outputsrv", action="store_true", help="EXPERIMENTAL: Run as exchange server (interchange-like mock server mode)")
    p.add_argument("--exchange-server-data-source", default="sqlite:///fdx.db", help="Data source for the server (e.g., sqlite:///path/to/db)")
    p.add_argument("--exchange-server-port", type=int, default=9090, help="Port for the server")
    p.add_argument("--exchange-server-base-url", default="http://localhost:3323", help="Base URL for the exchange server")
    p.add_argument("--exchange-server-authz-base", default="http://localhost:3324", help="Authorization base URL")

    p.add_argument("--exchange-client-inputsrc", action="store_true", help="EXPERIMENTAL: Try to use the interchange exchange client to grab transaction data from your bank.")
    p.add_argument("--exchange-client-port", type=int, default=9090, help="Port for the bank")
    p.add_argument("--exchange-client-base-url", default="http://localhost:4323", help="Base URL for the bank server")
    p.add_argument("--exchange-client-authz-base", default="http://localhost:4324", help="Base URL for the bank authorizer")


    args = p.parse_args()
    
    if args.learn_from:
        learner = RuleLearner()
        learner.learn(args.learn_from)
        return

    exchange_client_config = {}
    remote_bank_acct_data = None

    if args.exchange_client_inputsrc:
       print(f"Running Exchange Client against port {args.exchange_client_port}")
       exchange_client_config = {
        "port": args.exchange_client_port,
        "base_url": args.exchange_client_base_url,
        "authz_base": args.exchange_client_authz_base
       }
       remote_bankacct_input_data = run_exchange_client(exchange_client_config, "/fdx/v6/accounts")

    # determine file type
    # ----------------- DETERMINE INPUT TYPE -----------------
    def infer_input_type(path, explicit_type="auto", verbose=False):
        """
        Infer input type from file extension or by sniffing content.
        Safe for stdin/pipes: uses peek_stdin() so data isn't consumed.
        Returns one of: qif, ofx, qfx, qbo, txf, msmoney, csv, gnucash, camt053, beancount
        """
        if explicit_type and explicit_type != "auto":
            return explicit_type.lower()

        # --- Infer from extension ---
        if path not in ("-", "/dev/stdin"):
            ext = Path(path).suffix.lower()

            # Handle formats based on file extension
            if ext in (".qbo", ".ofx", ".qfx", ".qif", ".txf"):
                if verbose:
                    print(f"[DEBUG] Inferred from extension: {ext}")
                return ext.lstrip(".")
            if "money" in ext or ext == ".mny":
                return "msmoney"
            if ext == ".gnucash":
                return "gnucash"
            if ext == ".camt053":
                return "camt053"
            if ext == ".beancount":
                return "beancount"
            # else fall through to sniffing

        # --- Sniff content safely ---
        if path in ("-", "/dev/stdin"):
            buf_io = peek_stdin()
            pos = buf_io.tell()
            buf = buf_io.read(8192)
            buf_io.seek(pos)
        else:
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    buf = f.read(8192)
            except  FileNotFoundError:
                print(f"❌ Error: Input file '{path}' not found. Please check the path or filename.")
                import sys
                sys.exit(1)
                

        guessed = sniff_input_type(buf)
        print(f"🔍 Detected input type: {guessed}")
        return guessed


   # ----------------- DETERMINE INPUT TYPE -----------------
    fmt = infer_input_type(args.input_file, args.input_type, args.verbose)

    # ----------------- PARSE MAIN INPUT -----------------
    if fmt == "qbo":
        rows = parse_qbo(args.input_file, args.account)
    elif fmt in ("ofx", "qfx"):
        rows = parse_ofx(args.input_file, args.account)
    elif fmt == "qif":
        rows = parse_qif(args.input_file, args.account)
    elif fmt == "txf":
        rows = parse_txf(args.input_file, args.account)
    elif fmt == "msmoney":
        rows = parse_msmoney(args.input_file, args.account)
    elif fmt == "beancount":
        rows = parse_beancount(args.input_file, args.account, args.verbose)
    elif fmt == "gnucash":
        rows = parse_gnucash(args.input_file, args.account)
    elif fmt == "camt053":
        rows = parse_camt053(args.input_file, args.account)
    elif fmt == "csv":
        # Load CSV safely (works for stdin or files)
        rows_raw, _ = load_csv_rows(args.input_file, "\t" if args.delimiter == "tab" else None)

        # Detect CSV subtype
        try:
            csv_fmt = detect_format(rows_raw, args.verbose)
        except Exception as e:
            csv_fmt = "unknown"
            if args.verbose:
                print(f"[DEBUG] detect_format failed: {e}")

        print(f"📄 Detected CSV subtype: {csv_fmt}")

        if csv_fmt == "bank":
            rows = parse_bank(rows_raw, args.account, args.verbose)
        elif csv_fmt == "credit":
            rows = parse_credit(rows_raw, args.account, args.verbose)
        elif csv_fmt == "quicken":
            # Already Quicken-formatted, just read in as dicts
            print("ℹ️  Input already in Quicken CSV format — passing through.")
            reader = csv.DictReader([",".join(r) for r in rows_raw])
            rows = [dict(r) for r in reader]
        else:
            raise ValueError("Unrecognized CSV subtype.")
    else:
        raise ValueError(f"Unknown input type: {fmt}")

    # ----------------- MERGE & DEDUPE -----------------
    all_datasets = [rows]
    if args.merge:
        for extra in args.merge:
            try:
                fmt2 = infer_input_type(extra, "auto", args.verbose)
                if fmt2 == "csv":
                    raw, _ = load_csv_rows(extra)
                    fmt3 = detect_format(raw, args.verbose)
                    parsed = (
                        parse_bank(raw, args.account, args.verbose)
                        if fmt3 == "bank"
                        else parse_credit(raw, args.account, args.verbose)
                    )
                elif fmt2 == "qbo":
                    parsed = parse_qbo(extra, args.account)
                elif fmt2 in ("ofx", "qfx"):
                    parsed = parse_ofx(extra, args.account)
                elif fmt2 == "qif":
                    parsed = parse_qif(extra, args.account)
                elif fmt2 == "txf":
                    parsed = parse_txf(extra, args.account)
                elif fmt2 == "msmoney":
                    parsed = parse_msmoney(extra, args.account)
                elif fmt2 == "beancount":
                    parsed = parse_beancount(extra, args.account, args.verbose)
                elif fmt2 == "gnucash":
                    parsed = parse_gnucash(extra, args.account)
                elif fmt2 == "camt053":
                    parsed = parse_camt053(extra, args.account)
                else:
                    raise ValueError(f"Unsupported format: {fmt2}")
                all_datasets.append(parsed)
                print(f"📎 Merged {extra} ({len(parsed)} txns, type={fmt2})")
            except Exception as e:
                print(f"⚠️ Skipped {extra}: {e}")



    consolidator = TransactionConsolidator(
        dedupe=args.dedupe,
        strategy=args.dedupe_strategy,
        max_date_drift=args.max_date_drift,
    )

    if len(all_datasets) > 1:
        rows = consolidator.merge(all_datasets)
    if args.dedupe:
        rows = consolidator.dedupe(rows)

    # rules
    user_rules = {}
    if args.rules:
        with open_maybe_stdin(args.rules, "r", encoding="utf-8") as f:
            if args.rules.lower().endswith(".json"):
                user_rules = json.load(f)
            else:
                import yaml; user_rules = yaml.safe_load(f)
    inline_rules = {"category": [], "tag": []}
    for cr in (args.category_rule or []):
        ptn, val = cr.split("=", 1)
        inline_rules["category"].append({"pattern": ptn.strip(), "value": val.strip()})
    for tr in (args.tag_rule or []):
        ptn, val = tr.split("=", 1)
        inline_rules["tag"].append({"pattern": ptn.strip(), "value": val.strip()})
    for k in inline_rules:
        if inline_rules[k]:
            user_rules.setdefault(k, []).extend(inline_rules[k])

    enricher = TransactionEnricher(
        rules=user_rules, fi_payee=args.fi_payee, memo_template=args.memo_template,
        auto_dc=args.auto_debitcredit, force_signed=args.force_signed,
        disable_defaults=args.no_default_rules, autocat=args.autocat,
        autocat_backend=args.autocat_backend, autocat_model=args.autocat_model,
        fasttext_model=args.fasttext_model, autocat_labels=args.autocat_labels,
        autocat_threshold=args.autocat_threshold, autocat_report=args.autocat_report,
    )

    enriched = []
    for r in rows:
        enriched_row = enricher.apply_rules(r)
        if enriched_row:
            enriched.append(enriched_row)

    output_path = Path(args.output) if args.output else Path(args.input_file).with_name(
        f"{Path(args.input_file).stem}_quicken.csv"
    )
    
    # ----------------- REPORT QUERY SUPPORT -----------------
    if args.report_query:
        import sys, os, sqlite3, tempfile, shutil, subprocess, code

        try:
            import pandas as pd
        except ImportError:
            raise SystemExit("❌ pandas is required for --report-query")

        rptsys_df = pd.DataFrame(enriched)
        rptsys_query = (args.report_query or "").strip()

        # ----------------- INTERACTIVE MODE -----------------
        if rptsys_query.upper() == "INTERACTIVE":
            rptsys_tmpdb = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
            rptsys_conn = sqlite3.connect(rptsys_tmpdb.name)
            rptsys_df.to_sql("df", rptsys_conn, index=False)
            rptsys_conn.close()

            print("💾 Transactions loaded into temporary SQLite DB (table: df)")
            print(f"⚙️  DB file: {rptsys_tmpdb.name}")
            print("💬 Launching interactive SQL shell (.exit to quit)\n")

            rptsys_sqlite_exe = shutil.which("sqlite3")
            if rptsys_sqlite_exe:
                # Open a new tty so sqlite3 gets real interactive input
                try:
                    os.system(f"{rptsys_sqlite_exe} {rptsys_tmpdb.name} < /dev/tty > /dev/tty")
                except Exception as e:
                    print(f"⚠️  Could not open sqlite3 tty shell: {e}")
            else:
                # fallback: in-process Python REPL
                rptsys_conn = sqlite3.connect(rptsys_tmpdb.name)
                rptsys_cursor = rptsys_conn.cursor()
                rptsys_banner = (
                    "SQLite interactive mode (Python fallback)\n"
                    "Use: cursor.execute('SELECT ...'); print(cursor.fetchall())\n"
                    "Type exit() or Ctrl+D to leave."
                )
                code.interact(banner=rptsys_banner, local={"conn": rptsys_conn, "cursor": rptsys_cursor})
                rptsys_conn.close()

            os.unlink(rptsys_tmpdb.name)
            print("🧹 Temporary DB removed. Goodbye!")
            return

        # ----------------- STATIC QUERY MODE -----------------
        print(f"🧮 Running query:\n{rptsys_query}\n")

        rptsys_backend = "sqlite3"
        try:
            import pandasql
            rptsys_backend = "pandasql"
            print("Using pandasql backend.")
            rptsys_result = pandasql.sqldf(rptsys_query, {"df": rptsys_df})
        except ImportError:
            print("pandasql not available; using sqlite3 backend.")
            rptsys_conn = sqlite3.connect(":memory:")
            rptsys_df.to_sql("df", rptsys_conn, index=False)
            try:
                rptsys_result = pd.read_sql_query(rptsys_query, rptsys_conn)
            except Exception as e:
                rptsys_conn.close()
                raise SystemExit(f"❌ SQLite query failed: {e}")
            rptsys_conn.close()

        rptsys_fmt = (args.report_format or "table").lower()
        rptsys_outpath = args.report_output

        print(f"\n📊 Query result ({rptsys_backend}, format={rptsys_fmt}):\n")

        if rptsys_result is None or rptsys_result.empty:
            print("⚠️ No rows returned.")
        elif rptsys_fmt == "json":
            rptsys_text = rptsys_result.to_json(orient="records", indent=2)
            print(rptsys_text)
            if rptsys_outpath:
                with open(rptsys_outpath, "w", encoding="utf-8") as f:
                    f.write(rptsys_text)
                    print(f"✅ Report written to {rptsys_outpath}")
        elif rptsys_fmt == "csv":
            rptsys_csv = rptsys_result.to_csv(index=False)
            if rptsys_outpath:
                with open(rptsys_outpath, "w", encoding="utf-8") as f:
                    f.write(rptsys_csv)
                    print(f"✅ Report written to {rptsys_outpath}")
            else:
                print(rptsys_csv)
        else:  # "table"
            # minimal ASCII table output, no dependencies
            rptsys_cols = list(rptsys_result.columns)
            rptsys_rows = rptsys_result.values.tolist()
            rptsys_widths = [max(len(str(x)) for x in [col] + [r[i] for r in rptsys_rows]) for i, col in enumerate(rptsys_cols)]
            rptsys_line = "+".join("-" * (w + 2) for w in rptsys_widths)
            print("+" + rptsys_line + "+")
            print("| " + " | ".join(f"{col:<{rptsys_widths[i]}}" for i, col in enumerate(rptsys_cols)) + " |")
            print("+" + rptsys_line + "+")
            for r in rptsys_rows:
                print("| " + " | ".join(f"{str(r[i]):<{rptsys_widths[i]}}" for i in range(len(rptsys_cols))) + " |")
            print("+" + rptsys_line + "+")
            if rptsys_outpath:
                rptsys_result.to_csv(rptsys_outpath, index=False)
                print(f"✅ Report written to {rptsys_outpath}")

        print("\n✅ Report query complete.")
        
        if args.reuseQueryAsNewEnrichedData:
            # If reusing enriched data, we need to convert the DataFrame (df) back into rows.
            if 'df' in locals() and isinstance(df, pd.DataFrame):
                # Convert the DataFrame into a list of rows (list of dictionaries, as rows is expected)
                enriched = df.to_dict(orient="records")  # Convert to a list of dicts (rows)
                print("Reusing DataFrame as new enriched data and converting back into rows.")
            else:
                print("Error: DataFrame 'df' is not available. Returning.")
                return  # Exit the function if 'df' is not available
        else:
            # If reuseQueryAsNewEnrichedData is False, simply return as originally without modifying.
            return
        
    if args.exchange_server_outputsrv:
        # Running the exchange server as a server of the materialization we would otherwise export.
        print(f"Running Exchange Server on port {args.port}")
        config = {
            "data_source": args.exchange_server_data_source,
            "port": args.exchange_server_port,
            "base_url": args.exchange_server_base_url,
            "authz_base": args.exchange_server_authz_base
        }
        run_exchange_server(enriched, config)
        return


    # ----------------- VISUALIZATION -----------------
    if args.budgetviz:
        try:
            import pandas as pd
        except ImportError:
            raise SystemExit("⚠️  Visualization requires pandas. Install with `pip install pandas`.")

        df_viz = pd.DataFrame(enriched)
        viz_output = None if args.budgetviz is True else args.budgetviz
        visualizer = BudgetVisualizer(df_viz, verbose=args.verbose, pd=pd)
        visualizer.render(output_path=viz_output)
        
    # ----------------- RECONCILIATION -----------------
    if args.reconcile:
        rec = Reconciler(
            target_balance=args.reconcile_balance,
            external_file=args.reconcile_against,
            verbose=args.verbose,
        )
        rec.run(enriched)
        
    
    # ----------------- QIFY2K EXPORT -----------------
    if args.qif4:
        output_path = Path(args.output) if args.output else Path(args.input_file).with_name(f"{Path(args.input_file).stem}_qif4.qif")
        write_qif4(rows, output_path, verbose=args.verbose)
        print(f"✅ QIF 4-digit format written to {output_path}")
        return


    # ----------------- BEANCOUNT EXPORT -----------------
    if args.beancount:
        bc_output = (
            Path(args.beancount)
            if args.beancount is not True
            else Path(args.input_file).with_suffix(".beancount")
        )
        write_beancount(
            enriched,
            bc_output,
            account_name=args.account,
            currency=args.beancount_currency,
            verbose=args.verbose,
        )
        print(f"✅ Beancount journal written to {bc_output}")
        return

    
    write_quicken(enriched, output_path, args.compliance)
    enricher.write_autocat_report()

    print(f"✅ Parsed {fmt.upper()} statement ({len(rows)} transactions)")
    if args.autocat:
        print(f"🤖 Autocat backend={args.autocat_backend}, model={args.autocat_model}")
    print(f"✅ Output written to {output_path}")

if __name__ == "__main__":
    main()

