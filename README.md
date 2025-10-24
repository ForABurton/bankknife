# 🏦🔪 bankknife 

[![Python 3.8+](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Status: Experimental](https://img.shields.io/badge/status-experimental-orange.svg)](#)
[![ETL / Data Tools](https://img.shields.io/badge/type-ETL%20%7C%20Data%20Pipeline-yellow.svg)](#)
[![Finance Utilities](https://img.shields.io/badge/domain-personal%20finance-lightgrey.svg)](#)

**bankknife.py** is a like a personal / end-user-sovereign local ETL data pipeline swiss army knife for your financial statement files, potentially useful in many personal consumer accounting workflows. It reads formats like **CSV (Quicken-like, Bank or Credit "Excel" Style for a particular bank), QIF, OFX, QFX, QBO, Microsoft Money, GnuCash**, and more (e.g. nonstandard CAMT.053 & TXF) then outputs **clean, Quicken-compatible CSVs or Beancount ledgers** with optional categorization, deduplication, visualization, querying, and reconciliation. 

---

## ✨ Key Features

- 🧩 **Multi-format parsing** — Supports CSV, QIF, OFX/QFX/QBO, TXF, MSMoney, GnuCash, Beancount, CAMT.053 XML  
- 🔍 **Automatic format detection** — Detects file type via extension or content signatures  
- 🧠 **Smart enrichment** — Regex and ML-driven transaction categorization and tagging  
- 🧾 **Learning mode** — Learns new regex rules from existing categorized CSVs  
- 🔁 **Merge & dedupe** — Combine multiple statement files and eliminate duplicates  
- 🧮 **SQL reporting** — Query processed transactions with SQL syntax  
- 🧱 **Reconciliation tools** — Validate against expected balances or existing registers  
- 📊 **Visualization** — Generate budget and spending reports (pie charts, inflow/outflow graphs)  
- 🌐 **Very Experimental / Unimplemented: interchange API** — Act as a mock financial data client as an input, or a server to your other finance apps as a sink/output

---

## 🚀 Quick Start

```bash
# Clone and install dependencies
git clone https://github.com/yourusername/bankknife.git
cd bankknife

# Install requirements.txt if you choose (customize for your use of features, possibly use uv or a virtualenv)
#pip install -r requirements.txt

# Basic conversion
python3 bankknife.py statement.qfx -a "BofA Checking" -o output.csv
```

This:
- Parses and converts your `.qfx` statement  
- Detects the input format automatically  
- Tags transactions with the account name `"BofA Checking"`  
- Outputs a standardized CSV ready for Quicken import

---

## 🧩 Example Usage

### Categorize and enrich transactions
```bash
python3 bankknife.py statement.csv -a "Chase Visa" \
  --rules my_rules.json \
  --auto-debitcredit \
  --autocat --autocat-backend fasttext \
  --fasttext-model models/expenses.ftz
```

### Merge multiple statements and deduplicate
```bash
python3 bankknife.py jan.qfx feb.qfx mar.qfx -a "BofA Checking" \
  --merge jan.qfx feb.qfx mar.qfx --dedupe --dedupe-strategy fuzzy -o merged.csv
```

### Run a spending report query
```bash
python3 bankknife.py data.csv -a "BofA" \
  --report-query "SELECT category, SUM(amount) FROM transactions GROUP BY category" \
  --report-format table
```

### Learn new category rules
```bash
python3 bankknife.py --learn-from categorized.csv > new_rules.json
```

---

## 📚 Categorization Rules

You can create regex-based rule files for categories and tags.

Example `my_rules.json`:

```json
{
  "rules": [
    { "pattern": "SAFEWAY|WHOLE FOODS", "category": "Groceries" },
    { "pattern": "SHELL|CHEVRON", "category": "Fuel" },
    { "pattern": "UBER|LYFT", "category": "Transportation" }
  ]
}
```

---

## ⚙️ Full Command-Line Reference

```text
usage: bankknife.py [-h] -a ACCOUNT [-o OUTPUT]
                    [--input-type {auto,csv,qif,ofx,qfx,qbo,msmoney,txf}]
                    [--delimiter {,,tab}] [--compliance {loose,standard,strict}]
                    [--auto-debitcredit] [--force-signed]
                    [--rules RULES] [--category-rule CATEGORY_RULE] [--tag-rule TAG_RULE]
                    [--fi-payee FI_PAYEE] [--memo-template MEMO_TEMPLATE]
                    [--no-default-rules] [--autocat]
                    [--autocat-backend {sentence,fasttext}]
                    [--autocat-model AUTOCAT_MODEL] [--fasttext-model FASTTEXT_MODEL]
                    [--autocat-labels AUTOCAT_LABELS] [--autocat-threshold AUTOCAT_THRESHOLD]
                    [--autocat-report AUTOCAT_REPORT] [--learn-from LEARN_FROM]
                    [--merge [MERGE ...]] [--dedupe]
                    [--dedupe-strategy {strict,fuzzy}] [--max-date-drift MAX_DATE_DRIFT]
                    [-v] [--report-query REPORT_QUERY] [--report-output REPORT_OUTPUT]
                    [--report-format {table,csv,json}] [--reuseQueryAsNewEnrichedData]
                    [--reconcile] [--reconcile-balance RECONCILE_BALANCE]
                    [--reconcile-against RECONCILE_AGAINST]
                    [--output-format {csv,beancount}] [--beancount [BEANCOUNT]]
                    [--beancount-currency BEANCOUNT_CURRENCY] [--qif4 [QIF4]]
                    [--budgetviz [BUDGETVIZ]]
                    [--exchange-server-outputsrv]
                    [--exchange-server-data-source EXCHANGE_SERVER_DATA_SOURCE]
                    [--exchange-server-port EXCHANGE_SERVER_PORT]
                    [--exchange-server-base-url EXCHANGE_SERVER_BASE_URL]
                    [--exchange-server-authz-base EXCHANGE_SERVER_AUTHZ_BASE]
                    [--exchange-client-inputsrc]
                    [--exchange-client-port EXCHANGE_CLIENT_PORT]
                    [--exchange-client-base-url EXCHANGE_CLIENT_BASE_URL]
                    [--exchange-client-authz-base EXCHANGE_CLIENT_AUTHZ_BASE]
                    input_file
```

---

## 🔍 Advanced Options Breakdown

| Option | Description |
|--------|--------------|
| `--input-type` | Force input type instead of autodetect |
| `--delimiter` | Choose delimiter for CSV (`,` or tab) |
| `--compliance` | Set parser strictness (loose / standard / strict) |
| `--auto-debitcredit` | Infer debit/credit automatically |
| `--force-signed` | Force signed amounts |
| `--rules`, `--category-rule`, `--tag-rule` | Apply categorization/tagging regex files |
| `--fi-payee` | Override payee field with FI-provided payee |
| `--memo-template` | Custom memo text template |
| `--no-default-rules` | Disable built-in regex rules |
| `--autocat` | Enable ML-based categorization |
| `--autocat-backend` | Choose backend: `sentence` or `fasttext` |
| `--autocat-model`, `--fasttext-model` | Path to model files |
| `--autocat-labels` | Label mapping file |
| `--autocat-threshold` | Confidence threshold for autocat |
| `--autocat-report` | Save autocat report summary |
| `--learn-from` | Learn regex rules from categorized CSV |
| `--merge` | Merge multiple input files before processing |
| `--dedupe` | Enable deduplication |
| `--dedupe-strategy` | Strategy: strict (ID/date/amount) or fuzzy (similar text) |
| `--max-date-drift` | Allowable date variance for deduplication |
| `--report-query` | Run SQL-like queries on processed data |
| `--report-format` | Output query as table, CSV, or JSON |
| `--reconcile` | Run reconciliation checks |
| `--reconcile-balance` | Expected ending balance |
| `--reconcile-against` | Compare against existing register CSV |
| `--output-format` | `csv` (default) or `beancount` |
| `--beancount` | Write a Beancount journal (optionally specify output) |
| `--beancount-currency` | Currency for Beancount output (default: USD) |
| `--qif4` | Export to QIF 4-digit date format |
| `--budgetviz` | Generate visualization reports (optionally specify output file) |
| `--exchange-server-*` | Run as experimental interchange server |
| `--exchange-client-*` | Use exchange client mode to fetch remote data |
| `-v, --verbose` | Enable verbose debug output |

---

## 📊 Visualization Example

```bash
python3 bankknife.py statement.qfx -a "BofA" --budgetviz report.pdf
```

Generates:
- Pie chart by category  
- Inflows/outflows over time  
- Balance progression chart  

---

## 🧾 Example Output (CSV)

```csv
Date,Description,Amount,Category,Account,Tags,Memo
2025-03-01,SAFEWAY #3412,-45.67,Groceries,BofA Checking,food,Weekly groceries
2025-03-02,UBER TRIP,-18.50,Transportation,BofA Checking,ride,Airport ride
2025-03-04,APPLE.COM/BILL,-9.99,Subscriptions,BofA Checking,tech,App Store subscription
```

---

## Requirements

Beyond the Python standard library, these are useful for some features:
```
PyYAML
sentence-transformers
fasttext
matplotlib
pandas
pandasql
```

And for client/server use (very embryonic).
```
fastapi
uvicorn
```



---

## Caution


> ⚠️ **Note:** This is extremely experimental. It is not recommended to use this for actual financial transactions, and any use is entirely at your own risk. It was made for the author's personal use, and may not fit your purposes.
> **Disclaimer:** These modules are independent open-source tools created for educational and interoperability testing purposes. They are not affiliated with, endorsed by, or certified by any financial institution or consortium. All product names, logos, and brands mentioned in this project are the property of their respective owners, and use is is for identification and interoperability purposes only and does not imply endorsement, affiliation, sponsorship, or approval by the trademark holders.


---





