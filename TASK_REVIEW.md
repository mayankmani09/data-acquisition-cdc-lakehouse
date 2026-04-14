# Codebase Issue Review: Proposed Tasks

## 1) Typo fix task (README markdown typo)
**Issue:** The `Quick start` bash snippet opens with `````bash` but never closes the code fence, so the remainder of the README renders incorrectly in many markdown viewers.

**Where observed:** `README.md` lines 127-131.

**Proposed task:** Add a closing triple-backtick after the final command in the quick-start block.

---

## 2) Bug fix task (customer country mapping)
**Issue:** `normalize_source_b_customers` maps every non-`Canada` region to `US`:

```python
F.when(F.col("region") == "Canada", F.lit("CA")).otherwise(F.lit("US"))
```

That collapses unknown/other regions into US and can silently corrupt country-level analytics.

**Where observed:** `src/transforms/bronze_to_silver/customers.py`.

**Proposed task:** Replace hardcoded fallback with an explicit mapping strategy (e.g., `Canada -> CA`, `USA/US -> US`, else `null` or `UNK`) and quarantine unknown values for data-quality follow-up.

---

## 3) Code comment/documentation discrepancy task
**Issue:** The README documents a `products` domain and example table `silver.products_conformed`, but the current code/config only implements customers, orders, and payments.

**Where observed:**
- `README.md` lines 21, 29, and 83 mention products.
- `configs/base.yml` defines domains as only `customers`, `orders`, and `payments`.

**Proposed task:** Either (a) implement the products pipeline end-to-end, or (b) update README scope statements to match the currently implemented domains.

---

## 4) Test improvement task
**Issue:** `test_latest_record_per_key_returns_one_record_per_business_key` only asserts row count (`len(rows) == 1`) and does not assert the chosen record is actually the latest by `event_ts`.

**Where observed:** `tests/unit/test_cdc_merge.py`.

**Proposed task:** Strengthen the test to assert retained field values (e.g., `status == "inactive"`, max timestamp selected), and add a tie-case test for deterministic behavior when timestamps are equal.
