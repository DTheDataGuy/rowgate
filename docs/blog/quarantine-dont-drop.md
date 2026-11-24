# Quarantine, don't drop

Somewhere in Seattle's public building permit data there's a permit with an estimated cost of **$25,000,000,000**. That's twenty-five billion dollars, nearly three times the [city's entire $8.9 billion 2026 budget](https://www.seattle.gov/city-budget-office/current-budgets), attached to a single construction permit.

It's almost certainly a typo. The interesting question is what your pipeline does when it meets that row.

## The three things pipelines usually do

**Fail the whole batch.** One bad row out of 454 and nothing loads. The dashboard goes stale, someone gets paged, and the fix is to make a call to the city.

**Drop the row.** A `filter()` quietly removes it. The batch loads and the numbers look fine. Nobody ever finds out that a permit is missing, and when the city corrects the source, nothing brings it back.

**Load it anyway.** Now the average permit cost for that neighborhood is off by a factor of a thousand, and a commissioner is reading it in a briefing.

None of these is a good answer. There's a fourth option that hospitals, airports and factories have used for a long time.

## Quarantine

Hold the bad row to one side, write down exactly why it was held, load everything else, and keep track of it until someone deals with it.

That's what [rowgate](https://github.com/DTheDataGuy/rowgate) does for Polars pipelines:

```python
import rowgate as rg

contract = rg.Contract.from_toml("permits.toml")
valid, quarantine = contract.split(permits)
```

On a 454-row sample of the [Seattle permits data](https://data.seattle.gov/Built-Environment/Building-Permits/76t5-zqzr), 447 rows go straight through and 7 are held. Each held row lists every rule it broke, not just the first:

| permit_num | why it was held |
|---|---|
| `6973063-CN` | `est_cost:max`, $25,000,000,000 is 5 times the $5B limit |
| `6069714-CN` | `issued_on_or_after_applied`, issued 2003-05-21 but applied for 2003-08-21 |
| `6070969-CN`, `6074822-CN` | `issued_on_or_after_applied` |
| `6112685-EX-002`, `6478457-EX-002`, `6544003-EX-002` | `permit_num:pattern` |

rowgate also checks its own work: `447 valid + 7 quarantined = 454 input`. No row disappears without a record.

## Not every failure is a data problem

Look at those last three. They all have the same shape, a sub-permit suffix like `-EX-002`. rowgate notices that too:

```text
Where to start:
  - Every permit_num value that failed the pattern has the same shape (9999999-AA-999).
    If these are valid, widen the pattern instead of fixing the rows.
```

That's the right call. Sub-permits are a real part of how Seattle numbers permits, so the pattern was too strict. Quarantine gives you a chance to learn this. Dropping the rows hides it, and failing the batch makes it urgent at 2 a.m.

## Quarantine needs a memory

A quarantine table that's rebuilt from scratch every run is just a fancier error log. The useful question is never "what failed today?" It's "what has been failing all week, and has anyone looked at it?"

rowgate 0.2 keeps a history:

```python
store = rg.QuarantineStore("quality/history")
store.record(quarantine, run_id="2026-11-20")
print(store.summary())
```

```text
quarantine history: 3 runs since 2026-11-18. 3 rows are in quarantine; 3 have been failing for 3+ days.
Most common open reasons: issued_on_or_after_applied (3).
Last run: 0 new, 3 still failing, 3 fixed.
```

When someone fixes a row upstream, `rowgate replay` runs it back through the contract and marks it resolved. Over three days the $25 billion typo was corrected, the sub-permit pattern was widened, and the three permits issued before they were applied for are still there. Those three need a conversation with the people who own the data, and now that's obvious.

## Start from your data, not a blank file

Writing a contract by hand is the part people skip. So rowgate can draft one:

```bash
rowgate infer permits.parquet > permits.toml
```

On the Seattle sample, the draft spotted two of the three problems on its own. It flagged the three `-EX-002` values as not matching the usual shape, and it suggested the `issued_date >= applied_date` rule because that held for all but 3 of 405 rows. You review the draft and keep what's true.

## It fits where you already work

- **CI:** a GitHub Action that runs the contract on every pull request and posts the report.
- **dbt:** turn a contract into `schema.yml` tests, or check a model's output directly.
- **Dagster, Airflow and Prefect:** a contract check as an asset check, a task or a flow step.
- **Any frame:** pandas, DuckDB and Arrow inputs work as well as Polars.

## Is it fast?

On a clean batch, which is what most batches are, rowgate validated 5 million rows in 0.2 seconds with 44 MB of extra memory. That's about 3 times faster than Pandera and Pointblank and about a tenth of the memory. On a batch with bad rows it's about 25% slower than both, because it returns more: the full valid table plus every bad row with all of its reasons. The [benchmark page](../benchmark.md) has the method and the raw numbers, including where rowgate loses.

## Try it

```bash
pip install rowgate
rowgate infer your_data.parquet
```

rowgate is MIT licensed, written by [The Data Guys™](https://github.com/DTheDataGuy), and the [source is on GitHub](https://github.com/DTheDataGuy/rowgate). If it saves you from a $25 billion row, a star helps other people find it.
