# Alert a channel when a batch fails

The explanation is written for people, so it works as an alert as is.

```python
import json, os, urllib.request
import rowgate as rg

out = rg.validate(df, "contracts/orders.toml", history="quality/orders")
if not out.passed:
    text = out.explanation.text + "\n\n" + out.store.summary()
    req = urllib.request.Request(os.environ["SLACK_WEBHOOK_URL"],
                                 data=json.dumps({"text": text}).encode(),
                                 headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req)
    out.raise_for_errors()
```

The same text works for Microsoft Teams, Discord or email. `out.markdown()` gives the full report as Markdown for tools that render it.
