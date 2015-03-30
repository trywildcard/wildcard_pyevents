# Wildcard Pyevents

> Wildcard Pyevents is a python package to do event logging at Wildcard.


# Examples

Here's a basic example:

```python
from wildcard_pyevents import client as wildcard_pyevents_client

events_client = wildcard_pyevents_client.WildcardPyEventsClient("localhost", 
"etang-local", influx_username="root", influx_password="root")

events_client.send("test_event", {"key1": "value1", "key2": 5, "key3": True})
```
