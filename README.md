# dobaos.py

## Installation

```text
pip install dobaos
```

## Usage

```python
# simple example, may be wrong

import time
import dobaos

doba = dobaos.Dobaos()

# description of datapoints
# all
print(doba.get_description(None))
# single
print(doba.get_description(1))
# multiple
print(doba.get_description([1, 2, 3]))

# same for datapoint values: all, single, multiple
print(doba.get_value(None))
print(doba.get_value(1))
print(doba.get_value([1, 2, 3]))

# send read request to bus. datapoints should have "UPDATE" flag
doba.read_value(1)
doba.read_value([1, 2, 3])

# set and send value to bus
doba.set_value({ "id": 1, "value": True })
# multiple values
doba.set_value([{ "id": 1, "value": True }, { "id": 2, "value": False}])

# get BAOS information
print(doba.get_server_items())

# get programming mode info
print(doba.get_progmode())

# set KNX programming mode
doba.set_progmode(1)

# get daemon version
print(doba.get_version())

# send reset request
doba.reset()

# now process incoming messages
while True:
    time.sleep(0.01)
    cast = doba.get_cast()
    if cast:
        print(cast)
```
