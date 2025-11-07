# Azure Service Bus Peek Tool

A small Python CLI to peek (non-destructive read) messages from an Azure Service Bus queue and its Dead-Letter Queue (DLQ) using a SAS connection string.

## Features

* Peek from the active queue.
* Peek from the dead letter queue.

## Requirements

* Python 3.9+ recommended.
* azure-servicebus==7.12.3

## Installation

```
# (optional) create and activate a virtual environment
python3 -m venv .venv
. .venv/bin/activate  # Windows: .venv\Scripts\activate

# install dependency
pip install -r requirements.txt
```

## Usage

```
python azure_sb_peek.py "<connection-string>" [--max N] [--queue NAME] [--active-only | --dlq-only]
```

## Examples

### 1) Connection string with EntityPath

```
python azure_sb_peek.py "Endpoint=sb://mybus.servicebus.windows.net/;SharedAccessKeyName=read;SharedAccessKey=ABC...=;EntityPath=my-queue" --max 20
```

### 2) Connection string without EntityPath
```
python azure_sb_peek.py "Endpoint=sb://mybus.servicebus.windows.net/;SharedAccessKeyName=read;SharedAccessKey=ABC...=" --queue my-queue
```

### 3) DLQ only

```
python azure_sb_peek.py "$(cat conn.txt)" --dlq-only --max 100
```

### 4) Active only

```
python azure_sb_peek.py "$(cat conn.txt)" --active-only
```


