# ⚡ Async Redis Debouncer

A graceful Python demonstration of debouncing with Redis and asyncio, designed to show how bursty events can be reduced into a single, clean action.

## ✨ In Motion

```text
⠋ Collecting rapid events...
⠙ Waiting for a quiet window...
⠹ Executing once at the end...
⠸ Done ✨
```

## 🌟 Project Overview

This repository showcases a lightweight debouncing mechanism that prevents duplicate work when many events arrive in a short span of time.

Instead of triggering a callback for every incoming event, the system waits for a short pause and then executes only the latest relevant action. This is especially useful in real-time systems where repeated input can create noise, duplication, or unnecessary processing.

The project includes two examples:

- [debouncing.py](debouncing.py) — a basic async debouncer example
- [multiple-servers.py](multiple-servers.py) — a distributed-style simulation showing how debouncing behaves across multiple servers

## 🧠 What Debouncing Does

Debouncing helps ensure that:

- only the final event in a burst is processed,
- repeated actions are suppressed,
- backend workloads stay efficient,
- users experience smoother interactions.

## 🚀 Why This Project Matters

This repository is useful for understanding how to handle repeated user actions and distributed events in a clean, asynchronous way.

It demonstrates a practical pattern used in modern applications where speed and reliability matter.

## 🛠️ Features

- Asynchronous execution with Python `asyncio`
- Redis-backed state management
- Event suppression for repeated triggers
- Simple callback-based processing
- Demo for both single and multi-server scenarios

## 📌 Use Cases

Debouncing is widely used in:

- Search boxes and autocomplete inputs
- Form auto-save systems
- Button click protection to avoid double submissions
- Chat and message sending interfaces
- Real-time dashboards and event streams
- API request throttling and deduplication
- Distributed systems where multiple workers may receive the same event

## ▶️ How to Run

1. Make sure Redis is running locally.
2. Install the required Python package:

```bash
pip install redis
```

3. Run the simple example:

```bash
python debouncing.py
```

4. Run the multi-server simulation:

```bash
python multiple-servers.py
```

## 🧪 Example Behavior

When several events happen quickly, only the last meaningful one is allowed to trigger the callback after the waiting period.

This makes the system ideal for reducing unnecessary database writes, duplicate commands, or repeated business logic execution.

## 💡 Summary

This project is a compact and elegant example of how to build a resilient debounce strategy using Python and Redis. It highlights the importance of controlling event storms and ensuring that only meaningful actions are executed.
