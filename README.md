# What's `diskcache` ?

- SQLite3 based cache. Read: [Here](https://pypi.org/project/diskcache/)

### Install it:
```
pip install diskcache
```

---
# What's this wrapper?

- X: More on this: Read: [Here](https://zadzmo.org/nepenthes-demo/)
- Safe wrapper on `diskcache` with slightly enhanced retry logic via exponential backoffing/retring.
- No `failed` queues, distributed schedulers and retry logic.
