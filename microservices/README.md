# Interservice communication example

This example includes a simple service that can subscribe to various subjects.

It uses a special broekr written specifically for that. All subject filtering is done on NATS side.


To start the example, run:

```bash
docker compose up --build
```

This will start two apps on ports `8000` and `9000`. Go to /docs of each to send en example task using a dedicated endpoint, send a task and see in logs which service get the task.
