# System Overview

```mermaid
flowchart TB
  subgraph Current["Current implementation"]
    F["FastAPI shell"]
    R["EventBus / module registry / tool registry (memory)"]
    U["React status console"] --> F --> R
  end
  subgraph Planned["Planned governed vertical slice"]
    S["MQTT source"] --> O["Bounded observation"] --> C["Durable stream catalog"]
    C --> D["Governance and review evidence"]
  end
  subgraph Future["Optional future"]
    K["Knowledge-graph projection after query validation"]
    X["Deferred research modules"]
  end
  R -. "future implementation work" .-> S
  D -.-> K
  D -.-> X
```

PostgreSQL and Mosquitto support the current R1 slice, and accepted normalized observations are delivered from the PostgreSQL outbox to InfluxDB 2.x. ChromaDB remains Compose-provisioned only and is outside the delivery path.
