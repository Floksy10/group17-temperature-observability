# Initial architecture

The first milestone runs the complete solution on the group VM. The public REST API listens on port `3003`.

The Kafka consumer, REST API, database, and monitoring components remain separate so they can be scaled independently. Consumer instances will share a Kafka consumer group. Persistent state will live in the database rather than container memory.

Additional personal VMs may later run stateless consumer or API replicas if the course staff permits using them for the group assignment. The group VM remains the public entry point required by the demo instructions.
