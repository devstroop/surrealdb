# syntax=docker/dockerfile:1

# Use the official SurrealDB image
FROM surrealdb/surrealdb:latest

# Run as root so the process can write to mounted volumes like /data
# (compose will not override this unless explicitly set)
USER 0

# Declare the data volume path used by SurrealDB
VOLUME ["/data"]

# Keep the upstream entrypoint and cmd (surreal) intact
