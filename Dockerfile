# Dockerfile for the stale-write-guard-fs MCP server.
#
# Consumed by the Glama MCP directory, which builds and runs the image to
# introspect the server (its coherence / tool-definition score requires a
# runnable release, not just a repo scan). Not part of the pip install path —
# end users run `uvx --from "agent-coherence[mcp]" stale-write-guard-fs`.
#
# Builds the `mcp` extra from source and launches the stdio server via the
# console script from pyproject.toml:
#   [project.scripts] stale-write-guard-fs = "ccs.mcp.server:main"
FROM python:3.12-slim

WORKDIR /app
COPY . /app

# The `mcp` extra is the only new runtime dep (official MCP SDK, pinned <2).
# Version is a static attr (ccs.__version__), so no .git is needed to build.
RUN pip install --no-cache-dir ".[mcp]"

# stdio transport: Glama starts the process and reads tools/list to score it.
ENTRYPOINT ["stale-write-guard-fs"]
