"""R7 -- the read-generation fence stays SERVER-SIDE.

A CI-enforced terminal invariant: no public write API may accept a
client-supplied generation/fence argument. This test IS the boundary, not a
documentation promise. The day a write API needs a generation as input is the
cross-host enforcement step (a client-carried fence token) -- which is out of
scope until demand-pulled, and must fail this test until then. Same discipline
as the plugin's TERMINAL_DENIAL_CLASSES lock.
"""

from __future__ import annotations

import inspect

import pytest

from ccs.adapters.coherent_volume import CoherentVolume
from ccs.agent.runtime import AgentRuntime
from ccs.coordinator.service import CoordinatorService

# Argument names that would indicate a generation/fence VALUE crossing into the
# write path as a client-supplied input.
#
# Note: ``fence_agent_id`` is deliberately NOT here. It is an agent_id known
# server-side on the pessimistic commit() path (not a client-supplied
# generation), and it lives on the internal registry primitive
# ``set_artifact_and_content`` -- not a public write API. The fence's
# ``read_generation`` / ``owner_generation`` are read server-side from the
# registry, never passed in.
_FENCE_ARG_NAMES = frozenset(
    {
        "generation",
        "owner_generation",
        "read_generation",
        "fence",
        "fence_token",
        "fence_generation",
        "expected_generation",
    }
)

# The public, client-facing write APIs. None may take a generation argument.
_PUBLIC_WRITE_APIS = [
    CoordinatorService.write,
    CoordinatorService.commit,
    CoordinatorService.commit_cas,
    AgentRuntime.write,
    AgentRuntime.write_cas,
    CoherentVolume.write,
    CoherentVolume.write_cas,
]


@pytest.mark.parametrize("fn", _PUBLIC_WRITE_APIS, ids=lambda f: f.__qualname__)
def test_no_public_write_api_accepts_a_generation_argument(fn) -> None:
    params = set(inspect.signature(fn).parameters)
    leaked = params & _FENCE_ARG_NAMES
    assert not leaked, (
        f"{fn.__qualname__} accepts a generation/fence argument {leaked}; the "
        "read-generation fence must stay server-side (R7). Promoting it to a "
        "client argument is the cross-host enforcement step -- out of scope "
        "until demand-pulled. Remove the argument or open the cross-host work."
    )
