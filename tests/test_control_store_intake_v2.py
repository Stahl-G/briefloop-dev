from __future__ import annotations

import ast
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from multi_agent_brief.contracts.v2 import (
    ArtifactSubmitRequest,
    EventEnvelope,
    Invocation,
    RunIdentity,
    SourceCommitRequest,
    SourcePackCommitRequest,
    StageState,
    SourceProposal,
    WorkspaceRunHead,
)
from multi_agent_brief.control_store import (
    ControlStoreCommitOutcomeUnknown,
    ControlStoreIntegrityError,
    SQLiteControlStore,
)
from multi_agent_brief.control_store.serialization import canonical_fingerprint
from multi_agent_brief.intake_v2.errors import IntakeError, IntakeResult
from multi_agent_brief.intake_v2.service import (
    IntakeService,
    _SourcePackBytes,
    _SourcePackMemberBytes,
)
from multi_agent_brief.intake_v2.policy import (
    SourcePolicyError,
    evaluate_source_eligibility,
)


RUN_ID = "RUN-PR3-001"
WORKSPACE_ID = "WS-PR3-001"
NOW = "2026-07-15T12:00:00Z"
CLOCK = lambda: datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
ROOT = Path(__file__).parents[1]


def _record(model_type, **values):
    return model_type.model_validate(
        {"schema_version": model_type.schema_id, **values},
        strict=True,
    )


def _by_invocation(snapshot, invocation_id: str):
    return next(
        item for item in snapshot.invocations if item.invocation_id == invocation_id
    )


def _write_json(path: Path, payload: dict[str, object]) -> bytes:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return data


def _seed_workspace(workspace: Path, *, include_head: bool = True) -> None:
    workspace.mkdir()
    with SQLiteControlStore.create(
        workspace / "briefloop.db",
        workspace_id=WORKSPACE_ID,
        clock=CLOCK,
    ) as store:
        unit = store.begin(RUN_ID, "TX-SEED-001", "private_test_seed", 0)
        unit.put_run(
            _record(
                RunIdentity,
                run_id=RUN_ID,
                workspace_id=WORKSPACE_ID,
                runtime="operator",
                created_at=NOW,
            )
        )
        if include_head:
            unit.put_workspace_run_head(
                _record(
                    WorkspaceRunHead,
                    workspace_id=WORKSPACE_ID,
                    current_run_id=RUN_ID,
                    updated_at=NOW,
                )
            )
        for stage_id in (
            "source-discovery",
            "scout",
            "screener",
            "claim-ledger",
            "auditor",
        ):
            unit.put_stage_state(
                _record(
                    StageState,
                    run_id=RUN_ID,
                    stage_id=stage_id,
                    status="ready",
                    revision=0,
                    updated_at=NOW,
                )
            )
        for invocation_id, role_id in (
            ("INV-SOURCE-001", "source-provider"),
            ("INV-SCOUT-001", "scout"),
            ("INV-SCREEN-001", "scout"),
            ("INV-SCREENER-001", "screener"),
            ("INV-DRAFTS-001", "claim-ledger"),
            ("INV-AUDIT-001", "auditor"),
        ):
            unit.put_invocation(
                _record(
                    Invocation,
                    invocation_id=invocation_id,
                    run_id=RUN_ID,
                    role_id=role_id,
                    runtime="operator",
                    status="active",
                    started_at=NOW,
                )
            )
        unit.commit()


def _source_request(workspace: Path, *, expected_revision: int = 1) -> Path:
    scratch = workspace / "scratch" / "INV-SOURCE-001"
    content = b"Synthetic public filing bytes.\n"
    content_path = scratch / "source_content.pdf"
    content_path.parent.mkdir(parents=True, exist_ok=True)
    content_path.write_bytes(content)
    _write_json(
        scratch / "source_proposal.json",
        {
            "schema_version": "briefloop.source_proposal.v2",
            "proposal_id": "PROP-SOURCE-001",
            "run_id": RUN_ID,
            "source_id": "SRC-001",
            "origin_type": "uploaded_file",
            "acquisition_method": "manual_upload",
            "material_kind": "uploaded_file",
            "provider": None,
            "locator": {
                "kind": "file",
                "path": "scratch/INV-SOURCE-001/source_content.pdf",
            },
            "title": "Synthetic public filing",
            "publisher": None,
            "published_at": None,
            "retrieved_at": NOW,
            "source_category": "regulator",
            "retrieval_source_type": "local_file",
            "underlying_evidence_type": "filing",
            "raw_underlying_evidence_type": None,
            "content_sha256": hashlib.sha256(content).hexdigest(),
            "content_media_type": "application/pdf",
            "raw_payload_sha256": None,
            "raw_payload_media_type": None,
        },
    )
    request = scratch / "submit_request.json"
    _write_json(
        request,
        {
            "schema_version": "briefloop.source_commit_request.v2",
            "request_id": "REQ-SOURCE-001",
            "run_id": RUN_ID,
            "invocation_id": "INV-SOURCE-001",
            "proposal_path": "scratch/INV-SOURCE-001/source_proposal.json",
            "content_path": "scratch/INV-SOURCE-001/source_content.pdf",
            "raw_payload_path": None,
            "expected_store_revision": expected_revision,
        },
    )
    return request


def _source_pack_request(
    workspace: Path,
    *,
    expected_revision: int = 1,
) -> tuple[Path, bytes, bytes, bytes, bytes]:
    scratch = workspace / "scratch" / "INV-SOURCE-001"
    member_root = scratch / "sources" / "SRC-PACK-001"
    manifest = b'{"schema_version":"example.source_manifest.v1","sources":[]}'
    content = b"Durable source-pack content A.\n"
    raw = b'{"provider":"synthetic","version":"A"}'
    manifest_sha = hashlib.sha256(manifest).hexdigest()
    proposal = _write_json(
        member_root / "source_proposal.json",
        {
            "schema_version": "briefloop.source_proposal.v2",
            "proposal_id": "PROP-SOURCE-PACK-001",
            "run_id": RUN_ID,
            "source_id": "SRC-PACK-001",
            "origin_type": "provider_response",
            "acquisition_method": "provider_extract",
            "material_kind": "full_content",
            "provider": "synthetic-provider",
            "locator": {"kind": "web", "url": "https://example.com/source-pack"},
            "title": "Synthetic source pack",
            "publisher": "Example Publisher",
            "published_at": "2026-07-14",
            "retrieved_at": NOW,
            "source_category": "market_report",
            "retrieval_source_type": "paper_page",
            "underlying_evidence_type": "market_data",
            "raw_underlying_evidence_type": "research-report",
            "content_sha256": hashlib.sha256(content).hexdigest(),
            "content_media_type": "text/plain",
            "raw_payload_sha256": hashlib.sha256(raw).hexdigest(),
            "raw_payload_media_type": "application/json",
            "source_manifest_sha256": manifest_sha,
        },
    )
    (member_root / "source_content.txt").write_bytes(content)
    (member_root / "source_raw.json").write_bytes(raw)
    (scratch / "source_manifest.json").write_bytes(manifest)
    request = scratch / "submit_request.json"
    _write_json(
        request,
        {
            "schema_version": "briefloop.source_pack_commit_request.v2",
            "request_id": "REQ-SOURCE-PACK-001",
            "run_id": RUN_ID,
            "invocation_id": "INV-SOURCE-001",
            "members": [
                {
                    "member_id": "SRC-PACK-001",
                    "proposal_path": (
                        "scratch/INV-SOURCE-001/sources/SRC-PACK-001/"
                        "source_proposal.json"
                    ),
                    "content_path": (
                        "scratch/INV-SOURCE-001/sources/SRC-PACK-001/"
                        "source_content.txt"
                    ),
                    "raw_payload_path": (
                        "scratch/INV-SOURCE-001/sources/SRC-PACK-001/"
                        "source_raw.json"
                    ),
                }
            ],
            "manifest_path": "scratch/INV-SOURCE-001/source_manifest.json",
            "expected_manifest_sha256": manifest_sha,
            "expected_store_revision": expected_revision,
        },
    )
    return request, manifest, proposal, content, raw


def _candidate_request(workspace: Path, *, expected_revision: int = 2) -> Path:
    scratch = workspace / "scratch" / "INV-SCOUT-001"
    _write_json(
        scratch / "candidate_claims.json",
        {
            "schema_version": "briefloop.candidate_claims_proposal.v2",
            "proposal_id": "PROP-CANDIDATES-001",
            "run_id": RUN_ID,
            "created_at": NOW,
            "candidates": [
                {
                    "candidate_id": "CAND-001",
                    "source_id": "SRC-001",
                    "statement": "A synthetic public filing was supplied.",
                    "evidence_text": "Synthetic public filing bytes.",
                    "topic": "operations",
                    "claim_type": "fact",
                    "confidence": "high",
                }
            ],
        },
    )
    request = scratch / "submit_request.json"
    _write_json(
        request,
        {
            "schema_version": "briefloop.artifact_submit_request.v2",
            "request_id": "REQ-CANDIDATE-001",
            "run_id": RUN_ID,
            "artifact_id": "candidate_claims",
            "invocation_id": "INV-SCOUT-001",
            "input_path": "scratch/INV-SCOUT-001/candidate_claims.json",
            "expected_store_revision": expected_revision,
            "expected_artifact_revision": 0,
        },
    )
    return request


def _snippet_source_request(workspace: Path, *, expected_revision: int = 1) -> Path:
    scratch = workspace / "scratch" / "INV-SOURCE-001"
    content = b"Discovery snippet only."
    raw = b'{"results":[]}'
    scratch.mkdir(parents=True, exist_ok=True)
    (scratch / "source_content.txt").write_bytes(content)
    (scratch / "source_raw.json").write_bytes(raw)
    _write_json(
        scratch / "source_proposal.json",
        {
            "schema_version": "briefloop.source_proposal.v2",
            "proposal_id": "PROP-SOURCE-SNIPPET",
            "run_id": RUN_ID,
            "source_id": "SRC-SNIPPET",
            "origin_type": "provider_response",
            "acquisition_method": "provider_search",
            "material_kind": "search_snippet",
            "provider": "synthetic-provider",
            "locator": {"kind": "web", "url": "https://example.com/source"},
            "title": "Synthetic discovery snippet",
            "publisher": None,
            "published_at": None,
            "retrieved_at": NOW,
            "source_category": "other",
            "retrieval_source_type": "other",
            "underlying_evidence_type": "unknown",
            "raw_underlying_evidence_type": "provider-search-response",
            "content_sha256": hashlib.sha256(content).hexdigest(),
            "content_media_type": "text/plain",
            "raw_payload_sha256": hashlib.sha256(raw).hexdigest(),
            "raw_payload_media_type": "application/json",
        },
    )
    request = scratch / "submit_request.json"
    _write_json(
        request,
        {
            "schema_version": "briefloop.source_commit_request.v2",
            "request_id": "REQ-SOURCE-SNIPPET",
            "run_id": RUN_ID,
            "invocation_id": "INV-SOURCE-001",
            "proposal_path": "scratch/INV-SOURCE-001/source_proposal.json",
            "content_path": "scratch/INV-SOURCE-001/source_content.txt",
            "raw_payload_path": "scratch/INV-SOURCE-001/source_raw.json",
            "expected_store_revision": expected_revision,
        },
    )
    return request


def _proposal_request(
    workspace: Path,
    *,
    invocation_id: str,
    request_id: str,
    artifact_id: str,
    payload: dict[str, object],
    expected_store_revision: int,
    expected_artifact_revision: int,
) -> Path:
    scratch = workspace / "scratch" / invocation_id
    proposal_path = scratch / f"{artifact_id}.json"
    _write_json(proposal_path, payload)
    request = scratch / "submit_request.json"
    _write_json(
        request,
        {
            "schema_version": "briefloop.artifact_submit_request.v2",
            "request_id": request_id,
            "run_id": RUN_ID,
            "artifact_id": artifact_id,
            "invocation_id": invocation_id,
            "input_path": proposal_path.relative_to(workspace).as_posix(),
            "expected_store_revision": expected_store_revision,
            "expected_artifact_revision": expected_artifact_revision,
        },
    )
    return request


def _replace_seed_context(workspace: Path, case: str) -> None:
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        if case == "run_not_current":
            raise AssertionError("run_not_current requires a Core v2 reset fixture")
        else:
            unit = store.begin(
                RUN_ID,
                f"TX-CONTEXT-{case.upper()}",
                "private_test_context",
                1,
            )
            if case == "invocation_not_active":
                unit.put_invocation(
                    _record(
                        Invocation,
                        invocation_id="INV-SOURCE-001",
                        run_id=RUN_ID,
                        role_id="source-provider",
                        runtime="operator",
                        status="completed",
                        started_at=NOW,
                        completed_at=NOW,
                    )
                )
            elif case == "invocation_role_mismatch":
                unit.put_invocation(
                    _record(
                        Invocation,
                        invocation_id="INV-SOURCE-001",
                        run_id=RUN_ID,
                        role_id="scout",
                        runtime="operator",
                        status="active",
                        started_at=NOW,
                    )
                )
            elif case == "stage_not_ready":
                unit.put_stage_state(
                    _record(
                        StageState,
                        run_id=RUN_ID,
                        stage_id="source-discovery",
                        status="blocked",
                        revision=1,
                        updated_at=NOW,
                    )
                )
            elif case == "run_archived":
                unit.append_event(
                    _record(
                        EventEnvelope,
                        event_id="EVT-ARCHIVED-001",
                        run_id=RUN_ID,
                        event_type="run_archived",
                        created_at=NOW,
                        actor="system",
                        transaction_id="TX-CONTEXT-RUN_ARCHIVED",
                        stage_id="finalize",
                        decision="archived",
                        reason="",
                        metadata={},
                    )
                )
            else:
                raise AssertionError(case)
        unit.commit()


def test_source_and_candidate_commit_form_first_class_receipt_graph(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    service = IntakeService(workspace, clock=CLOCK)

    source = service.submit_source(
        _source_request(workspace).relative_to(workspace).as_posix()
    )
    candidate = service.submit_proposal(
        "candidate",
        _candidate_request(workspace).relative_to(workspace).as_posix(),
    )

    assert source.status == "committed"
    assert source.receipt is not None
    assert source.receipt.source_ids == ["SRC-001"]
    assert candidate.status == "committed"
    assert candidate.receipt is not None
    assert candidate.receipt.proposal_ids == ["PROP-CANDIDATES-001"]
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        snapshot = store.load_snapshot(RUN_ID)
        assert snapshot.workspace_run_head is not None
        assert snapshot.workspace_run_head.current_run_id == RUN_ID
        assert [item.source_id for item in snapshot.sources] == ["SRC-001"]
        assert [item.proposal_id for item in snapshot.accepted_proposals] == [
            "PROP-CANDIDATES-001"
        ]
        assert [
            (item.proposal_id, item.source_id)
            for item in snapshot.proposal_source_bindings
        ] == [("PROP-CANDIDATES-001", "SRC-001")]
        assert snapshot.store_revision == 3
        backup = store.backup_to(tmp_path / "backup")
    with SQLiteControlStore.open(
        backup / "control.db",
        blob_root=backup / "blobs",
    ) as restored:
        restored_snapshot = restored.load_snapshot(RUN_ID)
        assert restored_snapshot.sources == snapshot.sources
        assert restored_snapshot.accepted_proposals == snapshot.accepted_proposals


def test_discovery_only_source_commits_but_cannot_back_claim_candidate(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    service = IntakeService(workspace, clock=CLOCK)

    source = service.submit_source(
        _snippet_source_request(workspace).relative_to(workspace).as_posix()
    )
    candidate_request = _candidate_request(workspace)
    candidate_payload_path = (
        workspace / "scratch" / "INV-SCOUT-001" / "candidate_claims.json"
    )
    candidate_payload = json.loads(candidate_payload_path.read_text(encoding="utf-8"))
    candidate_payload["candidates"][0]["source_id"] = "SRC-SNIPPET"
    _write_json(candidate_payload_path, candidate_payload)
    candidate = service.submit_proposal(
        "candidate",
        candidate_request.relative_to(workspace).as_posix(),
    )

    assert source.status == "committed"
    assert candidate.status == "rejected_recorded"
    assert candidate.error_code == "source_not_claims_eligible"
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        snapshot = store.load_snapshot(RUN_ID)
        assert snapshot.sources[0].claims_eligible is False
        assert snapshot.sources[0].eligibility_reason == "ineligible_search_snippet"
        assert snapshot.accepted_proposals == ()


@pytest.mark.parametrize(
    ("relation_table", "delete_trigger"),
    [
        ("transaction_sources", "transaction_sources_no_delete"),
        ("transaction_proposals", "transaction_proposals_no_delete"),
    ],
)
def test_open_rejects_source_or_proposal_without_reverse_receipt_coverage(
    tmp_path: Path,
    relation_table: str,
    delete_trigger: str,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    service = IntakeService(workspace, clock=CLOCK)
    assert service.submit_source(
        _source_request(workspace).relative_to(workspace).as_posix()
    ).status == "committed"
    assert service.submit_proposal(
        "candidate",
        _candidate_request(workspace).relative_to(workspace).as_posix(),
    ).status == "committed"

    database = workspace / "briefloop.db"
    connection = sqlite3.connect(database)
    trigger_sql = connection.execute(
        "SELECT sql FROM sqlite_schema WHERE type = 'trigger' AND name = ?",
        (delete_trigger,),
    ).fetchone()[0]
    connection.execute(f"DROP TRIGGER {delete_trigger}")
    connection.execute(f"DELETE FROM {relation_table}")
    connection.execute(trigger_sql)
    connection.commit()
    connection.close()

    with pytest.raises(ControlStoreIntegrityError) as error:
        SQLiteControlStore.open(database)
    assert error.value.code == "transaction_relation_mismatch"


def test_new_authority_rows_and_relations_are_append_only(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    service = IntakeService(workspace, clock=CLOCK)
    service.submit_source(_source_request(workspace).relative_to(workspace).as_posix())
    service.submit_proposal(
        "candidate",
        _candidate_request(workspace).relative_to(workspace).as_posix(),
    )
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        for statement in (
            "UPDATE sources SET title = 'changed'",
            "DELETE FROM accepted_proposals",
            "UPDATE proposal_source_bindings SET source_id = 'OTHER'",
            "DELETE FROM transaction_sources",
            "UPDATE transaction_proposals SET proposal_id = 'OTHER'",
            "DELETE FROM workspace_run_heads",
        ):
            with pytest.raises(sqlite3.IntegrityError, match="append_only"):
                store._connection.execute(statement)


def test_exact_replay_returns_original_receipt_without_new_write(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    service = IntakeService(workspace, clock=CLOCK)
    request = _source_request(workspace).relative_to(workspace).as_posix()

    committed = service.submit_source(request)
    replayed = service.submit_source(request)

    assert committed.status == "committed"
    assert replayed.status == "replayed"
    assert replayed.receipt == committed.receipt
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        assert store.current_revision == 2


def test_host_source_writer_consumes_verified_bytes_without_reopening_paths(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    request_path = _source_request(workspace)
    request = SourceCommitRequest.model_validate_json(
        request_path.read_bytes(), strict=True
    )
    proposal_path = request_path.parent / "source_proposal.json"
    content_path = request_path.parent / "source_content.pdf"
    proposal_a = proposal_path.read_bytes()
    content_a = content_path.read_bytes()
    content_b = b"Replacement bytes that arrived after Host verification.\n"
    proposal_b_payload = json.loads(proposal_a)
    proposal_b_payload["title"] = "Replacement source"
    proposal_b_payload["content_sha256"] = hashlib.sha256(content_b).hexdigest()
    proposal_b = json.dumps(
        proposal_b_payload, sort_keys=True, separators=(",", ":")
    ).encode()
    proposal_path.write_bytes(proposal_b)
    content_path.write_bytes(content_b)

    service = IntakeService(workspace, clock=CLOCK)
    committed = service._submit_source_from_host(
        request,
        proposal_bytes=proposal_a,
        content_bytes=content_a,
        raw_bytes=None,
    )
    replayed = service._submit_source_from_host(
        request,
        proposal_bytes=proposal_a,
        content_bytes=content_a,
        raw_bytes=None,
    )
    conflict = service._submit_source_from_host(
        request,
        proposal_bytes=proposal_b,
        content_bytes=content_b,
        raw_bytes=None,
    )

    assert committed.status == "committed"
    assert replayed.status == "replayed"
    assert replayed.receipt == committed.receipt
    assert conflict.to_dict() == {
        "status": "failed_uncommitted",
        "error_code": "submission_replay_conflict",
    }
    expected_fingerprint = canonical_fingerprint(
        {
            "lane": "source",
            "request": request.model_dump(mode="json", exclude_unset=False),
            "proposal_sha256": hashlib.sha256(proposal_a).hexdigest(),
            "content_sha256": hashlib.sha256(content_a).hexdigest(),
            "raw_payload_sha256": None,
        }
    )
    replacement_sha = hashlib.sha256(content_b).hexdigest()
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        snapshot = store.load_snapshot(RUN_ID)
        history = store.load_history()
        source = snapshot.sources[0]
        assert snapshot.store_revision == 2
        assert source.content_sha256 == hashlib.sha256(content_a).hexdigest()
        assert source.title == "Synthetic public filing"
        assert source.request_fingerprint == expected_fingerprint
        source_event = next(
            item
            for item in snapshot.events
            if item.intake_binding is not None
            and item.intake_binding.source_id == source.source_id
        )
        assert source_event.intake_binding is not None
        assert source_event.intake_binding.request_fingerprint == expected_fingerprint
        assert (workspace / source.content_blob_path).read_bytes() == content_a
        assert replacement_sha not in repr(snapshot)
        assert replacement_sha not in repr(history.transactions)
        assert replacement_sha.encode() not in (workspace / "briefloop.db").read_bytes()


def test_host_source_pack_writer_consumes_complete_verified_pack_without_reopening_paths(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    request_path, manifest_a, proposal_a, content_a, raw_a = _source_pack_request(
        workspace
    )
    request_a = SourcePackCommitRequest.model_validate_json(
        request_path.read_bytes(), strict=True
    )
    pack_a = _SourcePackBytes(
        manifest_bytes=manifest_a,
        members=(
            _SourcePackMemberBytes(
                proposal_bytes=proposal_a,
                content_bytes=content_a,
                raw_bytes=raw_a,
            ),
        ),
    )
    manifest_b = b'{"schema_version":"example.source_manifest.v1","sources":["B"]}'
    content_b = b"Self-consistent replacement source-pack content B.\n"
    raw_b = b'{"provider":"synthetic","version":"B"}'
    proposal_b_payload = json.loads(proposal_a)
    proposal_b_payload.update(
        title="Replacement source pack",
        content_sha256=hashlib.sha256(content_b).hexdigest(),
        raw_payload_sha256=hashlib.sha256(raw_b).hexdigest(),
        source_manifest_sha256=hashlib.sha256(manifest_b).hexdigest(),
    )
    proposal_b = json.dumps(
        proposal_b_payload, sort_keys=True, separators=(",", ":")
    ).encode()
    request_b_payload = request_a.model_dump(mode="json", exclude_unset=False)
    request_b_payload["expected_manifest_sha256"] = hashlib.sha256(
        manifest_b
    ).hexdigest()
    request_path.write_bytes(
        json.dumps(
            request_b_payload, sort_keys=True, separators=(",", ":")
        ).encode()
    )
    request_b = SourcePackCommitRequest.model_validate_json(
        request_path.read_bytes(), strict=True
    )
    scratch = request_path.parent
    member_root = scratch / "sources" / "SRC-PACK-001"
    (scratch / "source_manifest.json").write_bytes(manifest_b)
    (member_root / "source_proposal.json").write_bytes(proposal_b)
    (member_root / "source_content.txt").write_bytes(content_b)
    (member_root / "source_raw.json").write_bytes(raw_b)
    pack_b = _SourcePackBytes(
        manifest_bytes=manifest_b,
        members=(
            _SourcePackMemberBytes(
                proposal_bytes=proposal_b,
                content_bytes=content_b,
                raw_bytes=raw_b,
            ),
        ),
    )

    service = IntakeService(workspace, clock=CLOCK)
    committed = service._submit_source_pack_from_host(request_a, pack_a)
    replayed = service._submit_source_pack_from_host(request_a, pack_a)
    conflict = service._submit_source_pack_from_host(request_b, pack_b)

    assert committed.status == "committed"
    assert replayed.status == "replayed"
    assert replayed.receipt == committed.receipt
    assert conflict.error_code == "submission_replay_conflict"
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        snapshot = store.load_snapshot(RUN_ID)
        source = snapshot.sources[0]
        event = next(
            item
            for item in snapshot.events
            if item.intake_binding is not None
            and item.intake_binding.request_id == request_a.request_id
        )
        assert snapshot.store_revision == 2
        assert source.title == "Synthetic source pack"
        assert source.content_sha256 == hashlib.sha256(content_a).hexdigest()
        assert source.raw_payload_sha256 == hashlib.sha256(raw_a).hexdigest()
        assert source.source_manifest_sha256 == hashlib.sha256(manifest_a).hexdigest()
        assert event.intake_binding is not None
        assert (workspace / source.content_blob_path).read_bytes() == content_a
        assert source.raw_payload_blob_path is not None
        assert (workspace / source.raw_payload_blob_path).read_bytes() == raw_a
        database_bytes = (workspace / "briefloop.db").read_bytes()
        assert content_b not in database_bytes
        assert raw_b not in database_bytes


@pytest.mark.parametrize(
    ("component", "replacement"),
    [
        ("request", b'{"replacement":"request"}'),
        ("manifest", b'{"replacement":"manifest"}'),
        ("proposal", b'{"replacement":"proposal"}'),
        ("content", b"replacement content"),
        ("raw", b'{"replacement":"raw"}'),
    ],
)
def test_host_source_pack_each_materialized_component_is_non_authoritative(
    tmp_path: Path,
    component: str,
    replacement: bytes,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    request_path, manifest, proposal, content, raw = _source_pack_request(workspace)
    request = SourcePackCommitRequest.model_validate_json(
        request_path.read_bytes(), strict=True
    )
    paths = {
        "request": request_path,
        "manifest": workspace / str(request.manifest_path),
        "proposal": workspace / request.members[0].proposal_path,
        "content": workspace / request.members[0].content_path,
        "raw": workspace / str(request.members[0].raw_payload_path),
    }
    paths[component].write_bytes(replacement)
    result = IntakeService(
        workspace, clock=CLOCK
    )._submit_source_pack_from_host(
        request,
        _SourcePackBytes(
            manifest_bytes=manifest,
            members=(
                _SourcePackMemberBytes(
                    proposal_bytes=proposal,
                    content_bytes=content,
                    raw_bytes=raw,
                ),
            ),
        ),
    )
    assert result.status == "committed"
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        source = store.load_snapshot(RUN_ID).sources[0]
    assert source.content_sha256 == hashlib.sha256(content).hexdigest()
    assert source.raw_payload_sha256 == hashlib.sha256(raw).hexdigest()
    assert (workspace / source.content_blob_path).read_bytes() == content
    assert source.raw_payload_blob_path is not None
    assert (workspace / source.raw_payload_blob_path).read_bytes() == raw


@pytest.mark.parametrize(
    ("case", "expected_error"),
    [
        ("manifest_hash", "source_hash_mismatch"),
        ("malformed_proposal", "proposal_contract_invalid"),
        ("proposal_manifest", "proposal_contract_invalid"),
        ("content_hash", "source_hash_mismatch"),
        ("raw_hash", "source_hash_mismatch"),
        ("cross_run", "proposal_contract_invalid"),
    ],
)
def test_host_source_pack_invalid_supplied_bytes_are_typed_and_zero_write(
    tmp_path: Path,
    case: str,
    expected_error: str,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    request_path, manifest, proposal, content, raw = _source_pack_request(workspace)
    request = SourcePackCommitRequest.model_validate_json(
        request_path.read_bytes(), strict=True
    )
    proposal_payload = json.loads(proposal)
    if case == "manifest_hash":
        manifest = b"wrong manifest bytes"
    elif case == "malformed_proposal":
        proposal = b"{}"
    elif case == "proposal_manifest":
        proposal_payload["source_manifest_sha256"] = "0" * 64
        proposal = json.dumps(proposal_payload).encode()
    elif case == "content_hash":
        content = b"wrong content bytes"
    elif case == "raw_hash":
        raw = b"wrong raw bytes"
    elif case == "cross_run":
        proposal_payload["run_id"] = "RUN-OTHER"
        proposal = json.dumps(proposal_payload).encode()
    else:
        raise AssertionError(case)
    result = IntakeService(
        workspace, clock=CLOCK
    )._submit_source_pack_from_host(
        request,
        _SourcePackBytes(
            manifest_bytes=manifest,
            members=(
                _SourcePackMemberBytes(
                    proposal_bytes=proposal,
                    content_bytes=content,
                    raw_bytes=raw,
                ),
            ),
        ),
    )
    assert result.to_dict() == {
        "status": "failed_uncommitted",
        "error_code": expected_error,
    }
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        snapshot = store.load_snapshot(RUN_ID)
        assert store.current_revision == 1
    assert snapshot.sources == ()


def test_host_proposal_writer_consumes_verified_bytes_without_reopening_paths(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    service = IntakeService(workspace, clock=CLOCK)
    source_request = _source_request(workspace).relative_to(workspace).as_posix()
    assert service.submit_source(source_request).status == "committed"
    request_path = _candidate_request(workspace)
    request = ArtifactSubmitRequest.model_validate_json(
        request_path.read_bytes(), strict=True
    )
    proposal_path = request_path.parent / "candidate_claims.json"
    proposal_a = proposal_path.read_bytes()
    proposal_b_payload = json.loads(proposal_a)
    proposal_b_payload["candidates"][0]["statement"] = (
        "Replacement statement written after Host verification."
    )
    proposal_b = json.dumps(
        proposal_b_payload, sort_keys=True, separators=(",", ":")
    ).encode()
    proposal_path.write_bytes(proposal_b)

    committed = service._submit_proposal_from_host("candidate", request, proposal_a)
    replayed = service._submit_proposal_from_host("candidate", request, proposal_a)
    conflict = service._submit_proposal_from_host("candidate", request, proposal_b)

    assert committed.status == "committed"
    assert replayed.status == "replayed"
    assert replayed.receipt == committed.receipt
    assert conflict.to_dict() == {
        "status": "failed_uncommitted",
        "error_code": "submission_replay_conflict",
    }
    expected_fingerprint = canonical_fingerprint(
        {
            "lane": "candidate",
            "request": request.model_dump(mode="json", exclude_unset=False),
            "proposal_sha256": hashlib.sha256(proposal_a).hexdigest(),
        }
    )
    replacement_sha = hashlib.sha256(proposal_b).hexdigest()
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        snapshot = store.load_snapshot(RUN_ID)
        history = store.load_history()
        accepted = snapshot.accepted_proposals[0]
        assert snapshot.store_revision == 3
        assert accepted.proposal_sha256 == hashlib.sha256(proposal_a).hexdigest()
        assert accepted.request_fingerprint == expected_fingerprint
        proposal_event = next(
            item
            for item in snapshot.events
            if item.intake_binding is not None
            and item.intake_binding.proposal_id == accepted.proposal_id
        )
        assert proposal_event.intake_binding is not None
        assert proposal_event.intake_binding.request_fingerprint == expected_fingerprint
        revision = next(
            item
            for item in snapshot.artifact_revisions
            if item.artifact_id == accepted.artifact_id
            and item.revision == accepted.artifact_revision
        )
        assert (workspace / revision.path).read_bytes() == proposal_a
        assert replacement_sha not in repr(snapshot)
        assert replacement_sha not in repr(history.transactions)
        assert replacement_sha.encode() not in (workspace / "briefloop.db").read_bytes()


def test_invalid_trusted_candidate_records_one_failure_uow(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    request = _candidate_request(workspace, expected_revision=1)
    service = IntakeService(workspace, clock=CLOCK)

    result = service.submit_proposal(
        "candidate",
        request.relative_to(workspace).as_posix(),
    )

    assert result.status == "rejected_recorded"
    assert result.error_code == "source_not_found"
    assert result.receipt is not None
    assert result.receipt.artifact_revisions == []
    assert result.receipt.proposal_ids == []
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        snapshot = store.load_snapshot(RUN_ID)
        invocation = next(
            item
            for item in snapshot.invocations
            if item.invocation_id == "INV-SCOUT-001"
        )
        assert invocation.status == "failed"
        assert invocation.failure_reason == "source_not_found"
        assert snapshot.accepted_proposals == ()


def test_failed_request_exactly_replays_and_changed_bytes_conflict(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    request_path = _candidate_request(workspace, expected_revision=1)
    relative = request_path.relative_to(workspace).as_posix()
    service = IntakeService(workspace, clock=CLOCK)

    rejected = service.submit_proposal("candidate", relative)
    replayed = service.submit_proposal("candidate", relative)
    proposal_path = workspace / "scratch" / "INV-SCOUT-001" / "candidate_claims.json"
    proposal_path.write_bytes(proposal_path.read_bytes() + b" ")
    conflict = service.submit_proposal("candidate", relative)

    assert rejected.status == "rejected_recorded"
    assert replayed.status == "rejected_recorded"
    assert replayed.receipt == rejected.receipt
    assert conflict.to_dict() == {
        "status": "failed_uncommitted",
        "error_code": "submission_replay_conflict",
    }
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        assert store.current_revision == 2


def test_missing_explicit_run_head_is_zero_write(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace, include_head=False)
    request = _source_request(workspace).relative_to(workspace).as_posix()
    before = (workspace / "briefloop.db").read_bytes()

    result = IntakeService(workspace, clock=CLOCK).submit_source(request)

    assert result.to_dict() == {
        "status": "failed_uncommitted",
        "error_code": "current_run_binding_missing",
    }
    assert (workspace / "briefloop.db").read_bytes() == before
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        assert store.current_revision == 1


@pytest.mark.parametrize(
    ("case", "error_code"),
    [
        ("run_archived", "new_run_required"),
        ("invocation_not_active", "invocation_not_active"),
        ("invocation_role_mismatch", "invocation_role_mismatch"),
        ("stage_not_ready", "stage_not_ready"),
    ],
)
def test_untrusted_or_closed_submission_context_is_zero_write(
    tmp_path: Path,
    case: str,
    error_code: str,
) -> None:
    workspace = tmp_path / case
    _seed_workspace(workspace)
    _replace_seed_context(workspace, case)
    request = _source_request(workspace, expected_revision=2)

    result = IntakeService(workspace, clock=CLOCK).submit_source(
        request.relative_to(workspace).as_posix()
    )

    assert result.to_dict() == {
        "status": "failed_uncommitted",
        "error_code": error_code,
    }
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        assert store.current_revision == 2
        snapshot = store.load_snapshot(RUN_ID)
        assert snapshot.sources == ()
        assert snapshot.accepted_proposals == ()


def test_owner_and_artifact_revision_preconditions_are_zero_write(
    tmp_path: Path,
) -> None:
    owner_workspace = tmp_path / "owner"
    _seed_workspace(owner_workspace)
    request = _candidate_request(owner_workspace, expected_revision=1)
    request_payload = json.loads(request.read_text(encoding="utf-8"))
    request_payload["artifact_id"] = "screened_candidates"
    request_payload["input_path"] = (
        "scratch/INV-SCOUT-001/screened_candidates.json"
    )
    (request.parent / "screened_candidates.json").write_bytes(
        (request.parent / "candidate_claims.json").read_bytes()
    )
    _write_json(request, request_payload)

    owner = IntakeService(owner_workspace, clock=CLOCK).submit_proposal(
        "candidate",
        request.relative_to(owner_workspace).as_posix(),
    )
    assert owner.error_code == "artifact_owner_mismatch"
    with SQLiteControlStore.open(owner_workspace / "briefloop.db") as store:
        assert store.current_revision == 1

    stale_workspace = tmp_path / "stale"
    _seed_workspace(stale_workspace)
    service = IntakeService(stale_workspace, clock=CLOCK)
    assert service.submit_source(
        _source_request(stale_workspace).relative_to(stale_workspace).as_posix()
    ).status == "committed"
    assert service.submit_proposal(
        "candidate",
        _candidate_request(stale_workspace).relative_to(stale_workspace).as_posix(),
    ).status == "committed"
    screened = _proposal_request(
        stale_workspace,
        invocation_id="INV-SCREEN-001",
        request_id="REQ-SCREEN-STALE",
        artifact_id="screened_candidates",
        expected_store_revision=3,
        expected_artifact_revision=7,
        payload={
            "schema_version": "briefloop.screened_candidates_proposal.v2",
            "proposal_id": "PROP-SCREEN-STALE",
            "run_id": RUN_ID,
            "candidate_claims_proposal_id": "PROP-CANDIDATES-001",
            "created_at": NOW,
            "decisions": [
                {
                    "candidate_id": "CAND-001",
                    "decision": "selected",
                    "reason_code": None,
                    "explanation": None,
                }
            ],
        },
    )
    stale = service.submit_proposal(
        "screened",
        screened.relative_to(stale_workspace).as_posix(),
    )
    assert stale.error_code == "expected_artifact_revision_conflict"
    with SQLiteControlStore.open(stale_workspace / "briefloop.db") as store:
        assert store.current_revision == 3


@pytest.mark.parametrize(
    "failure_stage",
    ["before_blob_write", "after_blob_write:1", "after_records"],
)
def test_intake_commit_failure_keeps_invocation_active_and_db_unaccepted(
    tmp_path: Path,
    failure_stage: str,
) -> None:
    workspace = tmp_path / failure_stage.replace(":", "-")
    _seed_workspace(workspace)
    request = _source_request(workspace).relative_to(workspace).as_posix()

    def fail(stage: str) -> None:
        if stage == failure_stage:
            raise ControlStoreIntegrityError("injected_intake_failure")

    result = IntakeService(
        workspace,
        clock=CLOCK,
        _store_failure_hook=fail,
    ).submit_source(request)

    assert result.to_dict() == {
        "status": "failed_uncommitted",
        "error_code": "intake_commit_failed",
    }
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        snapshot = store.load_snapshot(RUN_ID)
        assert store.current_revision == 1
        assert snapshot.sources == ()
        assert snapshot.artifacts == ()
        invocation = next(
            item
            for item in snapshot.invocations
            if item.invocation_id == "INV-SOURCE-001"
        )
        assert invocation.status == "active"
        expected_orphans = 0 if failure_stage == "before_blob_write" else 1
        assert len(store.scan_orphans().orphan_hashes) == expected_orphans


def test_post_commit_outcome_unknown_recovers_by_exact_replay(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    request = _source_request(workspace).relative_to(workspace).as_posix()

    def fail(stage: str) -> None:
        if stage == "after_commit":
            raise ControlStoreIntegrityError("injected_after_commit_failure")

    unknown = IntakeService(
        workspace,
        clock=CLOCK,
        _store_failure_hook=fail,
    ).submit_source(request)
    replay = IntakeService(workspace, clock=CLOCK).submit_source(request)

    assert unknown.to_dict() == {
        "status": "commit_outcome_unknown",
        "error_code": "commit_outcome_unknown",
    }
    assert replay.status == "replayed"
    assert replay.source_id == "SRC-001"
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        assert store.current_revision == 2
        assert [item.source_id for item in store.load_snapshot(RUN_ID).sources] == [
            "SRC-001"
        ]


@pytest.mark.parametrize("outcome", ["accepted_proposal", "rejection"])
def test_intake_proposal_and_rejection_postcommit_unknown_exactly_replay(
    tmp_path: Path,
    outcome: str,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    if outcome == "accepted_proposal":
        source_request = _source_request(workspace).relative_to(workspace).as_posix()
        source = IntakeService(workspace, clock=CLOCK).submit_source(source_request)
        assert source.status == "committed"
        request_path = _candidate_request(workspace, expected_revision=2)
    else:
        request_path = _candidate_request(workspace, expected_revision=1)
    request = request_path.relative_to(workspace).as_posix()

    def fail(stage: str) -> None:
        if stage == "after_commit":
            raise ControlStoreIntegrityError("injected_after_commit_failure")

    unknown = IntakeService(
        workspace,
        clock=CLOCK,
        _store_failure_hook=fail,
    ).submit_proposal("candidate", request)
    replay = IntakeService(workspace, clock=CLOCK).submit_proposal(
        "candidate",
        request,
    )

    assert unknown.to_dict() == {
        "status": "commit_outcome_unknown",
        "error_code": "commit_outcome_unknown",
    }
    if outcome == "accepted_proposal":
        assert replay.status == "replayed"
        assert replay.proposal_id == "PROP-CANDIDATES-001"
        expected_revision = 3
    else:
        assert replay.status == "rejected_recorded"
        assert replay.error_code == "source_not_found"
        expected_revision = 2
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        assert store.current_revision == expected_revision


def test_intake_postcommit_readback_failure_is_unknown_then_replays(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    request = _source_request(workspace).relative_to(workspace).as_posix()

    def fail_readback(*_args, **_kwargs):
        raise IntakeError("injected_postcommit_readback_failure")

    with monkeypatch.context() as patch:
        patch.setattr(IntakeService, "_verify_source_readback", fail_readback)
        unknown = IntakeService(workspace, clock=CLOCK).submit_source(request)

    assert unknown.to_dict() == {
        "status": "commit_outcome_unknown",
        "error_code": "commit_outcome_unknown",
    }
    replay = IntakeService(workspace, clock=CLOCK).submit_source(request)
    assert replay.status == "replayed"
    assert replay.source_id == "SRC-001"
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        assert store.current_revision == 2


def test_existing_intake_receipt_with_failed_readback_stays_unknown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    request = _source_request(workspace).relative_to(workspace).as_posix()
    committed = IntakeService(workspace, clock=CLOCK).submit_source(request)
    assert committed.status == "committed"

    original_load_snapshot = SQLiteControlStore.load_snapshot
    calls = 0

    def fail_after_receipt(self, run_id):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ControlStoreIntegrityError("injected_replay_readback_failure")
        return original_load_snapshot(self, run_id)

    with monkeypatch.context() as patch:
        patch.setattr(SQLiteControlStore, "load_snapshot", fail_after_receipt)
        unknown = IntakeService(workspace, clock=CLOCK).submit_source(request)

    assert unknown.to_dict() == {
        "status": "commit_outcome_unknown",
        "error_code": "commit_outcome_unknown",
    }
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        assert store.current_revision == 2


def test_source_receipt_lookup_failure_is_unknown_then_exactly_replays(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    request_path = _source_request(workspace)
    request = request_path.relative_to(workspace).as_posix()
    service = IntakeService(workspace, clock=CLOCK)
    committed = service.submit_source(request)
    assert committed.status == "committed"

    def fail_lookup(*_args, **_kwargs):
        raise ControlStoreIntegrityError("injected_receipt_lookup_failure")

    with monkeypatch.context() as patch:
        patch.setattr(
            SQLiteControlStore,
            "load_transaction_receipt",
            fail_lookup,
        )
        unknown = service.submit_source(request)

    assert unknown.to_dict() == {
        "status": "commit_outcome_unknown",
        "error_code": "commit_outcome_unknown",
    }
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        assert store.current_revision == 2

    replay = service.submit_source(request)
    assert replay.status == "replayed"
    assert replay.receipt == committed.receipt

    content = workspace / "scratch/INV-SOURCE-001/source_content.pdf"
    content.write_bytes(content.read_bytes() + b"changed")
    conflict = service.submit_source(request)
    assert conflict.to_dict() == {
        "status": "failed_uncommitted",
        "error_code": "submission_replay_conflict",
    }
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        assert store.current_revision == 2


def test_intake_commit_sites_share_the_postcommit_observer_boundary() -> None:
    path = ROOT / "src/multi_agent_brief/intake_v2/service.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_commit_uow"
    ]
    assert len(calls) == 4
    assert all(len(call.args) == 2 for call in calls)


def test_commit_outcome_unknown_intake_result_is_strictly_value_free() -> None:
    result = IntakeResult(
        status="commit_outcome_unknown",
        error_code="commit_outcome_unknown",
    )
    assert result.exit_code == 1
    assert result.to_dict() == {
        "status": "commit_outcome_unknown",
        "error_code": "commit_outcome_unknown",
    }
    with pytest.raises(ValueError, match="invalid intake result shape"):
        IntakeResult(
            status="commit_outcome_unknown",
            error_code="commit_outcome_unknown",
            source_id="must-not-leak",
        )


def test_both_intake_public_operations_preserve_unknown_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    service = IntakeService(workspace, clock=CLOCK)

    def unknown(*_args, **_kwargs):
        raise ControlStoreCommitOutcomeUnknown()

    with monkeypatch.context() as patch:
        patch.setattr(service, "_submit_source", unknown)
        source = service.submit_source("scratch/unused.json")
    with monkeypatch.context() as patch:
        patch.setattr(service, "_submit_proposal", unknown)
        proposal = service.submit_proposal("candidate", "scratch/unused.json")

    for result in (source, proposal):
        assert result.to_dict() == {
            "status": "commit_outcome_unknown",
            "error_code": "commit_outcome_unknown",
        }


def test_stale_store_revision_and_unsafe_scratch_are_zero_write(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    stale_request = _source_request(workspace, expected_revision=0)
    service = IntakeService(workspace, clock=CLOCK)

    stale = service.submit_source(stale_request.relative_to(workspace).as_posix())
    assert stale.error_code == "expected_store_revision_conflict"
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        assert store.current_revision == 1

    content = workspace / "scratch" / "INV-SOURCE-001" / "source_content.pdf"
    outside = tmp_path / "outside.pdf"
    outside.write_bytes(content.read_bytes())
    content.unlink()
    content.symlink_to(outside)
    unsafe = service.submit_source(stale_request.relative_to(workspace).as_posix())
    assert unsafe.error_code == "scratch_entry_unsafe"
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        assert store.current_revision == 1


def test_finalized_current_run_requires_new_run_without_consuming_intake(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        unit = store.begin(RUN_ID, "TX-FINALIZED-001", "private_test_finalize", 1)
        unit.put_stage_state(
            _record(
                StageState,
                run_id=RUN_ID,
                stage_id="finalize",
                status="complete",
                revision=1,
                updated_at=NOW,
            )
        )
        unit.commit()
    request = _source_request(workspace, expected_revision=2)

    result = IntakeService(workspace, clock=CLOCK).submit_source(
        request.relative_to(workspace).as_posix()
    )

    assert result.error_code == "new_run_required"
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        assert store.current_revision == 2
        invocation = next(
            item
            for item in store.load_snapshot(RUN_ID).invocations
            if item.invocation_id == "INV-SOURCE-001"
        )
        assert invocation.status == "active"

def test_malformed_request_is_uncommitted_but_malformed_owned_proposal_is_recorded(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    request = _candidate_request(workspace, expected_revision=1)
    request_relative = request.relative_to(workspace).as_posix()
    request.write_text('{"schema_version":', encoding="utf-8")
    service = IntakeService(workspace, clock=CLOCK)

    invalid_request = service.submit_proposal("candidate", request_relative)
    assert invalid_request.error_code == "intake_request_invalid"
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        assert store.current_revision == 1

    request = _candidate_request(workspace, expected_revision=1)
    proposal = workspace / "scratch" / "INV-SCOUT-001" / "candidate_claims.json"
    proposal.write_text('{"schema_version":', encoding="utf-8")
    invalid_proposal = service.submit_proposal(
        "candidate",
        request.relative_to(workspace).as_posix(),
    )
    assert invalid_proposal.status == "rejected_recorded"
    assert invalid_proposal.error_code == "proposal_contract_invalid"
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        assert store.current_revision == 2


def test_pr3_unbound_intake_never_invokes_core_run_verifier(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)

    def forbidden_core_verification(*_args, **_kwargs):
        pytest.fail("PR-3 unbound intake must not invoke the PR-4A domain verifier")

    monkeypatch.setattr(
        IntakeService,
        "_verify_core_run",
        forbidden_core_verification,
    )
    service = IntakeService(workspace, clock=CLOCK)
    committed = service.submit_source(
        _source_request(workspace).relative_to(workspace).as_posix()
    )
    assert committed.status == "committed"

    request = _candidate_request(workspace, expected_revision=2)
    proposal = workspace / "scratch" / "INV-SCOUT-001" / "candidate_claims.json"
    proposal.write_text('{"schema_version":', encoding="utf-8")
    rejected = service.submit_proposal(
        "candidate",
        request.relative_to(workspace).as_posix(),
    )
    assert rejected.status == "rejected_recorded"
    assert rejected.error_code == "proposal_contract_invalid"
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        assert store.current_revision == 3


def test_all_five_lanes_and_both_screening_owners_commit_without_stage_advance(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    service = IntakeService(workspace, clock=CLOCK)
    before_stages = None
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        before_stages = store.load_snapshot(RUN_ID).stage_states

    assert service.submit_source(
        _source_request(workspace).relative_to(workspace).as_posix()
    ).status == "committed"
    assert service.submit_proposal(
        "candidate",
        _candidate_request(workspace).relative_to(workspace).as_posix(),
    ).status == "committed"

    screened_default = _proposal_request(
        workspace,
        invocation_id="INV-SCREEN-001",
        request_id="REQ-SCREEN-001",
        artifact_id="screened_candidates",
        expected_store_revision=3,
        expected_artifact_revision=0,
        payload={
            "schema_version": "briefloop.screened_candidates_proposal.v2",
            "proposal_id": "PROP-SCREENED-001",
            "run_id": RUN_ID,
            "candidate_claims_proposal_id": "PROP-CANDIDATES-001",
            "created_at": NOW,
            "decisions": [
                {
                    "candidate_id": "CAND-001",
                    "decision": "selected",
                    "reason_code": None,
                    "explanation": None,
                }
            ],
        },
    )
    assert service.submit_proposal(
        "screened",
        screened_default.relative_to(workspace).as_posix(),
    ).status == "committed"

    screened_strict = _proposal_request(
        workspace,
        invocation_id="INV-SCREENER-001",
        request_id="REQ-SCREEN-STRICT-001",
        artifact_id="screened_candidates",
        expected_store_revision=4,
        expected_artifact_revision=1,
        payload={
            "schema_version": "briefloop.screened_candidates_proposal.v2",
            "proposal_id": "PROP-SCREENED-STRICT-001",
            "run_id": RUN_ID,
            "candidate_claims_proposal_id": "PROP-CANDIDATES-001",
            "created_at": NOW,
            "decisions": [
                {
                    "candidate_id": "CAND-001",
                    "decision": "selected",
                    "reason_code": None,
                    "explanation": None,
                }
            ],
        },
    )
    assert service.submit_proposal(
        "screened",
        screened_strict.relative_to(workspace).as_posix(),
    ).status == "committed"

    drafts = _proposal_request(
        workspace,
        invocation_id="INV-DRAFTS-001",
        request_id="REQ-DRAFTS-001",
        artifact_id="claim_drafts",
        expected_store_revision=5,
        expected_artifact_revision=0,
        payload={
            "schema_version": "briefloop.claim_drafts_proposal.v2",
            "proposal_id": "PROP-DRAFTS-001",
            "run_id": RUN_ID,
            "screened_candidates_proposal_id": "PROP-SCREENED-STRICT-001",
            "created_at": NOW,
            "drafts": [
                {
                    "draft_id": "DRAFT-001",
                    "statement": "A synthetic public filing was supplied.",
                    "evidence_text": "Synthetic public filing bytes.",
                    "source_ids": ["SRC-001"],
                    "claim_type": "fact",
                }
            ],
        },
    )
    assert service.submit_proposal(
        "claim-drafts",
        drafts.relative_to(workspace).as_posix(),
    ).status == "committed"

    audit = _proposal_request(
        workspace,
        invocation_id="INV-AUDIT-001",
        request_id="REQ-AUDIT-001",
        artifact_id="audit_proposal",
        expected_store_revision=6,
        expected_artifact_revision=0,
        payload={
            "schema_version": "briefloop.audit_proposal.v2",
            "proposal_id": "PROP-AUDIT-001",
            "run_id": RUN_ID,
            "artifact_id": "candidate_claims",
            "artifact_revision": 1,
            "decision": "pass",
            "created_at": NOW,
            "findings": [],
        },
    )
    assert service.submit_proposal(
        "audit",
        audit.relative_to(workspace).as_posix(),
    ).status == "committed"

    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        snapshot = store.load_snapshot(RUN_ID)
        assert snapshot.stage_states == before_stages
        assert snapshot.store_revision == 7
        assert [item.proposal_kind for item in snapshot.accepted_proposals] == [
            "audit",
            "candidate",
            "claim_drafts",
            "screened",
            "screened",
        ]
        assert snapshot.approvals == ()
        assert snapshot.deliveries == ()


def test_screening_parent_universe_mismatch_records_only_failure_uow(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    service = IntakeService(workspace, clock=CLOCK)
    assert service.submit_source(
        _source_request(workspace).relative_to(workspace).as_posix()
    ).status == "committed"
    assert service.submit_proposal(
        "candidate",
        _candidate_request(workspace).relative_to(workspace).as_posix(),
    ).status == "committed"
    request = _proposal_request(
        workspace,
        invocation_id="INV-SCREEN-001",
        request_id="REQ-SCREEN-MISMATCH",
        artifact_id="screened_candidates",
        expected_store_revision=3,
        expected_artifact_revision=0,
        payload={
            "schema_version": "briefloop.screened_candidates_proposal.v2",
            "proposal_id": "PROP-SCREEN-MISMATCH",
            "run_id": RUN_ID,
            "candidate_claims_proposal_id": "PROP-CANDIDATES-001",
            "created_at": NOW,
            "decisions": [
                {
                    "candidate_id": "CAND-OTHER",
                    "decision": "selected",
                    "reason_code": None,
                    "explanation": None,
                }
            ],
        },
    )

    result = service.submit_proposal(
        "screened",
        request.relative_to(workspace).as_posix(),
    )

    assert result.status == "rejected_recorded"
    assert result.error_code == "candidate_universe_mismatch"
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        snapshot = store.load_snapshot(RUN_ID)
        assert snapshot.store_revision == 4
        assert [item.proposal_kind for item in snapshot.accepted_proposals] == [
            "candidate"
        ]
        assert _by_invocation(snapshot, "INV-SCREEN-001").status == "failed"


def test_claim_draft_parent_and_final_claim_identity_fail_closed(
    tmp_path: Path,
) -> None:
    parent_workspace = tmp_path / "parent"
    _seed_workspace(parent_workspace)
    parent_request = _proposal_request(
        parent_workspace,
        invocation_id="INV-DRAFTS-001",
        request_id="REQ-DRAFTS-PARENT",
        artifact_id="claim_drafts",
        expected_store_revision=1,
        expected_artifact_revision=0,
        payload={
            "schema_version": "briefloop.claim_drafts_proposal.v2",
            "proposal_id": "PROP-DRAFTS-PARENT",
            "run_id": RUN_ID,
            "screened_candidates_proposal_id": "PROP-MISSING",
            "created_at": NOW,
            "drafts": [
                {
                    "draft_id": "DRAFT-001",
                    "statement": "Synthetic statement.",
                    "evidence_text": "Synthetic evidence.",
                    "source_ids": ["SRC-MISSING"],
                    "claim_type": "fact",
                }
            ],
        },
    )
    parent = IntakeService(parent_workspace, clock=CLOCK).submit_proposal(
        "claim-drafts",
        parent_request.relative_to(parent_workspace).as_posix(),
    )
    assert parent.status == "rejected_recorded"
    assert parent.error_code == "proposal_parent_invalid"

    claim_workspace = tmp_path / "claim-id"
    _seed_workspace(claim_workspace)
    claim_request = _proposal_request(
        claim_workspace,
        invocation_id="INV-DRAFTS-001",
        request_id="REQ-DRAFTS-CLAIM-ID",
        artifact_id="claim_drafts",
        expected_store_revision=1,
        expected_artifact_revision=0,
        payload={
            "schema_version": "briefloop.claim_drafts_proposal.v2",
            "proposal_id": "PROP-DRAFTS-CLAIM-ID",
            "run_id": RUN_ID,
            "screened_candidates_proposal_id": "PROP-MISSING",
            "created_at": NOW,
            "drafts": [
                {
                    "draft_id": "DRAFT-001",
                    "claim_id": "CLAIM-001",
                    "statement": "Synthetic statement.",
                    "evidence_text": "Synthetic evidence.",
                    "source_ids": ["SRC-MISSING"],
                    "claim_type": "fact",
                }
            ],
        },
    )
    claim = IntakeService(claim_workspace, clock=CLOCK).submit_proposal(
        "claim-drafts",
        claim_request.relative_to(claim_workspace).as_posix(),
    )
    assert claim.status == "rejected_recorded"
    assert claim.error_code == "proposal_contract_invalid"


def test_audit_requires_current_frozen_same_run_target_revision(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    _seed_workspace(workspace)
    request = _proposal_request(
        workspace,
        invocation_id="INV-AUDIT-001",
        request_id="REQ-AUDIT-MISSING-TARGET",
        artifact_id="audit_proposal",
        expected_store_revision=1,
        expected_artifact_revision=0,
        payload={
            "schema_version": "briefloop.audit_proposal.v2",
            "proposal_id": "PROP-AUDIT-MISSING-TARGET",
            "run_id": RUN_ID,
            "artifact_id": "candidate_claims",
            "artifact_revision": 1,
            "decision": "pass",
            "created_at": NOW,
            "findings": [],
        },
    )

    result = IntakeService(workspace, clock=CLOCK).submit_proposal(
        "audit",
        request.relative_to(workspace).as_posix(),
    )

    assert result.status == "rejected_recorded"
    assert result.error_code == "audit_target_invalid"
    with SQLiteControlStore.open(workspace / "briefloop.db") as store:
        snapshot = store.load_snapshot(RUN_ID)
        assert snapshot.accepted_proposals == ()
        assert snapshot.artifacts == ()


@pytest.mark.parametrize(
    (
        "origin",
        "method",
        "material",
        "provider",
        "raw",
        "eligible",
        "reason",
    ),
    [
        (
            "uploaded_file",
            "manual_upload",
            "uploaded_file",
            None,
            False,
            True,
            "eligible_durable_source_content",
        ),
        (
            "manual_evidence",
            "manual_evidence",
            "partial_extract",
            None,
            False,
            True,
            "eligible_durable_source_content",
        ),
        (
            "provider_response",
            "provider_search",
            "search_result",
            "provider",
            True,
            False,
            "ineligible_search_result",
        ),
        (
            "provider_response",
            "provider_extract",
            "full_content",
            "provider",
            True,
            True,
            "eligible_durable_source_content",
        ),
        (
            "authorized_web_fetch",
            "authorized_web_fetch",
            "partial_extract",
            None,
            False,
            True,
            "eligible_durable_source_content",
        ),
        (
            "cached_provider_response",
            "cached_provider_response",
            "search_snippet",
            "provider",
            True,
            False,
            "ineligible_search_snippet",
        ),
        (
            "claim_ledger_derivative",
            "downstream_derivative",
            "downstream_derivative",
            None,
            False,
            False,
            "ineligible_downstream_derivative",
        ),
        (
            "model_summary_derivative",
            "model_generated",
            "model_synthesis",
            None,
            False,
            False,
            "ineligible_model_synthesis",
        ),
        (
            "search_snippet_only",
            "provider_search",
            "search_snippet",
            "provider",
            True,
            False,
            "ineligible_search_snippet",
        ),
        (
            "unknown",
            "unknown",
            "unknown",
            None,
            False,
            False,
            "ineligible_unknown_origin",
        ),
    ],
)
def test_source_eligibility_matrix_is_literal_and_deterministic(
    origin: str,
    method: str,
    material: str,
    provider: str | None,
    raw: bool,
    eligible: bool,
    reason: str,
) -> None:
    proposal = SourceProposal.model_validate(
        {
            "schema_version": SourceProposal.schema_id,
            "proposal_id": "PROP-SOURCE-POLICY",
            "run_id": RUN_ID,
            "source_id": "SRC-POLICY",
            "origin_type": origin,
            "acquisition_method": method,
            "material_kind": material,
            "provider": provider,
            "locator": {
                "kind": "file",
                "path": "scratch/INV-SOURCE-001/source_content.pdf",
            },
            "title": "Synthetic public source",
            "publisher": None,
            "published_at": None,
            "retrieved_at": NOW,
            "source_category": "regulator",
            "retrieval_source_type": "local_file",
            "underlying_evidence_type": "filing",
            "raw_underlying_evidence_type": None,
            "content_sha256": "a" * 64,
            "content_media_type": "application/pdf",
            "raw_payload_sha256": "b" * 64 if raw else None,
            "raw_payload_media_type": "application/json" if raw else None,
        },
        strict=True,
    )
    assert evaluate_source_eligibility(
        proposal,
        raw_payload_present=raw,
    ) == (eligible, reason)


def test_source_policy_rejects_impossible_combination_instead_of_downgrading() -> (
    None
):
    proposal = SourceProposal.model_validate(
        {
            **SourceProposal.minimal_example,
            "origin_type": "uploaded_file",
            "acquisition_method": "provider_search",
            "material_kind": "search_result",
            "provider": "provider",
            "raw_payload_sha256": "b" * 64,
            "raw_payload_media_type": "application/json",
        },
        strict=True,
    )
    with pytest.raises(SourcePolicyError, match="^source_origin_policy_invalid$"):
        evaluate_source_eligibility(proposal, raw_payload_present=True)
