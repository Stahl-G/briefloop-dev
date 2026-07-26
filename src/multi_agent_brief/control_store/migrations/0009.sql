BEGIN IMMEDIATE;

CREATE TABLE run_source_discovery_authorizations (
    run_id TEXT NOT NULL CHECK(typeof(run_id)='text' AND length(run_id)>0),
    authorization_id TEXT NOT NULL CHECK(typeof(authorization_id)='text' AND length(authorization_id)>0),
    workspace_id TEXT NOT NULL CHECK(typeof(workspace_id)='text' AND length(workspace_id)>0),
    schema_version TEXT NOT NULL CHECK(typeof(schema_version)='text' AND schema_version='briefloop.run_source_discovery_authorization.v2'),
    run_contract_fingerprint TEXT NOT NULL CHECK(typeof(run_contract_fingerprint)='text' AND length(run_contract_fingerprint)=64 AND run_contract_fingerprint NOT GLOB '*[^0-9a-f]*'),
    run_direction_fingerprint TEXT NOT NULL CHECK(typeof(run_direction_fingerprint)='text' AND length(run_direction_fingerprint)=64 AND run_direction_fingerprint NOT GLOB '*[^0-9a-f]*'),
    runtime_source_plan_fingerprint TEXT NOT NULL CHECK(typeof(runtime_source_plan_fingerprint)='text' AND length(runtime_source_plan_fingerprint)=64 AND runtime_source_plan_fingerprint NOT GLOB '*[^0-9a-f]*'),
    source_route_id TEXT NOT NULL CHECK(typeof(source_route_id)='text' AND source_route_id='web-search'),
    route_fingerprint TEXT NOT NULL CHECK(typeof(route_fingerprint)='text' AND length(route_fingerprint)=64 AND route_fingerprint NOT GLOB '*[^0-9a-f]*'),
    provider_id TEXT NOT NULL CHECK(typeof(provider_id)='text' AND provider_id='tavily'),
    execution_owner TEXT NOT NULL CHECK(typeof(execution_owner)='text' AND execution_owner='deterministic'),
    secret_env_name TEXT NOT NULL CHECK(typeof(secret_env_name)='text' AND secret_env_name='TAVILY_API_KEY'),
    completion_target TEXT NOT NULL CHECK(typeof(completion_target)='text' AND completion_target='finalized_local'),
    repair_budget INTEGER NOT NULL CHECK(typeof(repair_budget)='integer' AND repair_budget=1),
    authorization_event_id TEXT NOT NULL CHECK(typeof(authorization_event_id)='text' AND length(authorization_event_id)>0),
    accepted_transaction_id TEXT NOT NULL CHECK(typeof(accepted_transaction_id)='text' AND length(accepted_transaction_id)>0),
    request_fingerprint TEXT NOT NULL CHECK(typeof(request_fingerprint)='text' AND length(request_fingerprint)=64 AND request_fingerprint NOT GLOB '*[^0-9a-f]*'),
    created_at TEXT NOT NULL CHECK(typeof(created_at)='text' AND length(created_at)>0),
    payload_json TEXT NOT NULL CHECK(typeof(payload_json)='text'),
    PRIMARY KEY(run_id, authorization_id),
    UNIQUE(run_id),
    FOREIGN KEY(run_id) REFERENCES runs(run_id) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY(workspace_id) REFERENCES workspaces(workspace_id) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY(run_id, authorization_event_id) REFERENCES events(run_id, event_id) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY(run_id, accepted_transaction_id) REFERENCES transactions(run_id, transaction_id) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY(run_id, accepted_transaction_id, authorization_event_id) REFERENCES transaction_events(run_id, transaction_id, event_id) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
);

CREATE TRIGGER run_source_discovery_authorizations_no_update
BEFORE UPDATE ON run_source_discovery_authorizations
BEGIN SELECT RAISE(ABORT,'append_only'); END;

CREATE TRIGGER run_source_discovery_authorizations_no_delete
BEFORE DELETE ON run_source_discovery_authorizations
BEGIN SELECT RAISE(ABORT,'append_only'); END;

CREATE TABLE transaction_run_source_discovery_authorizations (
    run_id TEXT NOT NULL CHECK(typeof(run_id)='text' AND length(run_id)>0),
    transaction_id TEXT NOT NULL CHECK(typeof(transaction_id)='text' AND length(transaction_id)>0),
    position INTEGER NOT NULL CHECK(typeof(position)='integer' AND position>=0),
    authorization_id TEXT NOT NULL CHECK(typeof(authorization_id)='text' AND length(authorization_id)>0),
    PRIMARY KEY(run_id, transaction_id, position),
    UNIQUE(run_id, transaction_id, authorization_id),
    FOREIGN KEY(run_id, transaction_id) REFERENCES transactions(run_id, transaction_id) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY(run_id, authorization_id) REFERENCES run_source_discovery_authorizations(run_id, authorization_id) ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
);

INSERT INTO schema_migrations(version,name) VALUES(9,'0009');
PRAGMA user_version=9;
COMMIT;
