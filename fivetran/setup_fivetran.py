import requests
import yaml
import time

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_KEY    = "{{PLACEHOLDER_FIVETRAN_API_KEY}}"
API_SECRET = "{{PLACEHOLDER_FIVETRAN_API_SECRET}}"
BASE_URL   = "https://api.fivetran.com/v1"

AUTH    = (API_KEY, API_SECRET)
HEADERS = {
    "Content-Type": "application/json",
    "Accept":       "application/json;version=2",   # required by Fivetran API v2
}


# ---------------------------------------------------------------------------
# Core API wrapper
# ---------------------------------------------------------------------------
def api_call(method, path, payload=None, retries=3):
    url = BASE_URL + path
    print(f"  [API] {method} {url}")
    if payload:
        import json
        print(f"  [REQ] {json.dumps(payload, indent=2)}")
    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(
                method, url, auth=AUTH, headers=HEADERS,
                json=payload, timeout=30
            )
            print(f"  [RSP] HTTP {resp.status_code}: {resp.text}")
            if method == "POST" and (
                resp.status_code == 409
                or (resp.status_code == 400 and "already exists" in resp.text.lower())
            ):
                print(f"  [WARN] Object already exists ({path}) — skipping creation")
                return resp.json().get("data", {})
            if resp.status_code == 429:
                wait = 60 * attempt
                print(f"  [WARN] Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            if not resp.ok:
                raise RuntimeError(
                    f"API error {resp.status_code} on {method} {path}: {resp.text}"
                )
            return resp.json().get("data", {})
        except requests.exceptions.RequestException as e:
            if attempt == retries:
                raise RuntimeError(f"Network error after {retries} attempts: {e}")
            time.sleep(5)
    raise RuntimeError(f"Max retries exceeded: {method} {path}")


# ---------------------------------------------------------------------------
# Infrastructure setup (group + destination)
# ---------------------------------------------------------------------------
def create_group():
    print("Step 1: Creating Fivetran group...")
    group    = api_call("POST", "/groups", {"name": "NFP Analytics"})
    group_id = group["id"]
    print(f"  group_id = {group_id}")
    return group_id


def create_destination(group_id):
    print("Step 2: Creating Snowflake destination...")
    api_call("POST", "/destinations", {
        "group_id":         group_id,
        "service":          "snowflake",
        "region":           "GCP_US_EAST4",
        "time_zone_offset": "0",
        "config": {
            "host":          "{{PLACEHOLDER_SNOWFLAKE_ACCOUNT}}.snowflakecomputing.com",
            "port":          443,
            "database":      "NFP_BRONZE",
            "schema_prefix": "nfp_bronze",
            "warehouse":     "NFP_LOADING_WH",
            "auth":          "PASSWORD",
            "user":          "FIVETRAN_USER_NFP",
            "password":      "{{PLACEHOLDER_SNOWFLAKE_SVC_PASSWORD}}"
        }
    })
    print("  Snowflake destination configured")


# ---------------------------------------------------------------------------
# 8-step connector setup helpers
# ---------------------------------------------------------------------------
def _step_create_connector(connector_data, group_id):
    """Step 1/8 — Create connector in paused state (no sync, no schema capture yet)."""
    config = connector_data["config"].copy()
    config["schema_prefix"] = connector_data["destination"]["schema"].lower()

    payload = {
        "service":         connector_data["service"],
        "group_id":        group_id,
        "paused":          True,    # keep paused until schema is configured
        "run_setup_tests": False,   # we run tests explicitly in step 2
        "sync_frequency":  int(connector_data.get("sync_frequency", 1440)),
        "schedule_type":   "auto",
        "config":          config,
    }

    print("  [1/8] Creating connector (paused)...")
    data    = api_call("POST", "/connections", payload)
    conn_id = data.get("id")
    if not conn_id:
        raise RuntimeError(f"Failed to get connection_id from response: {data}")
    print(f"        connection_id = {conn_id}")
    return conn_id


def _step_run_setup_tests(conn_id):
    """Step 2/8 — Validate credentials and network connectivity."""
    print("  [2/8] Running setup tests...")
    result = api_call("POST", f"/connections/{conn_id}/test", {
        "trust_certificates": True,
        "trust_fingerprints": True,
    })
    tests  = result.get("setup_tests", [])
    failed = [t for t in tests if t.get("status") == "FAILED"]
    if failed:
        for t in failed:
            print(f"        [FAIL] {t['title']}: {t.get('message', '')}")
        raise RuntimeError(f"Setup tests failed for connection {conn_id}")
    print(f"        All {len(tests)} test(s) passed")


def _step_trigger_schema_discovery(conn_id):
    """Step 3/8 — Triggers schema discovery. Returns False for DB connectors."""
    print("  [3/8] Triggering schema discovery...")
    try:
        api_call("PATCH", f"/connections/{conn_id}", {
            "schema_status": "blocked_on_capture",
            "paused":        False,
        })
        return True
    except RuntimeError as e:
        if "UnsupportedOperation" in str(e) or "does not support schema capturing" in str(e):
            print("        Schema capturing not supported — skipping to direct sync")
            return False
        raise


def _step_wait_for_schema_capture(conn_id, timeout=300, poll_interval=10):
    """Step 4/8 — Poll until schema_status == 'blocked_on_customer'."""
    print("  [4/8] Waiting for schema capture", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        data   = api_call("GET", f"/connections/{conn_id}")
        status = data.get("schema_status")
        if status == "blocked_on_customer":
            print(" done.")
            return
        if status not in ("blocked_on_capture", None):
            raise RuntimeError(f"Unexpected schema_status during capture: {status}")
        print(".", end="", flush=True)
        time.sleep(poll_interval)
    raise RuntimeError(f"Schema capture timed out after {timeout}s for {conn_id}")


def _step_configure_schema(conn_id, schemas_config):
    """Steps 5+6/8 — List discovered schemas then apply YAML table selections."""
    print("  [5/8] Fetching discovered schemas...")
    discovered       = api_call("GET", f"/connections/{conn_id}/schemas")
    discovered_names = list(discovered.get("schemas", {}).keys())
    print(f"        Discovered: {discovered_names}")

    print("  [6/8] Applying table selections from YAML...")
    schemas_dict    = {}
    enabled_summary = []
    for schema in schemas_config:
        tables_dict = {}
        for table in schema.get("tables", []):
            enabled = table.get("enabled", True)
            tables_dict[table["name"]] = {"enabled": enabled}
            if enabled:
                enabled_summary.append(f"{schema['name']}.{table['name']}")
        schemas_dict[schema["name"]] = {"enabled": True, "tables": tables_dict}

    api_call("PATCH", f"/connections/{conn_id}/schemas", {
        "schema_change_handling": "ALLOW_ALL",   # new tables/columns auto-sync
        "schemas":                schemas_dict,
    })
    print(f"        Enabled tables: {enabled_summary}")


def _step_wait_and_configure_db_schema(conn_id, schemas_config, timeout=300, poll_interval=15):
    """DB connector branch: poll until schema discovered, then apply YAML selections."""
    if not schemas_config:
        return

    print("  [3b] Waiting for DB schema discovery", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            data = api_call("GET", f"/connections/{conn_id}/schemas")
            if data.get("schemas"):
                print(" done.")
                break
        except RuntimeError as e:
            if "NotFound_SchemaConfig" in str(e) or "404" in str(e):
                pass  # schema not populated yet — keep waiting
            else:
                raise
        print(".", end="", flush=True)
        time.sleep(poll_interval)
    else:
        print("\n  [WARN] Schema discovery timed out — applying config anyway")

    print("  [3c] Applying YAML table selections...")
    discovered         = api_call("GET", f"/connections/{conn_id}/schemas")
    discovered_schemas = discovered.get("schemas", {})

    # Normalise to uppercase for case-insensitive matching
    yaml_tables = {
        (s["name"].upper(), t["name"].upper())
        for s in schemas_config
        for t in s.get("tables", [])
    }

    schemas_dict     = {}
    enabled_summary  = []
    disabled_summary = []

    for schema_name, schema_data in discovered_schemas.items():
        tables_dict = {}
        for table_name in schema_data.get("tables", {}).keys():
            if (schema_name.upper(), table_name.upper()) in yaml_tables:
                tables_dict[table_name] = {"enabled": True}
                enabled_summary.append(f"{schema_name}.{table_name}")
            else:
                tables_dict[table_name] = {"enabled": False}
                disabled_summary.append(f"{schema_name}.{table_name}")
        schemas_dict[schema_name] = {"enabled": True, "tables": tables_dict}

    api_call("PATCH", f"/connections/{conn_id}/schemas", {
        "schema_change_handling": "ALLOW_COLUMNS",   # new columns auto-sync; new tables require opt-in
        "schemas":                schemas_dict,
    })
    print(f"        Enabled  tables : {enabled_summary}")
    print(f"        Disabled tables : {disabled_summary}")


def _step_set_schema_ready(conn_id):
    """Step 7/8 — Release the schema block; connector enters normal schedule."""
    print("  [7/8] Setting schema status to ready...")
    api_call("PATCH", f"/connections/{conn_id}", {"schema_status": "ready"})


def _step_trigger_initial_sync(conn_id):
    """Step 8/8 — Trigger the first (full historical) sync."""
    print("  [8/8] Triggering initial sync...")
    api_call("POST", f"/connections/{conn_id}/sync", {"force": True})
    print("        Initial sync triggered.")


def _wait_for_sync_completion(conn_id, timeout=7200, poll_interval=30):
    """Poll sync_state until it leaves 'syncing', then validate success."""
    print("  [ + ] Polling sync completion", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        data       = api_call("GET", f"/connections/{conn_id}")
        sync_state = data.get("status", {}).get("sync_state")
        succeeded  = data.get("succeeded_at")
        failed_at  = data.get("failed_at")

        if sync_state == "syncing":
            print(".", end="", flush=True)
            time.sleep(poll_interval)
            continue

        print()
        if failed_at and (not succeeded or failed_at > succeeded):
            raise RuntimeError(f"Sync failed for {conn_id} (failed_at={failed_at})")
        print(f"        Sync completed successfully at {succeeded}")
        return

    raise RuntimeError(f"Sync timed out after {timeout}s for {conn_id}")


# ---------------------------------------------------------------------------
# Full 8-step connector setup
# ---------------------------------------------------------------------------
def setup_connector(config_file, group_id):
    print(f"\n{'='*60}")
    print(f"Connector: {config_file}")
    print(f"{'='*60}")

    with open(config_file, "r") as f:
        data = yaml.safe_load(f)

    connector_data = data["connector"]
    schemas_config = connector_data.get("schemas", [])
    name           = connector_data.get("name", config_file)
    print(f"Name: {name}")

    conn_id = _step_create_connector(connector_data, group_id)
    _step_run_setup_tests(conn_id)
    supports_capture = _step_trigger_schema_discovery(conn_id)
    if supports_capture:
        _step_wait_for_schema_capture(conn_id)
        _step_configure_schema(conn_id, schemas_config)
        _step_set_schema_ready(conn_id)
    else:
        # Database connectors: unpause → wait for background schema discovery → configure
        api_call("PATCH", f"/connections/{conn_id}", {"paused": False})
        _step_wait_and_configure_db_schema(conn_id, schemas_config)
    _step_trigger_initial_sync(conn_id)
    _wait_for_sync_completion(conn_id)

    print(f"\n  '{name}' fully configured and initial sync complete.")
    return conn_id


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    group_id = create_group()
    create_destination(group_id)

    connectors = [
        "fivetran/nfp_aims_connector.yaml",
        "fivetran/nfp_amadeus_connector.yaml",
        "fivetran/nfp_champ_connector.yaml",
        "fivetran/nfp_epicor_connector.yaml",
        "fivetran/nfp_amos_connector.yaml",
        "fivetran/nfp_ref_connector.yaml",
    ]

    results = {}
    for config_file in connectors:
        conn_id = setup_connector(config_file, group_id)
        results[config_file] = conn_id

    print("\nAll connectors set up. Summary:")
    for path, cid in results.items():
        print(f"  {path:50s}  →  {cid}")