# Proposal: Tenant-Based Configuration Overrides for Thanos Components

* **Author(s):** ricket-son  
* **Created:** 2025-11-01  
* **Related Issue:** [#8544](https://github.com/thanos-io/thanos/issues/8544)

> TL;DR: This design doc is proposing configuration overrides on a per-tenant basis on several thanos components to integrate flexible configuration.

---

## Context

Thanos is increasingly deployed in **multi-tenant environments**, where each tenant may have distinct requirements for data retention, compaction scheduling, and performance tuning.  
Currently, most Thanos components—such as the **Compactor**, **Receive**, or **Store Gateway**—share a single configuration across all tenants.

This limitation means that:
- Operators cannot easily enforce **different retention periods** per tenant.
- All tenants are affected by the same compaction or concurrency settings.
- To achieve separation, operators must deploy **one component per tenant**, which adds operational overhead.

A native, configuration-based mechanism for **tenant-specific overrides** would reduce complexity, improve flexibility, and better support multi-tenant Thanos deployments.

---

## Goals

- Enable **tenant-based configuration overrides** for Thanos components, starting with the Compactor.
- Support common parameters such as:
  - Retention periods per resolution level.
  - Compaction concurrency and deletion delay.
  - Optional component-specific tunables (e.g., query or ingestion limits).
- Maintain backward compatibility (no overrides = current behavior).
- Support reloadable configuration files where possible.
- Standardize how Thanos identifies tenants for configuration purposes.

---

## Non-Goals

- Implementing full multi-tenancy isolation (e.g., per-tenant storage credentials or ACLs).
- Providing a web UI or API for managing overrides.
- Changing the existing data model or metadata schema beyond identifying tenants.

---

## Proposed Design

### Configuration Format

A new top-level configuration section, `tenant_overrides`, is added to the component configuration file.

Example configuration structure:

```yaml
# global compactor configs
compactor:
  retention:
    resolution_raw: 365d # --retention.resolution-raw=365d
    resolution_5m: 180d # --retention.resolution-5m=180d
    resolution_1h: 0d # --retention.resolution-1h=0d
  concurrency: 4 # --compact.concurrency=4
  delete_delay: 48h # --delete-delay=48h

# tenant-specific configs
tenant_overrides:
  default:
    compactor:
      retention:
        resolution_raw: 180d
        resolution_5m: 90d
        resolution_1h: 0d

  tenants:
    tenant-a:
      compactor:
        retention:
          resolution_raw: 90d

    tenant-b:
      compactor:
        retention:
          resolution_raw: 5d
          resolution_5m: 10d
        delete_delay: 12h

    # future:
    #   receive:
    #     max_series_per_tenant: 
    #     max_ingest_samples_per_second: 

    #   storegateway:
    #     query_timeout: 

```

**Key principles:**
- `default` defines the baseline configuration for all tenants.
- Each tenant ID in `tenants` provides overrides for one or more fields.
- Unspecified fields inherit from `default` or the component’s global config.
- Tenant IDs map to existing tenant identifiers in Thanos block metadata (e.g., `__tenant_id__`).

### Parameters Potentially Overridable

| Parameter | Component | Description |
|------------|------------|-------------|
| `retention.resolution_raw` | Compactor | Duration to keep raw metrics blocks |
| `retention.resolution_5m` | Compactor | Retention for 5-minute resolution |
| `retention.resolution_1h` | Compactor | Retention for 1-hour resolution |
| `delete_delay` | Compactor | Grace period before deleting old blocks |
| `concurrency` | Compactor | Number of concurrent compactions |
| `max_series_per_tenant` | Receive | (Future) Max allowed series per tenant |
| `max_ingest_samples_per_second` | Receive | (Future) Max allowed samples per second |
| `query_timeout` | Store Gateway | (Future) Query timeout per tenant |

### Runtime Behavior

1. On startup, the component loads the base configuration.
2. It parses `tenant_overrides` and stores them in memory.
3. For each tenant operation (e.g., block compaction), the component:
   - Determines the tenant ID from metadata.
   - Merges the `default` config with that tenant’s overrides.
   - Applies the effective configuration.
4. If no tenant override exists, defaults apply unchanged.
5. Optional: expose applied overrides as metrics/log entries.

### Example Log Line

```
level=info component=compact msg="Applied tenant override" tenant="tenant-a" resolution_raw="90d" concurrency="1"
```

---

## Alternatives

| Approach | Pros | Cons |
|-----------|------|------|
| Deploy separate components per tenant | Isolation | High operational overhead |
| Use object storage lifecycle policies | Simplicity | Limited scope; not integrated with Thanos |
| Extend block labels for retention hints | Simple | Not centrally controlled; hard to audit |
| Proposed override config | Integrated, flexible | Adds config and implementation complexity |

---

## Action Plan

1. **Design & Discussion**
   - Validate config format and naming.
   - Confirm tenant identification mechanism.
2. **Implementation (Phase 1 – Compactor)**
   - Extend compactor config struct.
   - Implement `TenantOverrides` parsing and merging logic.
   - Integrate into retention and compaction scheduling.
3. **Testing**
   - Unit tests for override resolution.
   - Integration tests simulating multiple tenants.
4. **Documentation**
   - Update component docs with examples.
   - Add migration notes.
5. **Future Phases**
   - Extend support to Store Gateway, Receive, Ruler.
   - Introduce optional API-based overrides.

---

## Future Work

- Support dynamic override reloads via API or file watcher.
- Extend to query-level or ingestion-level limits.
- Add wildcard or regex-based tenant matching (`tenant-*`).
- Provide observability via `/metrics` and `/config` endpoints.
- Explore integration with external tenant metadata sources.
