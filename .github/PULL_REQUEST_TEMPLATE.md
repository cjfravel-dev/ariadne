## Summary

Describe the behavior change and affected public or persisted surfaces.

## Related Issue

Closes #

## Test-first evidence

- **RED:** Identify the failing test added before implementation.
- **GREEN:** Identify the narrow command that passes after implementation.

## Compatibility

- **Spark lines:** Spark 3.5 / Scala 2.12 and Spark 4.1 / Scala 2.13 impact.
- **Persisted storage:** State whether metadata, Delta schemas, FileList, staging, or large indexes change.
- **Oldest verified index:** State the oldest persisted generation exercised.
- **Migration safety:** Describe idempotence, lock ownership, failure recovery, and rollback.

## Contract checklist

- [ ] RED was confirmed before implementation and GREEN after it.
- [ ] Both Spark build lines are covered where behavior overlaps.
- [ ] `CHANGELOG.md` is updated for user-visible behavior.
- [ ] User/contributor documentation is updated where applicable.
- [ ] Scaladoc is updated and `docs/api/` regenerated for API changes.
- [ ] Dependency scope and shading remain safe for host Spark environments.
- [ ] I agree to the [Contributor License Agreement](CLA.md)
