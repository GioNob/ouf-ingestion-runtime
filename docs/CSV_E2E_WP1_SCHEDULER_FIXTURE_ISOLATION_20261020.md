# CSV end-to-end WP1: scheduler fixture isolation

The global scheduler deliberately selects eligible work across tenants. The full
module run39380500855 selected tenant-budget/source fixtures retained by preceding
test classes in RunControlRuntimeTest, causing2assertion failures and1bundle/source
mismatch. These tests assumed their newly inserted schedule was the only due work.

RunControlRuntimeTest now pauses pre-existing ACTIVE schedules inside each test's
rollback transaction. Test changes, including policy/global pressure settings,
are rolled back; retained rows/history are neither deleted nor changed permanently.
The production scheduler and tenant selection remain unchanged. An explicit test
verifies that a backed-off source does not block another ready tenant.

Qualification requires the complete Java21/PostgreSQL17 module suite, including
HumanRecoveryBudgetRuntimeTest before RunControlRuntimeTest. Local syntax review
is not a substitute for that CI. No target database or runtime is involved.
