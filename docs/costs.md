# Cost controls

Near-zero cost is a design target, not a guarantee.

## Contributor approval policy

Before proposing or running an AWS operation, test, or design that may incur charges, contributors
must identify the possible charge, present a near-zero-cost alternative, and obtain explicit user
approval. Local-only validation is freely runnable.

Examples requiring approval include DynamoDB `Scan` or export operations, point-in-time recovery,
S3 storage or exports, and CloudWatch alarms.

Primary cost drivers:

- invocation frequency and Lambda duration
- external API plan
- DynamoDB provisioned capacity, request volume, and item size
- CloudWatch log ingestion and storage
- CloudWatch alarms when `alarms.enabled` is explicitly set to `true`
- SSM/KMS choices
- data transfer and AWS Region

Alarms are disabled by default to avoid their recurring per-alarm charges. Enable them only when the
operational benefit justifies the cost.

The configured DynamoDB table uses the explicitly requested minimum provisioned capacity of one read
and one write capacity unit. This incurs DynamoDB provisioned-capacity charges while the table exists.

The weather example needs no paid credential storage, but API terms and AWS charges still apply.
The scaffold disables scheduling and leaves its expression unfinished; choose the interval explicitly.
A disabled schedule does not remove table
capacity charges or prevent direct smoke-test invocations from calling the API and writing to AWS.
Each additional stack normally adds its own provisioned table and duplicates capacity charges;
stack-derived naming isolates resources, not costs.

Snapshot contract v2 writes one selected-field row per site/source-time sample, rather than the old
two metric rows. Smaller/fewer writes do not guarantee a lower bill at the provisioned 1-WCU floor;
item size, throughput, and storage still matter. ARM64, 14-day logs, least-privilege IAM, and destructive
table/log lifecycle policies are unchanged. No data migration or new infrastructure is included.

Whole-second numeric keys do not guarantee a lower bill at the provisioned 1-WCU floor.
Every successful observation issues a whole-item write, including
unchanged polls, same-time revisions, and retries; there is no duplicate-skip cost avoidance.
TTL remains source-based and does not extend per poll. No cloud cost savings have been measured.
The key/replacement change uses only stdlib plus managed Lambda SDK/urllib3 libraries; it adds no
bundled dependencies, resources, or IAM permissions. Historical rows are not automatically removed.

Code-only packaging and native JSON logging add no resources or permissions. ARM64, least-privilege
IAM, and 14-day default log retention remain unchanged. No cloud savings have been measured;
log volume may change. A smaller deployment ZIP does not directly reduce Lambda compute pricing;
invocation frequency, billed duration, and configured memory still matter.

`make check` and `make validate` avoid provider/AWS calls. The code-only SAM application build
installs no third-party pip dependencies; local bootstrap and tool installation can download packages.
The manual API preparation GET is separate: review provider terms/rate limits before sending it.
Manual deployment reruns readiness and requires changeset confirmation, not automatic cost approval.

Defaults minimize persistent and idle resources. Keep INFO logs compact. Never log full provider
payloads. TTL bounds long-term DynamoDB storage. Standard SSM parameters and the AWS-managed key are
the low-complexity default.

Review AWS pricing before deployment and create an account-level AWS Budget outside this application if needed.
