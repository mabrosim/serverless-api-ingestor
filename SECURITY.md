# Security policy

## Confidential reporting

After the GitHub repository is created, maintainers must enable and verify private vulnerability
reporting before public exposure. Verify hosted CI/security workflows and repository publication
settings at that stage too; this document does not assert an existing GitHub repository or an
already available reporting channel. Once enabled, use the repository's private vulnerability
reporting route. Do not post sensitive reports publicly while that route is unavailable.

Never include secrets, account metadata, private API endpoints, or production observations in
public issues or scanner reports.

## Secret scanning

Before initial push/publication, select the exact intended refs, review metadata privately, and
scan their history with full redaction. From the repository root, this scans current HEAD ancestry:

```bash
gitleaks git --redact=100 .
git grep -En '(AKIA|ASIA)[A-Z0-9]{16}|arn:aws:|[0-9]{12}'
```

The grep command is a local shape check, not a redacted report; inspect output privately and do not
publish raw matches. HEAD ancestry does not cover every local ref/reflog or a different release set.
Also scan a sanitized candidate tree containing every intended source, root document, and workflow.
Set `CANDIDATE_DIR` to that separately prepared tree before running:

```bash
gitleaks dir --redact=100 "$CANDIDATE_DIR"
```

Do not scan a mixed workspace root into shareable reports: ignored configs, exports, and generated
trees can contain private material. Review synthetic fixtures manually; a clean scan is not proof
that every example or dependency is safe.

Root [.gitleaksignore](.gitleaksignore) contains only two exact initial-commit fingerprints for
`tests/unit/test_ingestion.py`. Both `generic-api-key` matches captured the boolean keyword
`WithDecryption=True` in mocked SSM assertions, not a credential. Current mocks use neutral test
parameter paths and low-entropy test-only values. This is a verified false-positive disposition,
not evidence of a leaked secret or a rotation incident. No rule or directory suppression is used;
review any new finding independently rather than expanding exceptions to hide it.

If a real committed credential is confirmed, stop publication and follow approved incident handling
and rotation; deleting the latest copy is insufficient. History changes require separate approval.
