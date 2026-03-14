#!/usr/bin/env python3
"""
Generates a markdown test summary from Ginkgo's JSON report.

Usage: python3 generate_test_summary.py <path-to-test-report.json>

Reads Ginkgo v2 JSON report and outputs markdown to stdout.
Pipe to $GITHUB_STEP_SUMMARY in CI.
"""

import json
import os
import sys

FAILURE_STATES = {"failed", "panicked", "interrupted", "aborted"}
SKIPPED_STATES = {"skipped", "pending"}
STATE_ICONS = {"passed": "✅", "failed": "❌", "panicked": "💥", "interrupted": "⚠️", "aborted": "⚠️"}

# Ginkgo/K8s e2e framework prefixes that add noise to test names.
NOISY_PREFIXES = ("[sig-", "[Driver:", "[Testpattern:")


def build_test_name(spec):
    """Build a readable test name from the container hierarchy and leaf node text."""
    parts = [
        t.strip() for t in spec.get("ContainerHierarchyTexts", [])
        if t.strip() and not t.strip().startswith(NOISY_PREFIXES)
    ]
    leaf = spec.get("LeafNodeText", "").strip()
    if leaf:
        parts.append(leaf)
    return " > ".join(parts) or "(unnamed)"


def format_duration(ns):
    """Format nanosecond duration to human-readable string."""
    seconds = ns / 1_000_000_000
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    return f"{int(seconds // 60)}m {seconds % 60:.0f}s"


def get_failure_message(spec):
    """Extract failure message from spec, including AdditionalFailure if present."""
    parts = []
    if spec.get("Failure"):
        msg = spec["Failure"].get("Message", "")
        if msg:
            parts.append(msg)
    if spec.get("AdditionalFailure"):
        msg = spec["AdditionalFailure"].get("Failure", {}).get("Message", "")
        if msg:
            parts.append(msg)
    return "\n".join(parts)


def parse_report(report_path):
    """Parse Ginkgo JSON report and return list of spec results."""
    with open(report_path) as f:
        suites = json.load(f)

    specs = []
    for suite in suites:
        for spec in suite.get("SpecReports", []):
            state = spec.get("State", "unknown")
            # Skip suite setup/teardown entries with no test name
            if not spec.get("LeafNodeText", "").strip() and state == "passed":
                continue
            specs.append({
                "name": build_test_name(spec),
                "state": state,
                "duration": spec.get("RunTime", 0),
                "num_attempts": spec.get("NumAttempts", 1),
                "failure_message": get_failure_message(spec),
            })
    return specs


def generate_markdown(specs):
    """Generate markdown summary from parsed specs."""
    if not specs:
        print("### E2E Test Results\n\nNo test results found.")
        return

    passed = [s for s in specs if s["state"] == "passed"]
    failed = [s for s in specs if s["state"] in FAILURE_STATES]
    skipped = [s for s in specs if s["state"] in SKIPPED_STATES]

    status = "❌ Has failures" if failed else "✅ All passed"
    print(f"### E2E Test Results — {status}\n")
    print(f"**{len(passed)}** passed, **{len(failed)}** failed, **{len(skipped)}** skipped, **{len(specs)}** total\n")

    # Failed tests — expanded, with full error logs
    if failed:
        print("<details open>")
        print(f"<summary>❌ Failed Tests ({len(failed)})</summary>\n")
        for s in failed:
            icon = STATE_ICONS.get(s["state"], "❓")
            print(f"#### {icon} {s['name']} ({format_duration(s['duration'])})\n")
            if s["failure_message"]:
                print(f"```\n{s['failure_message']}\n```\n")
        print("</details>\n")

    # Passed tests — collapsed table
    if passed:
        print("<details>")
        print(f"<summary>✅ Passed Tests ({len(passed)})</summary>\n")
        print("| Test | Duration | Attempts |")
        print("|------|----------|----------|")
        for s in passed:
            print(f"| {s['name']} | {format_duration(s['duration'])} | {s['num_attempts']} |")
        print("\n</details>\n")

    if skipped:
        print(f"⏭️ {len(skipped)} tests skipped\n")


def main():
    if len(sys.argv) < 2:
        print("Usage: generate_test_summary.py <path-to-test-report.json>", file=sys.stderr)
        sys.exit(1)

    report_path = sys.argv[1]
    if not os.path.exists(report_path):
        print("### E2E Test Results\n\n⚠️ No test report file found. Tests may not have run.")
        sys.exit(0)

    try:
        specs = parse_report(report_path)
    except (json.JSONDecodeError, KeyError) as e:
        print(f"### E2E Test Results\n\n⚠️ Failed to parse test report: {e}")
        sys.exit(0)

    generate_markdown(specs)


if __name__ == "__main__":
    main()
