#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2021-2025 Constantine Evans <qslib@mb.costi.net>
# SPDX-License-Identifier: EUPL-1.2
"""Download and install qslib wheel from GitHub Actions artifacts."""

import io
import json
import os
import subprocess
import sys
import zipfile
from urllib.error import HTTPError
from urllib.request import Request, urlopen

REPO = "cgevans/qslib"
ARTIFACT_NAME = "wheels-linux-x86_64"


def _make_request(url: str, token: str) -> dict:
    """Make authenticated GitHub API request, handling errors."""
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    req = Request(url, headers=headers)
    try:
        with urlopen(req) as resp:
            return json.load(resp)
    except HTTPError as e:
        error_body = e.read().decode("utf-8")
        try:
            error_data = json.loads(error_body)
            error_msg = error_data.get("message", error_body)
        except json.JSONDecodeError:
            error_msg = error_body
        raise RuntimeError(
            f"GitHub API request failed: {e.code} {e.reason}\n{error_msg}"
        ) from e


def get_artifact_download_url(commit_sha: str, token: str | None = None) -> str:
    """Find artifact download URL for a commit."""
    if not token:
        raise RuntimeError("Token required for artifact access")

    # Find workflow runs for this commit
    url = f"https://api.github.com/repos/{REPO}/actions/runs?head_sha={commit_sha}"
    runs = _make_request(url, token)

    if not runs.get("workflow_runs"):
        raise RuntimeError(f"No workflow runs found for commit {commit_sha}")

    run_id = runs["workflow_runs"][0]["id"]

    # Get artifacts for this run
    url = f"https://api.github.com/repos/{REPO}/actions/runs/{run_id}/artifacts"
    artifacts = _make_request(url, token)

    for artifact in artifacts.get("artifacts", []):
        if artifact["name"] == ARTIFACT_NAME:
            return artifact["archive_download_url"]

    raise RuntimeError(f"Artifact {ARTIFACT_NAME} not found in run {run_id}")


def download_and_extract_wheel(download_url: str, token: str) -> str:
    """Download artifact zip and extract wheel, return wheel path."""
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
    }
    req = Request(download_url, headers=headers)
    try:
        with urlopen(req) as resp:
            zip_data = resp.read()
    except HTTPError as e:
        error_body = e.read().decode("utf-8")
        try:
            error_data = json.loads(error_body)
            error_msg = error_data.get("message", error_body)
        except json.JSONDecodeError:
            error_msg = error_body
        raise RuntimeError(
            f"Failed to download artifact: {e.code} {e.reason}\n{error_msg}"
        ) from e

    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        wheel_files = [n for n in zf.namelist() if n.endswith(".whl")]
        if not wheel_files:
            raise RuntimeError("No wheel file found in artifact")
        wheel_name = wheel_files[0]
        zf.extract(wheel_name, ".")
        return wheel_name


def main():
    commit = os.environ.get("READTHEDOCS_GIT_COMMIT_HASH")
    if not commit:
        print("READTHEDOCS_GIT_COMMIT_HASH not set, falling back to HEAD")
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()

    token = os.environ.get("RTD_GH_TOKEN")
    if not token:
        raise RuntimeError("RTD_GH_TOKEN environment variable required")

    print(f"Finding artifact for commit {commit}")
    download_url = get_artifact_download_url(commit, token)
    print(f"Downloading from {download_url}")
    wheel_path = download_and_extract_wheel(download_url, token)
    print(f"Installing {wheel_path}")
    subprocess.check_call([sys.executable, "-m", "pip", "install", wheel_path + "[docs]"])


if __name__ == "__main__":
    main()

