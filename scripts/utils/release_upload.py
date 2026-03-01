"""GitHub Releases asset upload/download utilities.

Provides functions to interact with GitHub Releases API for managing
parquet data files as release assets.
"""

from __future__ import annotations

import logging
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

API_BASE = "https://api.github.com"
UPLOAD_BASE = "https://uploads.github.com"


def _headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _get_release(repo: str, tag: str, token: str) -> dict | None:
    """Fetch release by tag. Returns None if not found (404)."""
    url = f"{API_BASE}/repos/{repo}/releases/tags/{tag}"
    resp = requests.get(url, headers=_headers(token), timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def get_or_create_release(repo: str, tag: str, token: str) -> dict:
    """Get an existing release by tag, or create it if it doesn't exist.

    Args:
        repo: GitHub repo in "owner/repo" format.
        tag: Release tag name (e.g. "bars-cn-2025").
        token: GitHub personal access token or GITHUB_TOKEN.

    Returns:
        Release metadata dict from GitHub API.

    Raises:
        requests.HTTPError: On API failure (non-404).
    """
    release = _get_release(repo, tag, token)
    if release is not None:
        return release

    logger.info("Release '%s' not found, creating...", tag)
    url = f"{API_BASE}/repos/{repo}/releases"
    payload = {"tag_name": tag, "name": tag, "draft": False, "prerelease": False}
    resp = requests.post(url, headers=_headers(token), json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def download_asset(repo: str, tag: str, asset_name: str, dest: str | Path, token: str) -> Path:
    """Download a specific asset from a GitHub Release.

    Args:
        repo: GitHub repo in "owner/repo" format.
        tag: Release tag name.
        asset_name: Name of the asset file to download.
        dest: Local destination path for the downloaded file.
        token: GitHub token.

    Returns:
        Path to the downloaded file.

    Raises:
        FileNotFoundError: If the asset does not exist in the release.
        requests.HTTPError: On API failure.
    """
    dest = Path(dest)
    release = _get_release(repo, tag, token)
    if release is None:
        raise FileNotFoundError(f"Release '{tag}' not found in {repo}")

    # Find asset by name
    asset = next((a for a in release.get("assets", []) if a["name"] == asset_name), None)
    if asset is None:
        raise FileNotFoundError(f"Asset '{asset_name}' not found in release '{tag}'")

    # Download via browser_download_url with auth
    download_url = asset["browser_download_url"]
    headers = _headers(token)
    headers["Accept"] = "application/octet-stream"

    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(download_url, headers=headers, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

    return dest


def upload_asset(repo: str, tag: str, file_path: str | Path, token: str) -> dict:
    """Upload a file as an asset to a GitHub Release.

    If an asset with the same name already exists, it is deleted first
    and replaced with the new file.

    Args:
        repo: GitHub repo in "owner/repo" format.
        tag: Release tag name.
        file_path: Path to the local file to upload.
        token: GitHub token.

    Returns:
        Asset metadata dict from GitHub API.

    Raises:
        FileNotFoundError: If file_path does not exist.
        requests.HTTPError: On API failure.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    release = get_or_create_release(repo, tag, token)
    asset_name = file_path.name

    # Delete existing asset with same name if present
    for existing in release.get("assets", []):
        if existing["name"] == asset_name:
            logger.info("Deleting existing asset '%s' (id=%s)", asset_name, existing["id"])
            del_url = f"{API_BASE}/repos/{repo}/releases/assets/{existing['id']}"
            resp = requests.delete(del_url, headers=_headers(token), timeout=30)
            resp.raise_for_status()
            break

    # Upload new asset
    upload_url = release["upload_url"].split("{")[0]  # Strip {?name,label} template
    headers = _headers(token)
    headers["Content-Type"] = "application/octet-stream"

    with open(file_path, "rb") as f:
        resp = requests.post(
            upload_url,
            headers=headers,
            params={"name": asset_name},
            data=f,
            timeout=600,
        )
    resp.raise_for_status()
    return resp.json()


def list_assets(repo: str, tag: str, token: str) -> list[dict]:
    """List all assets in a GitHub Release.

    Args:
        repo: GitHub repo in "owner/repo" format.
        tag: Release tag name.
        token: GitHub token.

    Returns:
        List of asset metadata dicts.

    Raises:
        requests.HTTPError: On API failure.
    """
    release = _get_release(repo, tag, token)
    if release is None:
        return []
    return release.get("assets", [])
