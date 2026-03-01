"""Tests for scripts.utils.release_upload."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from scripts.utils.release_upload import (
    download_asset,
    get_or_create_release,
    list_assets,
    upload_asset,
)

if TYPE_CHECKING:
    from pathlib import Path

REPO = "owner/open-stock-data"
TAG = "bars-cn-2025"
TOKEN = "ghp_test_token_123"

API_BASE = "https://api.github.com"
UPLOAD_BASE = "https://uploads.github.com"


def _mock_release_payload(release_id: int = 1, tag: str = TAG) -> dict:
    return {
        "id": release_id,
        "tag_name": tag,
        "name": tag,
        "upload_url": f"{UPLOAD_BASE}/repos/{REPO}/releases/{release_id}/assets{{?name,label}}",
        "assets": [],
    }


def _mock_asset_payload(asset_id: int = 100, name: str = "daily_2025.parquet") -> dict:
    return {
        "id": asset_id,
        "name": name,
        "size": 1024,
        "browser_download_url": f"https://github.com/{REPO}/releases/download/{TAG}/{name}",
    }


# ---------------------------------------------------------------------------
# get_or_create_release
# ---------------------------------------------------------------------------


class TestGetOrCreateRelease:
    @patch("scripts.utils.release_upload.requests")
    def test_returns_existing_release(self, mock_requests: MagicMock):
        """If release already exists, return it directly."""
        release = _mock_release_payload()
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = release
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        result = get_or_create_release(REPO, TAG, TOKEN)

        assert result["tag_name"] == TAG
        mock_requests.get.assert_called_once()
        # Should NOT call post (no need to create)
        mock_requests.post.assert_not_called()

    @patch("scripts.utils.release_upload.requests")
    def test_creates_release_when_not_found(self, mock_requests: MagicMock):
        """If release doesn't exist (404), create it."""
        # GET returns 404
        get_resp = MagicMock()
        get_resp.status_code = 404
        get_resp.raise_for_status.side_effect = None
        mock_requests.get.return_value = get_resp

        # POST creates release
        release = _mock_release_payload()
        post_resp = MagicMock()
        post_resp.status_code = 201
        post_resp.json.return_value = release
        post_resp.raise_for_status = MagicMock()
        mock_requests.post.return_value = post_resp

        result = get_or_create_release(REPO, TAG, TOKEN)

        assert result["tag_name"] == TAG
        mock_requests.post.assert_called_once()

    @patch("scripts.utils.release_upload.requests")
    def test_uses_correct_auth_header(self, mock_requests: MagicMock):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = _mock_release_payload()
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        get_or_create_release(REPO, TAG, TOKEN)

        call_kwargs = mock_requests.get.call_args
        headers = call_kwargs[1].get("headers") or call_kwargs.kwargs.get("headers", {})
        assert TOKEN in headers.get("Authorization", "")


# ---------------------------------------------------------------------------
# download_asset
# ---------------------------------------------------------------------------


class TestDownloadAsset:
    @patch("scripts.utils.release_upload.requests")
    def test_downloads_asset_to_dest(self, mock_requests: MagicMock, tmp_path: Path):
        """Should download asset content to the destination path."""
        dest = tmp_path / "daily_2025.parquet"
        asset = _mock_asset_payload()
        release = _mock_release_payload()
        release["assets"] = [asset]

        # GET release
        release_resp = MagicMock()
        release_resp.status_code = 200
        release_resp.json.return_value = release
        release_resp.raise_for_status = MagicMock()

        # GET asset content
        content_resp = MagicMock()
        content_resp.status_code = 200
        content_resp.iter_content = MagicMock(return_value=[b"parquet-data"])
        content_resp.raise_for_status = MagicMock()
        content_resp.__enter__ = MagicMock(return_value=content_resp)
        content_resp.__exit__ = MagicMock(return_value=False)

        mock_requests.get.side_effect = [release_resp, content_resp]

        result = download_asset(REPO, TAG, "daily_2025.parquet", dest, TOKEN)

        assert result.exists()
        assert result.read_bytes() == b"parquet-data"

    @patch("scripts.utils.release_upload.requests")
    def test_raises_on_missing_asset(self, mock_requests: MagicMock, tmp_path: Path):
        """Should raise FileNotFoundError if asset name not in release."""
        release = _mock_release_payload()
        release["assets"] = []  # No assets

        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = release
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        with pytest.raises(FileNotFoundError, match=r"daily_2025\.parquet"):
            download_asset(REPO, TAG, "daily_2025.parquet", tmp_path / "out.parquet", TOKEN)


# ---------------------------------------------------------------------------
# upload_asset
# ---------------------------------------------------------------------------


class TestUploadAsset:
    @patch("scripts.utils.release_upload.requests")
    def test_uploads_new_asset(self, mock_requests: MagicMock, tmp_path: Path):
        """Should upload file as a new release asset."""
        file_path = tmp_path / "daily_2025.parquet"
        file_path.write_bytes(b"test-data")

        release = _mock_release_payload()
        release["assets"] = []

        # GET release
        release_resp = MagicMock()
        release_resp.status_code = 200
        release_resp.json.return_value = release
        release_resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = release_resp

        # POST upload
        asset = _mock_asset_payload()
        upload_resp = MagicMock()
        upload_resp.status_code = 201
        upload_resp.json.return_value = asset
        upload_resp.raise_for_status = MagicMock()
        mock_requests.post.return_value = upload_resp

        result = upload_asset(REPO, TAG, file_path, TOKEN)

        assert result["name"] == "daily_2025.parquet"
        mock_requests.post.assert_called_once()

    @patch("scripts.utils.release_upload.requests")
    def test_replaces_existing_asset(self, mock_requests: MagicMock, tmp_path: Path):
        """If asset already exists, delete it first then upload."""
        file_path = tmp_path / "daily_2025.parquet"
        file_path.write_bytes(b"new-data")

        existing_asset = _mock_asset_payload(asset_id=42)
        release = _mock_release_payload()
        release["assets"] = [existing_asset]

        # GET release
        release_resp = MagicMock()
        release_resp.status_code = 200
        release_resp.json.return_value = release
        release_resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = release_resp

        # DELETE existing asset
        delete_resp = MagicMock()
        delete_resp.status_code = 204
        delete_resp.raise_for_status = MagicMock()
        mock_requests.delete.return_value = delete_resp

        # POST upload
        new_asset = _mock_asset_payload(asset_id=43)
        upload_resp = MagicMock()
        upload_resp.status_code = 201
        upload_resp.json.return_value = new_asset
        upload_resp.raise_for_status = MagicMock()
        mock_requests.post.return_value = upload_resp

        result = upload_asset(REPO, TAG, file_path, TOKEN)

        mock_requests.delete.assert_called_once()
        mock_requests.post.assert_called_once()
        assert result["id"] == 43

    def test_raises_on_missing_file(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            upload_asset(REPO, TAG, tmp_path / "nonexistent.parquet", TOKEN)


# ---------------------------------------------------------------------------
# list_assets
# ---------------------------------------------------------------------------


class TestListAssets:
    @patch("scripts.utils.release_upload.requests")
    def test_returns_asset_list(self, mock_requests: MagicMock):
        release = _mock_release_payload()
        release["assets"] = [
            _mock_asset_payload(asset_id=1, name="daily_2024.parquet"),
            _mock_asset_payload(asset_id=2, name="daily_2025.parquet"),
        ]

        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = release
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        result = list_assets(REPO, TAG, TOKEN)

        assert len(result) == 2
        assert result[0]["name"] == "daily_2024.parquet"

    @patch("scripts.utils.release_upload.requests")
    def test_returns_empty_list_when_no_assets(self, mock_requests: MagicMock):
        release = _mock_release_payload()
        release["assets"] = []

        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = release
        resp.raise_for_status = MagicMock()
        mock_requests.get.return_value = resp

        result = list_assets(REPO, TAG, TOKEN)

        assert result == []
