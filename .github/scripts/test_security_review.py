"""Tests for security_review.py — consolidated comment, size splitting, and cleanup flows."""

import json
import unittest
from unittest.mock import MagicMock, patch

from security_review import (
    _BOT_MARKER,
    _post_comment_safe,
    delete_old_bot_comments,
    parse_claude_response,
    post_review_comments,
    post_analysis_failed_comment,
)


_NEXT_ID = 1


class FakeComment:
    """Minimal mock for a GitHub comment object."""

    def __init__(self, login: str, body: str, comment_id: int | None = None):
        global _NEXT_ID
        self.user = MagicMock()
        self.user.login = login
        self.body = body
        self.id = comment_id if comment_id is not None else _NEXT_ID
        _NEXT_ID += 1
        self.deleted = False

    def delete(self):
        self.deleted = True


class FakePR:
    """Fake PR object that tracks comments for integration-style tests."""

    def __init__(self, existing_comments: list[FakeComment] | None = None):
        self._issue_comments: list[FakeComment] = list(existing_comments or [])
        self._review_comments: list[FakeComment] = []

    def get_issue_comments(self):
        # Return a snapshot so iteration isn't affected by mutation
        return list(self._issue_comments)

    def get_review_comments(self):
        return list(self._review_comments)

    def create_issue_comment(self, body: str) -> FakeComment:
        comment = FakeComment("github-actions[bot]", body)
        self._issue_comments.append(comment)
        return comment

    @property
    def live_comments(self) -> list[FakeComment]:
        """Return comments that haven't been deleted."""
        return [c for c in self._issue_comments if not c.deleted]


# ---------------------------------------------------------------------------
# Unit tests for _post_comment_safe
# ---------------------------------------------------------------------------


class TestPostCommentSafe(unittest.TestCase):
    """Tests for _post_comment_safe (comment size splitting)."""

    def test_small_body_posts_single_comment(self):
        pr = FakePR()
        body = "Short body"
        ids = _post_comment_safe(pr, body)
        assert len(pr.live_comments) == 1
        assert pr.live_comments[0].body == body
        assert len(ids) == 1

    def test_body_at_limit_posts_single_comment(self):
        pr = FakePR()
        body = "x" * 60000
        ids = _post_comment_safe(pr, body)
        assert len(pr.live_comments) == 1
        assert len(ids) == 1

    def test_oversized_body_splits_into_multiple_comments(self):
        pr = FakePR()
        section = "A" * 30000
        body = f"{section}\n\n---\n\n{section}\n\n---\n\n{section}"
        ids = _post_comment_safe(pr, body)
        assert len(pr.live_comments) >= 2, (
            f"Expected >= 2 comments, got {len(pr.live_comments)}"
        )
        assert len(ids) >= 2
        # Each chunk must respect the 60K size limit
        for c in pr.live_comments:
            assert len(c.body) <= 60000, (
                f"Chunk exceeds 60K limit: {len(c.body)} chars"
            )

    def test_split_comments_contain_continuation_notice(self):
        pr = FakePR()
        section = "B" * 30000
        body = f"{section}\n\n---\n\n{section}\n\n---\n\n{section}"
        _post_comment_safe(pr, body)
        comments = pr.live_comments
        assert "Continued in next comment" in comments[0].body
        assert "continued" in comments[-1].body

    def test_split_comments_all_contain_bot_marker(self):
        """Every split chunk should contain the bot marker for reliable cleanup."""
        pr = FakePR()
        section = "C" * 30000
        body = f"{_BOT_MARKER}\n{section}\n\n---\n\n{section}\n\n---\n\n{section}"
        _post_comment_safe(pr, body)
        for comment in pr.live_comments:
            assert _BOT_MARKER in comment.body, (
                f"Bot marker missing from chunk: {comment.body[:100]}..."
            )

    def test_single_oversized_section_is_hard_wrapped(self):
        """A single section exceeding MAX_COMMENT_SIZE should be hard-wrapped."""
        pr = FakePR()
        # Single section with no --- separators, way over 60K
        body = "X" * 130000
        ids = _post_comment_safe(pr, body)
        assert len(pr.live_comments) >= 2, (
            f"Expected >= 2 comments for 130K body, got {len(pr.live_comments)}"
        )
        # Reassembled content should contain all original characters
        total_len = sum(len(c.body) for c in pr.live_comments)
        assert total_len > 130000  # Original content plus continuation markers
        # Each chunk must respect the 60K size limit
        for c in pr.live_comments:
            assert len(c.body) <= 60000, (
                f"Hard-wrapped chunk exceeds 60K limit: {len(c.body)} chars"
            )


# ---------------------------------------------------------------------------
# Unit tests for delete_old_bot_comments
# ---------------------------------------------------------------------------


class TestDeleteOldBotComments(unittest.TestCase):
    """Tests for delete_old_bot_comments (issue comment cleanup)."""

    def test_deletes_matching_issue_comments(self):
        pr = MagicMock()
        bot_comment = FakeComment("github-actions[bot]", "## Security Review: blah\n*Powered by Claude*")
        other_comment = FakeComment("someuser", "LGTM")
        pr.get_issue_comments.return_value = [bot_comment, other_comment]

        deleted = delete_old_bot_comments(pr)

        assert deleted == 1
        assert bot_comment.deleted
        assert not other_comment.deleted

    def test_deletes_comments_with_bot_marker(self):
        """Comments with the HTML bot marker should be cleaned up."""
        pr = MagicMock()
        marked_comment = FakeComment("github-actions[bot]", f"{_BOT_MARKER}\n## Security Review (continued)\nsome text")
        pr.get_issue_comments.return_value = [marked_comment]

        deleted = delete_old_bot_comments(pr)

        assert deleted == 1
        assert marked_comment.deleted

    def test_skips_non_bot_comments(self):
        pr = MagicMock()
        human_comment = FakeComment("developer", "Security Review looks good\nPowered by Claude")
        pr.get_issue_comments.return_value = [human_comment]

        deleted = delete_old_bot_comments(pr)

        assert deleted == 0
        assert not human_comment.deleted

    def test_skips_excluded_ids(self):
        """Comments in exclude_ids should not be deleted."""
        pr = MagicMock()
        new_comment = FakeComment("github-actions[bot]", f"{_BOT_MARKER}\n## Security Review: pass", comment_id=999)
        old_comment = FakeComment("github-actions[bot]", f"{_BOT_MARKER}\n## Security Review: old", comment_id=100)
        pr.get_issue_comments.return_value = [new_comment, old_comment]

        deleted = delete_old_bot_comments(pr, exclude_ids={999})

        assert deleted == 1
        assert not new_comment.deleted
        assert old_comment.deleted

    def test_handles_delete_failure_gracefully(self):
        pr = MagicMock()
        bot_comment = FakeComment("github-actions[bot]", "## Security Review: x\n*Powered by Claude*")
        bot_comment.delete = MagicMock(side_effect=Exception("403 Forbidden"))
        pr.get_issue_comments.return_value = [bot_comment]

        deleted = delete_old_bot_comments(pr)
        assert deleted == 0


# ---------------------------------------------------------------------------
# Unit tests for post_review_comments
# ---------------------------------------------------------------------------


_FAKE_ENV = {
    "GITHUB_TOKEN": "fake-token",
    "REPO_NAME": "owner/repo",
    "PR_NUMBER": "42",
}


class TestPostReviewComments(unittest.TestCase):
    """Tests for post_review_comments (post-then-delete ordering)."""

    @patch.dict("os.environ", _FAKE_ENV)
    @patch("security_review.Github")
    @patch("security_review.delete_old_bot_comments")
    @patch("security_review._post_comment_safe")
    def test_posts_before_deleting(self, mock_post, mock_delete, mock_github):
        """Verify new comment is posted before old ones are deleted."""
        mock_pr = MagicMock()
        mock_github.return_value.get_repo.return_value.get_pull.return_value = mock_pr
        mock_delete.return_value = 0

        call_order = []
        mock_post.side_effect = lambda pr, body: (call_order.append("post"), [])[1]
        mock_delete.side_effect = lambda pr, exclude_ids=None: (call_order.append("delete"), 0)[1]

        issues = [
            {"file": "foo.go", "line": 10, "severity": "high", "title": "Test", "description": "Desc"}
        ]

        post_review_comments(issues)

        assert call_order == ["post", "delete"], f"Expected post before delete, got {call_order}"

    @patch.dict("os.environ", _FAKE_ENV)
    @patch("security_review.Github")
    @patch("security_review.delete_old_bot_comments")
    @patch("security_review._post_comment_safe")
    def test_does_not_delete_if_post_fails(self, mock_post, mock_delete, mock_github):
        """If posting fails, old comments should NOT be deleted."""
        mock_pr = MagicMock()
        mock_github.return_value.get_repo.return_value.get_pull.return_value = mock_pr
        mock_post.side_effect = Exception("422 Unprocessable Entity")

        issues = [
            {"file": "foo.go", "line": 10, "severity": "high", "title": "Test", "description": "Desc"}
        ]

        with self.assertRaises(Exception):
            post_review_comments(issues)

        mock_delete.assert_not_called()

    @patch.dict("os.environ", _FAKE_ENV)
    @patch("security_review.Github")
    @patch("security_review.delete_old_bot_comments")
    @patch("security_review._post_comment_safe")
    def test_no_findings_still_posts(self, mock_post, mock_delete, mock_github):
        """Even with no actionable findings, a 'no issues' comment is posted."""
        mock_pr = MagicMock()
        mock_github.return_value.get_repo.return_value.get_pull.return_value = mock_pr
        mock_delete.return_value = 0
        mock_post.return_value = []

        post_review_comments([])

        mock_post.assert_called_once()
        body = mock_post.call_args[0][1]
        assert "No High/Medium Issues Found" in body

    @patch.dict("os.environ", _FAKE_ENV)
    @patch("security_review.Github")
    @patch("security_review.delete_old_bot_comments")
    @patch("security_review._post_comment_safe")
    def test_passes_exclude_ids_to_delete(self, mock_post, mock_delete, mock_github):
        """Verify that new comment IDs are passed as exclude_ids to delete."""
        mock_pr = MagicMock()
        mock_github.return_value.get_repo.return_value.get_pull.return_value = mock_pr
        mock_post.return_value = [42, 43]
        mock_delete.return_value = 0

        issues = [
            {"file": "foo.go", "line": 10, "severity": "high", "title": "Test", "description": "Desc"}
        ]

        post_review_comments(issues)

        mock_delete.assert_called_once()
        _, kwargs = mock_delete.call_args
        assert kwargs["exclude_ids"] == {42, 43}


# ---------------------------------------------------------------------------
# Unit tests for post_analysis_failed_comment
# ---------------------------------------------------------------------------


class TestPostAnalysisFailedComment(unittest.TestCase):
    """Tests for post_analysis_failed_comment (post-then-delete ordering)."""

    @patch.dict("os.environ", _FAKE_ENV)
    @patch("security_review.Github")
    @patch("security_review.delete_old_bot_comments")
    def test_posts_before_deleting(self, mock_delete, mock_github):
        mock_pr = MagicMock()
        mock_github.return_value.get_repo.return_value.get_pull.return_value = mock_pr
        mock_delete.return_value = 0

        call_order = []
        fake_result = MagicMock()
        fake_result.id = 500
        mock_pr.create_issue_comment.side_effect = lambda body: (call_order.append("post"), fake_result)[1]
        mock_delete.side_effect = lambda pr, exclude_ids=None: (call_order.append("delete"), 0)[1]

        post_analysis_failed_comment()

        assert call_order == ["post", "delete"], f"Expected post before delete, got {call_order}"

    @patch.dict("os.environ", _FAKE_ENV)
    @patch("security_review.Github")
    @patch("security_review.delete_old_bot_comments")
    def test_passes_exclude_ids_to_delete(self, mock_delete, mock_github):
        """Verify the newly posted comment ID is excluded from deletion."""
        mock_pr = MagicMock()
        mock_github.return_value.get_repo.return_value.get_pull.return_value = mock_pr
        fake_result = MagicMock()
        fake_result.id = 777
        mock_pr.create_issue_comment.return_value = fake_result
        mock_delete.return_value = 0

        post_analysis_failed_comment()

        mock_delete.assert_called_once()
        _, kwargs = mock_delete.call_args
        assert 777 in kwargs["exclude_ids"]


# ---------------------------------------------------------------------------
# Integration-style tests (real _post_comment_safe + delete_old_bot_comments)
# ---------------------------------------------------------------------------


class TestIntegrationPostThenDelete(unittest.TestCase):
    """Integration tests using FakePR to verify real post-then-delete behavior."""

    @patch.dict("os.environ", _FAKE_ENV)
    @patch("security_review.Github")
    def test_new_comment_not_self_deleted(self, mock_github):
        """The newly posted comment must survive the subsequent cleanup."""
        old_comment = FakeComment(
            "github-actions[bot]",
            f"{_BOT_MARKER}\n## Security Review: old findings\n*Powered by Claude 4.5 Sonnet*",
        )
        pr = FakePR(existing_comments=[old_comment])
        mock_github.return_value.get_repo.return_value.get_pull.return_value = pr

        post_review_comments([])

        # Old comment should be deleted
        assert old_comment.deleted, "Old bot comment should have been deleted"
        # Exactly one live comment should remain (the new one)
        live = pr.live_comments
        assert len(live) == 1, f"Expected 1 live comment, got {len(live)}"
        assert "No High/Medium Issues Found" in live[0].body

    @patch.dict("os.environ", _FAKE_ENV)
    @patch("security_review.Github")
    def test_split_comments_not_self_deleted(self, mock_github):
        """When comments are split, ALL new chunks must survive cleanup."""
        old_comment = FakeComment(
            "github-actions[bot]",
            f"{_BOT_MARKER}\n## Security Review: old\n*Powered by Claude 4.5 Sonnet*",
        )
        pr = FakePR(existing_comments=[old_comment])
        mock_github.return_value.get_repo.return_value.get_pull.return_value = pr

        # Generate enough findings to produce a large body
        issues = [
            {
                "file": f"pkg/file{i}.go",
                "line": i,
                "severity": "high",
                "title": f"Finding {i}",
                "description": "D" * 4000,  # 4K per finding
            }
            for i in range(20)  # ~80K total, should split
        ]

        post_review_comments(issues)

        assert old_comment.deleted, "Old bot comment should have been deleted"
        live = pr.live_comments
        assert len(live) >= 2, f"Expected >= 2 live comments (split), got {len(live)}"
        # All live comments should contain the bot marker and respect size limit
        for c in live:
            assert _BOT_MARKER in c.body
            assert len(c.body) <= 60000, (
                f"Split chunk exceeds 60K limit: {len(c.body)} chars"
            )

    @patch.dict("os.environ", _FAKE_ENV)
    @patch("security_review.Github")
    def test_failed_comment_not_self_deleted(self, mock_github):
        """post_analysis_failed_comment should not self-delete its own comment."""
        old_comment = FakeComment(
            "github-actions[bot]",
            f"{_BOT_MARKER}\n## Security Review: old\n*Powered by Claude 4.5 Sonnet*",
        )
        pr = FakePR(existing_comments=[old_comment])
        mock_github.return_value.get_repo.return_value.get_pull.return_value = pr

        post_analysis_failed_comment()

        assert old_comment.deleted
        live = pr.live_comments
        assert len(live) == 1
        assert "Analysis Failed" in live[0].body


# ---------------------------------------------------------------------------
# Edge-case tests for parse_claude_response
# ---------------------------------------------------------------------------


class TestParseClaudeResponse(unittest.TestCase):
    """Edge-case tests for parse_claude_response."""

    def test_oversized_description_rejected(self):
        """Issues with descriptions over 5000 chars should be filtered out."""
        huge = "x" * 5001
        response = f'[{{"file":"a.go","line":1,"severity":"high","title":"T","description":"{huge}"}}]'
        result = parse_claude_response(response)
        assert result == []

    def test_max_50_issues(self):
        """More than 50 valid issues should be capped at 50."""
        issues = [
            {"file": "a.go", "line": i, "severity": "high", "title": "T", "description": "D"}
            for i in range(60)
        ]
        result = parse_claude_response(json.dumps(issues))
        assert len(result) == 50


if __name__ == "__main__":
    unittest.main()
