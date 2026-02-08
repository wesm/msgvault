"""Tests for security_review.py — consolidated comment, size splitting, and cleanup flows."""

import unittest
from unittest.mock import MagicMock, patch

from security_review import (
    _post_comment_safe,
    delete_old_bot_comments,
    parse_claude_response,
    post_review_comments,
    post_analysis_failed_comment,
)


class FakeComment:
    """Minimal mock for a GitHub comment object."""

    def __init__(self, login: str, body: str):
        self.user = MagicMock()
        self.user.login = login
        self.body = body
        self.deleted = False

    def delete(self):
        self.deleted = True


class TestPostCommentSafe(unittest.TestCase):
    """Tests for _post_comment_safe (comment size splitting)."""

    def test_small_body_posts_single_comment(self):
        pr = MagicMock()
        body = "Short body"
        _post_comment_safe(pr, body)
        pr.create_issue_comment.assert_called_once_with(body)

    def test_body_at_limit_posts_single_comment(self):
        pr = MagicMock()
        body = "x" * 60000
        _post_comment_safe(pr, body)
        pr.create_issue_comment.assert_called_once_with(body)

    def test_oversized_body_splits_into_multiple_comments(self):
        pr = MagicMock()
        # Build a body with multiple sections separated by ---
        section = "A" * 30000
        body = f"{section}\n\n---\n\n{section}\n\n---\n\n{section}"
        _post_comment_safe(pr, body)
        # Should have been split into multiple comments
        assert pr.create_issue_comment.call_count >= 2, (
            f"Expected >= 2 comments, got {pr.create_issue_comment.call_count}"
        )

    def test_split_comments_contain_continuation_notice(self):
        pr = MagicMock()
        section = "B" * 30000
        body = f"{section}\n\n---\n\n{section}\n\n---\n\n{section}"
        _post_comment_safe(pr, body)
        calls = pr.create_issue_comment.call_args_list
        # First comment should have continuation notice
        assert "Continued in next comment" in calls[0][0][0]
        # Last comment should have the continuation header
        assert "continued" in calls[-1][0][0]


class TestDeleteOldBotComments(unittest.TestCase):
    """Tests for delete_old_bot_comments (issue + review comment cleanup)."""

    def test_deletes_matching_issue_comments(self):
        pr = MagicMock()
        bot_comment = FakeComment("github-actions[bot]", "## Security Review: blah\n*Powered by Claude*")
        other_comment = FakeComment("someuser", "LGTM")
        pr.get_issue_comments.return_value = [bot_comment, other_comment]
        pr.get_review_comments.return_value = []

        deleted = delete_old_bot_comments(pr)

        assert deleted == 1
        assert bot_comment.deleted
        assert not other_comment.deleted

    def test_deletes_legacy_inline_review_comments(self):
        pr = MagicMock()
        pr.get_issue_comments.return_value = []
        inline_comment = FakeComment("github-actions[bot]", "Finding: SQL injection\n*Powered by Claude*")
        pr.get_review_comments.return_value = [inline_comment]

        deleted = delete_old_bot_comments(pr)

        assert deleted == 1
        assert inline_comment.deleted

    def test_skips_non_bot_comments(self):
        pr = MagicMock()
        human_comment = FakeComment("developer", "Security Review looks good\nPowered by Claude")
        pr.get_issue_comments.return_value = [human_comment]
        pr.get_review_comments.return_value = []

        deleted = delete_old_bot_comments(pr)

        assert deleted == 0
        assert not human_comment.deleted

    def test_handles_review_comment_api_failure(self):
        pr = MagicMock()
        pr.get_issue_comments.return_value = []
        pr.get_review_comments.side_effect = Exception("API error")

        # Should not raise — just prints a warning
        deleted = delete_old_bot_comments(pr)
        assert deleted == 0

    def test_handles_delete_failure_gracefully(self):
        pr = MagicMock()
        bot_comment = FakeComment("github-actions[bot]", "## Security Review: x\n*Powered by Claude*")
        bot_comment.delete = MagicMock(side_effect=Exception("403 Forbidden"))
        pr.get_issue_comments.return_value = [bot_comment]
        pr.get_review_comments.return_value = []

        # Should not raise
        deleted = delete_old_bot_comments(pr)
        assert deleted == 0


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
        mock_post.side_effect = lambda pr, body: call_order.append("post")
        mock_delete.side_effect = lambda pr: (call_order.append("delete"), 0)[1]

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

        post_review_comments([])

        mock_post.assert_called_once()
        body = mock_post.call_args[0][1]
        assert "No High/Medium Issues Found" in body


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
        mock_pr.create_issue_comment.side_effect = lambda body: call_order.append("post")
        mock_delete.side_effect = lambda pr: (call_order.append("delete"), 0)[1]

        post_analysis_failed_comment()

        assert call_order == ["post", "delete"], f"Expected post before delete, got {call_order}"


class TestParseCloudeResponse(unittest.TestCase):
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
        import json
        result = parse_claude_response(json.dumps(issues))
        assert len(result) == 50


if __name__ == "__main__":
    unittest.main()
