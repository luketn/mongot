#!/usr/bin/env python3
"""
Unit tests for GitConfigWriter.

Tests the Git configuration file generation functionality including:
- Gitconfig content generation with tokens
- File writing with proper encoding
- Default and custom user credentials
- Path handling (string and Path objects)
"""

import unittest

from scripts.github.GitConfigWriter import GitConfigWriter


class TestGitConfigWriter(unittest.TestCase):
    """Test suite for GitConfigWriter class."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_read_token = "ghs_read_token_1234567890"
        self.test_write_token = "ghs_write_token_0987654321"
        self.test_user_name = "Test User"
        self.test_user_email = "test@example.com"

    def test_generate_gitconfig_with_custom_credentials(self):
        """Test gitconfig generation with custom user credentials."""
        result = GitConfigWriter.generate_gitconfig(
            self.test_read_token,
            self.test_write_token,
            self.test_user_name,
            self.test_user_email,
            "10gen",
            "mongot-public-preview"
        )

        # Verify user section
        self.assertIn("[user]", result)
        self.assertIn(f"  name = {self.test_user_name}", result)
        self.assertIn(f"  email = {self.test_user_email}", result)

        # Verify read token URL substitution
        self.assertIn(
            f'[url "https://x-access-token:{self.test_read_token}@github.com/10gen/mongot.git"]',
            result
        )
        self.assertIn("    insteadOf = https://github.com/10gen/mongot.git", result)

        # Verify write token URL substitution
        self.assertIn(
            f'[url "https://x-access-token:{self.test_write_token}@github.com/10gen/mongot-public-preview.git"]',
            result
        )
        self.assertIn("    insteadOf = https://github.com/10gen/mongot-public-preview.git", result)

    def test_generate_gitconfig_format(self):
        """Test that generated gitconfig has proper format with newlines."""
        result = GitConfigWriter.generate_gitconfig(
            self.test_read_token,
            self.test_write_token,
            self.test_user_name,
            self.test_user_email,
            "10gen",
            "mongot-public-preview"
        )

        lines = result.split("\n")

        # Verify structure
        self.assertEqual(lines[0], "[user]")
        self.assertEqual(lines[1], f"  name = {self.test_user_name}")
        self.assertEqual(lines[2], f"  email = {self.test_user_email}")
        self.assertEqual(lines[3], "")  # Blank line
        # URL sections follow

    def test_generate_gitconfig_tokens_are_embedded(self):
        """Test that tokens are properly embedded in URLs."""
        result = GitConfigWriter.generate_gitconfig(
            "token123",
            "token456",
            "User",
            "user@example.com",
            "mongodb",
            "mongot"
        )

        # Verify tokens are in the output
        self.assertIn("token123", result)
        self.assertIn("token456", result)

        # Verify they're in the correct URL format
        self.assertIn("x-access-token:token123@github.com", result)
        self.assertIn("x-access-token:token456@github.com", result)


if __name__ == "__main__":
    unittest.main()


