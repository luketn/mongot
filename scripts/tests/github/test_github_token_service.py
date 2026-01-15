#!/usr/bin/env python3
"""
Unit tests for GithubTokenService.

Tests the GitHub token service functionality including:
- Successful token retrieval
- Error handling for invalid credentials
- Error handling for invalid installation IDs
"""

import unittest
from unittest.mock import Mock, patch

from scripts.github.GithubTokenService import GithubTokenService


class TestGithubTokenService(unittest.TestCase):
    """Test suite for GithubTokenService class."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_app_id = 12345
        self.test_private_key = "-----BEGIN RSA PRIVATE KEY-----\ntest_key\n-----END RSA PRIVATE KEY-----"
        self.test_installation_id = 67890
        self.test_token = "ghs_test_token_1234567890"

    @patch('scripts.github.GithubTokenService.GithubIntegration')
    def test_init_creates_integration(self, mock_github_integration):
        """Test that __init__ creates a GithubIntegration instance."""
        service = GithubTokenService(self.test_app_id, self.test_private_key)

        mock_github_integration.assert_called_once_with(
            self.test_app_id,
            self.test_private_key
        )
        self.assertEqual(service.integration, mock_github_integration.return_value)

    @patch('scripts.github.GithubTokenService.GithubIntegration')
    def test_get_installation_token_success(self, mock_github_integration):
        """Test successful token retrieval."""
        # Setup mock
        mock_integration = Mock()
        mock_auth = Mock()
        mock_auth.token = self.test_token
        mock_integration.get_access_token.return_value = mock_auth
        mock_github_integration.return_value = mock_integration

        # Execute
        service = GithubTokenService(self.test_app_id, self.test_private_key)
        token = service.get_installation_token(self.test_installation_id)

        # Verify
        mock_integration.get_access_token.assert_called_once_with(self.test_installation_id)
        self.assertEqual(token, self.test_token)

    @patch('scripts.github.GithubTokenService.GithubIntegration')
    def test_get_installation_token_returns_none_on_failure(self, mock_github_integration):
        """Test that None is returned when token retrieval fails."""
        # Setup mock to return None
        mock_integration = Mock()
        mock_integration.get_access_token.return_value = None
        mock_github_integration.return_value = mock_integration

        # Execute
        service = GithubTokenService(self.test_app_id, self.test_private_key)
        token = service.get_installation_token(self.test_installation_id)

        # Verify
        mock_integration.get_access_token.assert_called_once_with(self.test_installation_id)
        self.assertIsNone(token)

    @patch('scripts.github.GithubTokenService.GithubIntegration')
    @patch('builtins.print')
    def test_get_installation_token_prints_error_on_failure(self, mock_print, mock_github_integration):
        """Test that error message is printed when token retrieval fails."""
        # Setup mock to return None
        mock_integration = Mock()
        mock_integration.get_access_token.return_value = None
        mock_github_integration.return_value = mock_integration

        # Execute
        service = GithubTokenService(self.test_app_id, self.test_private_key)
        service.get_installation_token(self.test_installation_id)

        # Verify error message was printed with installation_id
        mock_print.assert_called_once_with(f"Failed to get installation token for installation_id: {self.test_installation_id}")

    @patch('scripts.github.GithubTokenService.GithubIntegration')
    def test_get_installation_token_with_different_installation_ids(self, mock_github_integration):
        """Test token retrieval with multiple different installation IDs."""
        # Setup mock
        mock_integration = Mock()

        def get_token_side_effect(installation_id):
            mock_auth = Mock()
            mock_auth.token = f"token_for_{installation_id}"
            return mock_auth

        mock_integration.get_access_token.side_effect = get_token_side_effect
        mock_github_integration.return_value = mock_integration

        # Execute
        service = GithubTokenService(self.test_app_id, self.test_private_key)
        token1 = service.get_installation_token(111)
        token2 = service.get_installation_token(222)

        # Verify
        self.assertEqual(token1, "token_for_111")
        self.assertEqual(token2, "token_for_222")
        self.assertEqual(mock_integration.get_access_token.call_count, 2)

    @patch('scripts.github.GithubTokenService.GithubIntegration')
    def test_multiple_service_instances(self, mock_github_integration):
        """Test that multiple service instances can be created independently."""
        # Create two services with different credentials
        service1 = GithubTokenService(111, "key1")
        service2 = GithubTokenService(222, "key2")

        # Verify both were initialized correctly
        self.assertEqual(mock_github_integration.call_count, 2)
        mock_github_integration.assert_any_call(111, "key1")
        mock_github_integration.assert_any_call(222, "key2")


if __name__ == "__main__":
    unittest.main()

