"""Testting file paths.py"""

import unittest
from pathlib import Path
from src.common.utils.paths import Paths

class PathsTestCase(unittest.TestCase):
    """Class for testing class Paths"""
    def test_get_project_root_success(self):
        """Test function get_project_root success"""
        expected = Path(__file__).parent.parent.parent.parent.parent.absolute()
        self.assertEqual(Paths.get_project_root(), expected)

    def test_get_project_root_fails(self):
        """Test function get_project_root fails"""
        expected = Path(__file__).parent.parent.parent.parent.absolute()
        self.assertNotEqual(Paths.get_project_root(), expected)

    def test_get_file_path_success(self):
        """Test function get_file_path success"""
        expected = Paths.get_project_root() / 'config'
        result = Paths.get_file_path('config')
        self.assertEqual(result, expected)

    def test_get_file_path_fails(self):
        """Test function get_file_path fails"""
        expected = Paths.get_project_root()
        result = Paths.get_file_path('config')
        self.assertNotEqual(result, expected)
        # test file_path not found
        with self.assertRaises(FileNotFoundError):
            Paths.get_file_path('GorGroBalifosngholax')

if __name__ == '__main__':
    unittest.main()
