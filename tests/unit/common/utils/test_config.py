import os
import unittest
from unittest.mock import patch
import yaml
from src.common.utils.config import Config

class ConfigTestCase(unittest.TestCase):
    """Unit test for config file"""

    def setUp(self) -> None:
        self.pathfile = 'tests/unit/common/utils/testformat.yml'

    def test_get_config_not_found_fails(self):
        """Test get_config case one (Not found)"""
        with self.assertRaises(FileNotFoundError):
            Config.get_config('fakepath.yml')

    def test_get_config_found_success(self):
        """Test get_config case two (Found)"""
        yaml_file = {
            "value1": 1,
            "value2": 2
        }
        with patch.object(Config, 'get_config', return_value=yaml_file):
            config = Config.get_config('rightpath.yml')
            self.assertEqual(config, yaml_file)

    def test_get_config_erroryml_fails(self):
        """Test get_config case three (YAML error)"""
        yml_error_format = """
            invalid_yaml: {
        """

        with open(self.pathfile, 'w', encoding='utf-8') as file_obj:
            file_obj.write(yml_error_format)

        with self.assertRaises(yaml.YAMLError):
            Config.get_config(self.pathfile)

    #@patch('src.common.utils.paths.Paths.get_file_path')
    @patch('src.common.utils.config.Config.get_config')
    def test_get_config_yml_success(self, mock_get_config):
        """Test all cases in functions get_config_yml"""
        mock_config_key = 'section_key'
        mock_config_dict = {'section_key': {'option1': 'value1', 'option2': 'value2'}}
        #mock_get_file_path.return_value = self.pathfile
        mock_get_config.return_value = mock_config_dict
        result = Config.get_config_yml(mock_config_key)
        self.assertEqual(result, mock_config_dict[mock_config_key])
        result = Config.get_config_yml()
        self.assertEqual(result, mock_config_dict)

    def tearDown(self) -> None:
        try:
            os.remove(self.pathfile)
        except OSError:
            pass

if __name__ == '__main__':
    unittest.main()
