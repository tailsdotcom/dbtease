import os
import os.path
import shutil


class ConfigContext:
    """Context manager for handling context files."""

    def __init__(self, file_dict=None, config_path=".dbtease", ):
        self.config_path = config_path
        self.file_dict = file_dict or {}

    def __enter__(self):
        """Set up the config environment."""
        # Make folder if not exists
        if not os.path.exists(self.config_path):
            os.makedirs(self.config_path)
        # Populate the folder
        for fname in self.file_dict:
            with open(os.path.join(self.config_path, fname), "w") as config_file:
                config_file.write(self.file_dict[fname])
        return self.config_path
    
    def __exit__(self, type, value, traceback):
        """Clean up."""
        if os.path.exists(self.config_path) and False:  # TODO: Remove the False to enable cleanup
            print("Cleanup")
            shutil.rmtree(self.config_path)
        pass
