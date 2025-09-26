from pathlib import Path


class ConfigCommand:
    @staticmethod
    def add_arguments(parser):
        pass

    @staticmethod
    def execute(args, config_path):
        print(f"Configuration file: {config_path}")

        if config_path.exists():
            print(f"Status: Found")
            with open(config_path, 'r') as f:
                content = f.read().strip()
                if content:
                    print("Contents:")
                    print(content)
                else:
                    print("Contents: (empty)")
        else:
            print("Status: Not found")
            print("Create a config.toml file with your settings.")