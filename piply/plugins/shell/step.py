import subprocess
from piply.core.step import Step


class ShellStep(Step):
    def _execute(self, context) -> dict:
        """
        Execute a shell command with proper error handling.

        Returns:
            Dictionary with stdout, stderr, and return code
        """
        cmd = self.config.get("command")
        if not cmd:
            raise ValueError(
                f"ShellStep '{self.name}' missing 'command' configuration")

        print(f"[ShellStep] Running: {cmd}")

        try:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=self.timeout
            )

            output = {
                "stdout": result.stdout,
                "stderr": result.stderr,
                "returncode": result.returncode
            }

            if result.returncode != 0:
                error_msg = f"Command failed with exit code {result.returncode}\nSTDERR: {result.stderr}"
                raise RuntimeError(error_msg)

            print(f"[ShellStep] Completed successfully")
            if result.stdout:
                print(
                    f"  Output: {result.stdout[:200]}{'...' if len(result.stdout) > 200 else ''}")

            return output

        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"Command timed out after {self.timeout} seconds")
        except Exception as e:
            raise RuntimeError(f"Error executing shell command: {e}")
