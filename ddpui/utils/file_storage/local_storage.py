"""Local filesystem storage adapter"""

import os
import shutil
import glob
from typing import List, Optional
from pathlib import Path

from ddpui.utils.file_storage.storage_interface import StorageInterface


class LocalStorageAdapter(StorageInterface):
    """Local filesystem storage implementation"""

    @property
    def is_remote(self) -> bool:
        """Local storage is not remote"""
        return False

    def read_file(self, file_path: str) -> str:
        """Read content from a local file"""
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                return file.read()
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {file_path}")
        except Exception as e:
            raise IOError(f"Failed to read file {file_path}: {str(e)}")

    def write_file(self, file_path: str, content: str) -> None:
        """Write content to a local file"""
        try:
            # Create parent directories if they don't exist
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(content)
        except Exception as e:
            raise IOError(f"Failed to write file {file_path}: {str(e)}")

    def delete_file(self, file_path: str) -> None:
        """Delete a local file"""
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
            else:
                raise FileNotFoundError(f"File not found: {file_path}")
        except FileNotFoundError:
            raise
        except Exception as e:
            raise IOError(f"Failed to delete file {file_path}: {str(e)}")

    def exists(self, file_path: str) -> bool:
        """Check if a local file or directory exists"""
        return os.path.exists(file_path)

    def list_files(self, directory_path: str, pattern: Optional[str] = None) -> List[str]:
        """List files in a local directory"""
        try:
            if not os.path.exists(directory_path):
                raise FileNotFoundError(f"Directory not found: {directory_path}")

            if pattern:
                search_pattern = os.path.join(directory_path, pattern)
                return glob.glob(search_pattern)
            else:
                return [
                    os.path.join(directory_path, f)
                    for f in os.listdir(directory_path)
                    if os.path.isfile(os.path.join(directory_path, f))
                ]
        except FileNotFoundError:
            raise
        except Exception as e:
            raise IOError(f"Failed to list files in {directory_path}: {str(e)}")

    def create_directory(self, directory_path: str) -> None:
        """Create a local directory"""
        try:
            os.makedirs(directory_path, exist_ok=True)
        except Exception as e:
            raise IOError(f"Failed to create directory {directory_path}: {str(e)}")

    def copy_file(self, source_path: str, destination_path: str) -> None:
        """Copy a local file"""
        try:
            if not os.path.exists(source_path):
                raise FileNotFoundError(f"Source file not found: {source_path}")

            # Create parent directories if they don't exist
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            shutil.copy2(source_path, destination_path)
        except FileNotFoundError:
            raise
        except Exception as e:
            raise IOError(f"Failed to copy file from {source_path} to {destination_path}: {str(e)}")

    def copy_tree(self, source_directory: str, destination_directory: str) -> None:
        """Copy entire local directory tree"""
        try:
            if not os.path.exists(source_directory):
                raise FileNotFoundError(f"Source directory not found: {source_directory}")

            if os.path.exists(destination_directory):
                shutil.rmtree(destination_directory)

            shutil.copytree(source_directory, destination_directory)
        except FileNotFoundError:
            raise
        except Exception as e:
            raise IOError(
                f"Failed to copy tree from {source_directory} to {destination_directory}: {str(e)}"
            )

    def download_tree(self, remote_path: str, local_path: str) -> None:
        """Download not applicable for local storage"""
        raise NotImplementedError("download_tree is not applicable for local storage")
