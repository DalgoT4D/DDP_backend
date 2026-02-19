"""Abstract interface for file storage operations"""

from abc import ABC, abstractmethod
from typing import List, Optional
from pathlib import Path


class StorageInterface(ABC):
    """Abstract base class for file storage operations"""

    @abstractmethod
    def read_file(self, file_path: str) -> str:
        """
        Read content from a file

        Args:
            file_path: Path to the file

        Returns:
            File content as string

        Raises:
            FileNotFoundError: If file doesn't exist
            IOError: If read operation fails
        """
        pass

    @abstractmethod
    def write_file(self, file_path: str, content: str) -> None:
        """
        Write content to a file

        Args:
            file_path: Path to the file
            content: Content to write

        Raises:
            IOError: If write operation fails
        """
        pass

    @abstractmethod
    def delete_file(self, file_path: str) -> None:
        """
        Delete a file

        Args:
            file_path: Path to the file

        Raises:
            FileNotFoundError: If file doesn't exist
            IOError: If delete operation fails
        """
        pass

    @abstractmethod
    def exists(self, file_path: str) -> bool:
        """
        Check if a file or directory exists

        Args:
            file_path: Path to check

        Returns:
            True if exists, False otherwise
        """
        pass

    @abstractmethod
    def list_files(self, directory_path: str, pattern: Optional[str] = None) -> List[str]:
        """
        List files in a directory

        Args:
            directory_path: Path to the directory
            pattern: Optional glob pattern to filter files

        Returns:
            List of file paths

        Raises:
            FileNotFoundError: If directory doesn't exist
        """
        pass

    @abstractmethod
    def create_directory(self, directory_path: str) -> None:
        """
        Create a directory (including parent directories)

        Args:
            directory_path: Path to the directory

        Raises:
            IOError: If create operation fails
        """
        pass

    @abstractmethod
    def copy_file(self, source_path: str, destination_path: str) -> None:
        """
        Copy a file from source to destination

        Args:
            source_path: Source file path
            destination_path: Destination file path

        Raises:
            FileNotFoundError: If source file doesn't exist
            IOError: If copy operation fails
        """
        pass

    @abstractmethod
    def copy_tree(self, source_directory: str, destination_directory: str) -> None:
        """
        Copy entire directory tree from source to destination

        Args:
            source_directory: Source directory path
            destination_directory: Destination directory path

        Raises:
            FileNotFoundError: If source directory doesn't exist
            IOError: If copy operation fails
        """
        pass
