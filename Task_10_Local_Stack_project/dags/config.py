from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    file_dir = Path(
        "/home/lifeavg/Documents/projects/de/Task_10_Local_Stack_project/data"
    )
    file_name = Path("lhlogo1.jpg")
    bucket = "helsinki-city-bikes"

    @property
    def file(self) -> Path:
        return self.file_dir / self.file_name
