
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
import os

SOURCE = Path(r"D:\EquipmentRecipes")
OUTPUT = Path(r"D:\RecipeZIP")
BATCH_SIZE = 1000

for equipment in sorted(SOURCE.iterdir()):
    if not equipment.is_dir():
        continue

    recipes = sorted(
        (p for p in equipment.iterdir() if p.is_dir()),
        key=lambda p: p.name
    )

    output_dir = OUTPUT / equipment.name
    output_dir.mkdir(parents=True, exist_ok=True)

    for start in range(0, len(recipes), BATCH_SIZE):
        batch = recipes[start:start + BATCH_SIZE]
        part = start // BATCH_SIZE + 1

        zip_path = output_dir / f"{equipment.name}_{part:03d}.zip"

        with ZipFile(
            zip_path,
            "w",
            compression=ZIP_DEFLATED,
            compresslevel=1,
            allowZip64=True
        ) as zf:
            for recipe in batch:
                for root, dirs, files in os.walk(recipe):
                    root_path = Path(root)
                    relative = root_path.relative_to(SOURCE)

                    zf.write(root_path, relative.as_posix() + "/")

                    for filename in files:
                        path = root_path / filename
                        zf.write(
                            path,
                            path.relative_to(SOURCE).as_posix()
                        )

        print(f"[완료] {zip_path.name} : {len(batch)}개 Recipe")
