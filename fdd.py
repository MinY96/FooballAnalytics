
from pathlib import Path
from zipfile import ZipFile
import time

# 장비 폴더들이 모여 있는 최상위 경로
ROOT_DIR = Path(r"D:\RecipeData")


def extract_recipes():
    start_time = time.perf_counter()

    total_success = 0
    total_failed = 0

    for equipment_dir in sorted(ROOT_DIR.iterdir()):
        if not equipment_dir.is_dir():
            continue

        zip_files = sorted(equipment_dir.glob("*.zip"))

        if not zip_files:
            continue

        print(f"\n[장비] {equipment_dir.name}")
        print(f"ZIP 파일 수: {len(zip_files)}")

        for idx, zip_path in enumerate(zip_files, 1):
            try:
                with ZipFile(zip_path, "r") as zf:
                    members = zf.infolist()

                    # ZIP 내부 경로 검증
                    for member in members:
                        name = member.filename
                        parts = name.split("/")

                        if (
                            parts[0] != equipment_dir.name
                            or ".." in parts
                            or name.startswith("/")
                            or "\\" in name
                        ):
                            raise ValueError(
                                f"잘못된 내부 경로: {name}"
                            )

                    # 장비 폴더의 상위 경로에 압축 해제
                    zf.extractall(equipment_dir.parent)

                # 압축 해제 성공 후 ZIP 삭제
                zip_path.unlink()

                total_success += 1

                print(
                    f"[{idx}/{len(zip_files)}] "
                    f"{zip_path.name} 완료 / ZIP 삭제"
                )

            except Exception as e:
                total_failed += 1
                print(f"[오류] {zip_path.name}: {e}")
                # 실패한 ZIP 파일은 삭제하지 않음

    elapsed = time.perf_counter() - start_time

    print("\n========== 작업 완료 ==========")
    print(f"성공: {total_success}개")
    print(f"실패: {total_failed}개")
    print(f"소요 시간: {elapsed:.1f}초")


if __name__ == "__main__":
    extract_recipes()
