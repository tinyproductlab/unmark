from io import BytesIO
from pathlib import Path
import zipfile

import fitz
from PIL import Image

from server import export_pdf_as_webp


def test_export_clean_pdf_as_one_webp_per_page(tmp_path: Path) -> None:
    source = tmp_path / "clean.pdf"
    document = fitz.open()
    for color in ((1, 0, 0), (0, 0, 1)):
        page = document.new_page(width=160, height=100)
        page.draw_rect(page.rect, color=color, fill=color)
    document.save(source)
    document.close()

    archive = tmp_path / "clean_WebP图片包.zip"
    assert export_pdf_as_webp(source, archive) == 2

    with zipfile.ZipFile(archive) as bundle:
        names = bundle.namelist()
        assert names == ["clean_001.webp", "clean_002.webp"]
        with Image.open(BytesIO(bundle.read(names[0]))) as image:
            assert image.format == "WEBP"
            assert image.size == (240, 150)
