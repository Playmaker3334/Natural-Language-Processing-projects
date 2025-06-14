# ---------- dataset_stream_win.py ----------
# pip install datasets tqdm

import os, itertools
from datasets import load_dataset
from tqdm import tqdm

# 1️⃣  Ruta de caché corta
os.environ["HF_HOME"]            = r"C:/hf_cache"
os.environ["HF_HUB_CACHE"]       = r"C:/hf_cache/hub"
os.environ["HF_DATASETS_CACHE"]  = r"C:/hf_cache/datasets"

# 2️⃣  Cargar en streaming (solo 'train')
print("📥 Descargando openwebtext en modo streaming…")
ds_stream = load_dataset(
    "openwebtext",
    split="train",           # ← no slice aquí
    trust_remote_code=True,
    streaming=True
)

# 3️⃣  Elegir cuántos docs ≈ 3 GB (ajusta si hace falta)
DOCS_TO_TAKE = 2_000_000      # ~3 GB de texto crudo

# 4️⃣  Guardar con barra de progreso
out_file = "openwebtext_3GB.txt"
with open(out_file, "w", encoding="utf-8") as f:
    for doc in tqdm(itertools.islice(ds_stream, DOCS_TO_TAKE),
                    total=DOCS_TO_TAKE,
                    desc="📤 Guardando texto"):
        f.write(doc["text"].replace("\n", " ").strip() + "\n")

print(f"\n🎉 Listo → {os.path.abspath(out_file)} "
      f"({os.path.getsize(out_file)/(1024**3):.2f} GB)")
# ---------- fin ----------


