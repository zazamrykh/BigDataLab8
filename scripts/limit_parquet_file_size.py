#!/usr/bin/env python3
import sys
import pyarrow as pa
import pyarrow.parquet as pq

in_path = sys.argv[1] if len(sys.argv) > 1 else "./data/food.parquet"
out_path = sys.argv[2] if len(sys.argv) > 2 else "./data/food_50k.parquet"
limit = int(sys.argv[3]) if len(sys.argv) > 3 else 50_000

pf = pq.ParquetFile(in_path)  # не загружает всё сразу
batches = []
read = 0

for rg_idx in range(pf.num_row_groups):
    if read >= limit:
        break
    # читаем один row group (не весь файл)
    table = pf.read_row_group(rg_idx)
    need = limit - read
    if table.num_rows > need:
        table = table.slice(0, need)  # обрежем до нужного количества
    batches.append(table)
    read += table.num_rows

if not batches:
    # Пустой или нет строк — создадим пустую таблицу на основе схемы файла
    schema = pf.schema_arrow
    table = pa.Table.from_arrays([pa.array([], type=f.type) for f in schema], schema=schema)
else:
    table = pa.concat_tables(batches, promote=True)

# Запись компактно и быстро (можно 'snappy' вместо 'zstd' для совместимости)
pq.write_table(table, out_path, compression="zstd", write_statistics=True)
print(f"Wrote {read} rows to {out_path}")
