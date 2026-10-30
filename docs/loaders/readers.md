# Readers

A reader yields a file as Arrow record batches holding the model columns present in the file. Text formats yield raw strings, so the conversion policy sees exactly what the file contains. Typed formats yield their stored types.

`open_reader` picks the reader from the file suffix: `.parquet` is read as Parquet, any other suffix as delimited text. `load_file` opens the reader with it, taking `quote_mode` and `chunksize` as arguments, and `load_file(reader=...)` passes a configured reader explicitly.

| Reader | Input |
|------|------|
| `DelimitedTextReader` | Delimited text with a tab or comma delimiter and detected encoding |
| `ParquetReader` | Parquet files, in batches of `chunksize` rows |

---

## FileReader

A new format is a new `FileReader` subclass that implements `batches()` and sets `columns` and `empty`.

::: orm_loader.loaders.readers.base.FileReader

---

## DelimitedTextReader

Reads raw strings in 4 MB blocks, so memory does not grow with the row count. Header names are lowercased and stripped of quotes, padding and a trailing `_hash`, and file columns that are not model columns are ignored with a warning. Parquet column names are normalised the same way. An unquoted empty field is null and a quoted empty field is an empty string.

`quote_mode` selects how double quotes are read:

| Mode | Behaviour |
|------|------|
| `csv` | Double quotes quote fields |
| `literal` | Double quotes are data |
| `by_delimiter` (default) | `literal` for tab-delimited files, `csv` otherwise |

::: orm_loader.loaders.readers.delimited.DelimitedTextReader

---

## ParquetReader

::: orm_loader.loaders.readers.parquet.ParquetReader

---

## open_reader

::: orm_loader.loaders.readers.open_reader

---

## Detection

The delimited reader detects the encoding with `chardet` (ASCII is normalised to UTF-8, and only a small prefix of the file is inspected), the delimiter from the first line (tabs win over commas when more frequent).

::: orm_loader.loaders.readers.detection.infer_encoding
::: orm_loader.loaders.readers.detection.infer_delim
::: orm_loader.loaders.readers.detection.resolve_quote_mode
