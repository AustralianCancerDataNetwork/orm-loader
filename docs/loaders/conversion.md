# Conversion

`orm_loader.conversion` interprets raw values for every file load, whatever the file format.

```mermaid
flowchart LR
    A[Arrow column] --> B[Policy picks the column's handler]
    B --> C[clean]
    C --> D[detect missing]
    D --> E[parse]
    E --> F[check]
    F --> G[values and invalid mask]
```

---

## ConversionPolicy

`ConversionPolicy` is a frozen settings object passed to `load_file(policy=...)`. It holds the settings shared by every column and one handler for each kind of column type.

| Field | Meaning |
|------|------|
| `nulls` | `NullPolicy`: what counts as a missing value |
| `errors` | `ErrorPolicy`: what happens to unreadable and missing required values |
| `text`, `integer`, `floating`, `numeric`, `boolean`, `date`, `datetime` | The handler for that kind of column, each with its own settings |
| `handlers` | Handlers for further column types, or replacements, by SQLAlchemy type class |
| `columns` | Handlers for single columns by `(table_name, column_name)` |

```python
policy = ConversionPolicy(
    nulls=NullPolicy(text_markers=frozenset({"NULL"})),
    errors=ErrorPolicy(on_error="reject_row", max_rejects=0.01),
    date=DateHandler(formats=("dd/mm/yyyy",)),
    columns={("concept", "concept_code"): TextHandler(strip=False)},
)
```

::: orm_loader.conversion.ConversionPolicy

---

## NullPolicy

- Text columns keep every nonempty value verbatim. Only empty and whitespace-only text is null.
- `text_markers` adds exact, case-sensitive words that are null in every text column of the load. To treat a word as missing in some columns only, subclass `TextHandler`, override `missing()` and register it in `ConversionPolicy.columns`.
- `marker_quoting="unquoted_only"` matches a marker only when the source did not quote it.
- Typed columns accept a valid value, a missing word (`null`, `none`, `na`, `n/a`, `nan` and similar) or a real null, and fail otherwise.

::: orm_loader.conversion.NullPolicy

---

## ErrorPolicy

`on_error` is `raise` (default) or `reject_row`. `max_rejects` stops a `reject_row` load once rejected rows pass a count or a fraction of the rows read.

::: orm_loader.conversion.ErrorPolicy

---

## Handlers

The handler for a column is the first match of: the entry in `ConversionPolicy.columns` and the handler for the most specific class of the column type. A `TypeDecorator` is handled as the type it stores. A column type with no handler is an error that names the table, column and type, raised before any row is processed. Loading such a column unchanged is an explicit choice: `columns={("table", "column"): PassthroughHandler()}`.

| Handler | Column types | Accepted text | Settings |
|------|------|------|------|
| `TextHandler` | `String`, `Text` | Any nonempty text; longer than the column is an error | `strip` |
| `EnumHandler` | `Enum` | One of the labels the column stores | `strip` |
| `IntegerHandler` | `Integer`, `SmallInteger`, `BigInteger` | Optional sign and digits, with an optional all-zero fraction (`1.0`); a value outside the 16, 32 or 64 bits of the column type is an error | none |
| `FloatHandler` | `Float` | Decimal or exponent form; `Infinity` and values that overflow to infinity are errors | none |
| `NumericHandler` | `Numeric` | Decimal form, converted to the declared precision and scale without rounding; a precision without a scale has scale 0 and a column without a precision reads as `decimal128(38, 18)` | none |
| `BooleanHandler` | `Boolean` | `true`, `t`, `yes`, `y`, `1`, `on` and their opposites | `true_words`, `false_words` |
| `DateHandler` | `Date` | One of `formats`, validated against the calendar; year 0 is an error | `formats` |
| `DateTimeHandler` | `DateTime` | ISO date and time, or a date in one of `formats`; see time zones below | `formats` |

Dates default to `yyyymmdd`, `dd-mon-yyyy` and `yyyy-mm-dd`. Ambiguous forms such as `02/01/2020` are never guessed; declare `DateHandler(formats=("dd/mm/yyyy",))` or `("mm/dd/yyyy",)`. A timestamp column reads date-only values with its own `formats`.

### Time zones

The column type decides, so no value is converted by a convention the schema did not ask for.

- A `DateTime(timezone=True)` column requires an offset (`Z` or `+hh:mm`) and stores UTC. A value without an offset and a date-only value are errors.
- A `DateTime` column without a time zone rejects a value with an offset.

### Typed input

Input that already has a type, such as a Parquet column, is cast to the column type and checked like parsed text. A value that does not come back unchanged when cast back to its input type is an error, so an integer `2` in a `Boolean` column, a timestamp with a time in a `Date` column and a decimal that loses digits are all rejected. Float `NaN` is missing.

### Adding a column type

Subclass `ConvertingHandler`, implement `arrow_type` and `parse`, and pass the handler in `ConversionPolicy.handlers` for the type class, or in `ConversionPolicy.columns` for a single column. `clean`, `missing` and `check` can be overridden.

```python
class HexHandler(ConvertingHandler):
    def arrow_type(self, sa_type):
        return pa.int64()

    def parse(self, cleaned, sa_type):
        ...  # return Interpretation(values, invalid)

policy = ConversionPolicy(handlers={MyHexType: HexHandler()})
```

### ConvertingHandler

::: orm_loader.conversion.ConvertingHandler

### ColumnHandler

::: orm_loader.conversion.ColumnHandler
