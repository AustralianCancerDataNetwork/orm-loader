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
    columns={("concept", "concept_name"): TextHandler(markers=frozenset({"N/A"}))},
)
```

::: orm_loader.conversion.ConversionPolicy

---

## NullPolicy

- Text columns keep every nonempty value verbatim. Only empty and whitespace-only text is null.
- `text_markers` adds exact, case-sensitive words that are null in text columns. `TextHandler(markers=...)` replaces them for the columns it serves.
- `marker_quoting="unquoted_only"` matches a marker only when the source did not quote it.
- Typed columns accept a valid value, a missing word (`null`, `none`, `na`, `n/a`, `nan` and similar) or a real null, and fail otherwise.

::: orm_loader.conversion.NullPolicy

---

## ErrorPolicy

`on_error` is `raise` (default) or `reject_row`. `max_rejects` stops a `reject_row` load once rejected rows pass a count or a fraction of the rows read.

::: orm_loader.conversion.ErrorPolicy

---

## Handlers

The handler for a column is the first match of: the entry in `ConversionPolicy.columns` and the handler for the most specific class of the column type. A column type with no handler is passed through unchanged.

| Handler | Column types | Accepted text | Settings |
|------|------|------|------|
| `TextHandler` | `String`, `Text`, `Enum` | Any nonempty text; longer than the column is an error | `strip`, `markers` |
| `IntegerHandler` | `Integer` | Optional sign and digits | none |
| `FloatHandler` | `Float` | Decimal or exponent form, no `Infinity` | none |
| `NumericHandler` | `Numeric` | Decimal form, converted to the declared precision and scale without rounding | none |
| `BooleanHandler` | `Boolean` | `true`, `t`, `yes`, `y`, `1`, `on` and their opposites | `true_words`, `false_words` |
| `DateHandler` | `Date` | One of `formats`, validated against the calendar | `formats`, `parser` |
| `DateTimeHandler` | `DateTime` | ISO date and time, or a date in one of `formats` | `formats`, `parser` |

Dates default to `yyyymmdd`, `dd-mon-yyyy` and `yyyy-mm-dd`. Ambiguous forms such as `02/01/2020` are never guessed; declare `DateHandler(formats=("dd/mm/yyyy",))` or `("mm/dd/yyyy",)`. A timestamp column reads date-only values with its own `formats`.

`DateHandler(parser=...)` replaces the date parser with a function that has the signature of `parse_dates`.

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
