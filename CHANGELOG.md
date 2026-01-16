## 0.1.0
- initial commit
- stripped out generalisable functionality from omop-alchemy so that it could be reused in multiple clinical data models

## 0.1.1
- modified load_csv to split out file dedupe & db dedupe

## 0.1.2
- adding inference for delimiter and encoding on load

## 0.1.3
- minimal logging updates

## 0.2.0
- added merge functionality with staging table for upsert

## 0.2.1
- bugfix for bulk load

## 0.2.2
- bugfix for staging load on postgres

## 0.2.3
- consistent api for loader args