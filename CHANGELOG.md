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

## 0.2.4
- changed date parse for non-onco-branch vocab files

## 0.3.0
- significant updates to handle loaders as class objects

## 0.3.1
- typo

## 0.3.2
- bugfix: branching stage-load logic for sqlite 

## 0.3.3
- moved data handling converters

## 0.3.4
- bugfix: string cast had typing wrong

## 0.3.5
- added mkdocs and docstrings

## 0.3.6
- added pytest & actions for linting and tests

## 0.3.7
- moved data type handling

## 0.3.8
- sqlite staging table creation now respects date types

## 0.3.9
- bugfix

## 0.3.10
- adding in mat views

# 0.3.11
- convert NaN to None for proper NULL insertion