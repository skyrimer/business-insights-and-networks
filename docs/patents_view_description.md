# Patents View
## Data Description
In the neighbouring file

## Filtering
 - CPC and IPC counts of over 20 are extreme and rare, but possible. Since we're given counts for just the 8 classes, we filter all rows with counts of more than 50 in either column.
 - application_number and application_year are core components, so we filter out the rows that don't have them.
## EDA Notes
- There are duplicates `patent_number`, since they are different in terms of `assignee`. So, it tracks who owned the patent rights at different times or indication of co-ownership.
- There are 68 uniques `state`s, that's why I don't convert them to american states.
- `assignee_sequence` is filled to be `-1` when the Nan value was present. This indicates that no information about the asignee is available.
- `wipo_field_ids` is the duplicate for the sector and ipc value combinations, so we remove it.