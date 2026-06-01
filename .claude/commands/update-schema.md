Starting schema change workflow.

---

## Navigation (available at every Proceed prompt)

At any `Proceed? (y/n/back/all back/or describe what you want)` prompt:
- `y`        → apply the change and move to the next step
- `n`        → revise and re-present the same step (no files changed)
- `back`     → revert the previous step's file changes (`git checkout -- <files changed in that step>`) and return to that step
- `all back` → abort the entire workflow: revert all changes, delete the working branch, and return to `dev`
  - Run: `git checkout dev && git branch -D {branch_name}`
  - Display: `⚠️ Workflow aborted. All changes reverted. Back on dev.`
- **Any other text** → treat as a custom request for this step. Handle it, then re-present the step result for y/n confirmation. Record the request and outcome in the session log (see Post-Workflow section).

Always show `(y/n/back/all back/or describe what you want)` — not just `(y/n)`.

## Session Log (internal — maintained throughout the workflow)

Track every custom input made during the session:
```
[STEP {N}] Custom request: "{what the user asked}"
           Action taken  : "{what was done}"
           Outcome       : success / revised / skipped
```
This log is used in the Post-Workflow improvement step.

---

## Phase 1: Confirm Change Details

Gather all information before touching any files. Everything decided here drives Phase 2.

### 1-1. Confirm Operation

Arguments: $ARGUMENTS

Regardless of whether arguments are provided, always display the following first:

```
What kind of change is this?
  1. Add a column    → -add {column_name} {DataType}      e.g. -add video_category StringType
  2. Delete a column → -delete {column_name}               e.g. -delete video_duration_minutes
  3. Rename a column → --modify {old_name} {new_name}      e.g. --modify video_views views_count
  4. Both add & delete → -add ... -delete ...

Enter your change:
```

If arguments are provided, display them and confirm. Otherwise wait for input.

---

### 1-2. Existence check (before any other step)

Read `SOURCE_SCHEMA` from `src/glue/glue_job.py` and extract the current column list.

**For `-add`:** Check whether the column name already exists.
- If it exists → show error and return to 1-1:
  ```
  ❌ Column '{column_name}' already exists in the current schema.
     Current columns: {list}
     Please enter a different column name.
  ```
- If it does not exist → proceed.

**For `-delete` and `--modify`:** Check whether the target column actually exists.
- If it does not exist → show error and return to 1-1:
  ```
  ❌ Column '{column_name}' not found in the current schema.
     Current columns: {list}
  ```
- If it exists → proceed.

---

### 1-3. Confirm Insert Position (add operations only)

Read `SOURCE_SCHEMA` from `src/glue/glue_job.py` and display **source columns only**, numbered.
Do NOT show metadata columns added by the Glue Job (`processed_at`, `glue_job_run_id`, `ver_date`, etc.).

```
📌 Insert Position
  Current column order (from src/glue/glue_job.py SOURCE_SCHEMA — metadata columns excluded):
    1. video_title
    2. views
    ... (all source columns)

  Where should {column_name} be inserted?
  → Enter a number (e.g. 3 → insert after the 3rd column)
  → Type l or +last to append at the end
```

---

### 1-4. Final Confirmation

Display a summary of everything confirmed above. Ask for approval once before executing anything:

```
📋 Change Summary
  ─────────────────────────────
  Operation : add / delete / rename / both
  Add       : {column_name} ({DataType})          ← if adding
  Delete    : {column_name}                        ← if deleting
  Rename    : {old_name} → {new_name}              ← if renaming
  Position  : after {preceding_column}             ← if adding (or "end" if last)
  ─────────────────────────────
  Files to modify ({N} total):
    - terraform/modules/aws/glue_table/main.tf   … Glue Catalog table definition (referenced by Athena)
    - test_data/sns_advertisement_yyyymmdd.csv   … E2E test input CSV template
    - test_data/expected_output.csv              … E2E expected output (compared against Athena results)
    - src/glue/schema.py                         … Schema definition (SOURCE_SCHEMA / SOURCE_COLUMNS — single source of truth)
    - tests/test_glue_job.py                     … Schema structure tests (column presence, order, count)
    - tests/test_data_transform.py               … Data value tests (sample_df fixture tuples need manual update)
    - .github/workflows/e2e_test.yml             … E2E workflow definition (conditional)

Proceed with these changes? (y/n/back/all back)
```

If y → move to Phase 2. If n → return to 1-1.

---

## Phase 2: Execute (one step at a time)

**Show the diff for each step and ask `Proceed? (y/n/back/all back)` before making any change.**
Only `y` moves forward. Track which files were changed at each step to enable rollback.

---

### STEP 1/8: Create working branch

Branch naming rules:
- Add only    → `feature/{YYYYMMDD}/add_{column_name}`
- Delete only → `feature/{YYYYMMDD}/remove_{column_name}`
- Rename only → `feature/{YYYYMMDD}/rename_{old_name}_to_{new_name}`
- Both        → `feature/{YYYYMMDD}/schema_update`

Check current branch with `git branch --show-current`. If not on `dev`, switch first.

```
[STEP 1/8] Create branch
  Current branch : {current_branch}
  → {if not dev: "switching to dev first" / if dev: "creating from here"}
  New branch     : feature/{YYYYMMDD}/{operation}_{column_name}
Proceed? (y/n/back/all back)
```

`back` at STEP 1 → no files have been changed yet; return to Phase 1 final confirmation.

---

### STEP 2/8: terraform/modules/aws/glue_table/main.tf

```
[STEP 2/8] terraform/modules/aws/glue_table/main.tf
  Operation : {describe position with neighboring column names}
  Diff      :
    {columns block to add, remove, or rename}
Proceed? (y/n/back/all back)
```

`back` → `git checkout -- terraform/modules/aws/glue_table/main.tf` and return to STEP 1.

DataType → Terraform type:
StringType → string / LongType · IntegerType → bigint / DoubleType → double / BooleanType → boolean / DateType → date

**For `--modify`:** update the `name` field of the matching `columns` block.

---

### STEP 3/8: test_data/sns_advertisement_yyyymmdd.csv

```
[STEP 3/8] test_data/sns_advertisement_yyyymmdd.csv
  Before : {current header}
  After  : {new header}
  {if adding: show sample values for all 5 rows}
  {if deleting: show current values in the column being removed}
  {if renaming: show old column name → new column name in header}
Proceed? (y/n/back/all back)
```

`back` → `git checkout -- test_data/sns_advertisement_yyyymmdd.csv` and return to STEP 2.

---

### STEP 4/8: test_data/expected_output.csv

```
[STEP 4/8] test_data/expected_output.csv
  Before : {current header}
  After  : {new header}
  {if adding: show expected values for all 5 rows}
  {if renaming: show old column name → new column name}
Proceed? (y/n/back/all back)
```

`back` → `git checkout -- test_data/expected_output.csv` and return to STEP 3.

---

### STEP 5/8: src/glue/schema.py

`SOURCE_SCHEMA` and `SOURCE_COLUMNS` are defined here and imported by both `glue_job.py` and the test files. This is the only file that needs to change for schema modifications.

```
[STEP 5/8] src/glue/schema.py
  Location : SOURCE_SCHEMA definition (show line number)
  Diff     :
    {StructField to add, remove, or rename}
Proceed? (y/n/back/all back)
```

`back` → `git checkout -- src/glue/schema.py` and return to STEP 4.

**For `--modify`:** update the `name` string in the matching `StructField`.

---

### STEP 6/8: tests/test_glue_job.py + tests/test_data_transform.py

Both files must be updated together in this step. Show diffs for each file separately, then ask for a single confirmation.

**test_glue_job.py** — `SOURCE_COLUMNS` is imported from `schema.py` so column lists update automatically. Check for any hardcoded column lists in `createDataFrame` calls that still need manual update.

**test_data_transform.py** — `SOURCE_SCHEMA` and `SOURCE_COLUMNS` are imported automatically, but the `sample_df` fixture has hardcoded tuple values (one value per row per column). A new column requires a new value in each of the 5 tuples.

```
[STEP 6/8] tests/test_glue_job.py + tests/test_data_transform.py

  test_glue_job.py — {N} location(s):
    1. {function_name} (line {N}): {what changes}
    ...

  test_data_transform.py — sample_df fixture (line {N}):
    Before:
      ("Python Tutorial", 45000, "CodeMastery", 120000, 2300, 42.5),
      ...
    After:
      ("Python Tutorial", 45000, "CodeMastery", 120000, 2300, 42.5, {new_value}),
      ...

  Full diff:
    {combined diff for both files}
Proceed? (y/n/back/all back/or describe what you want)
```

`back` → `git checkout -- tests/test_glue_job.py tests/test_data_transform.py` and return to STEP 5.

**For `-delete`:** remove the value at the deleted column's position from each tuple in `sample_df`.
**For `--modify`:** update the column name in any hardcoded `createDataFrame` column lists.

---

### STEP 7/8: .github/workflows/e2e_test.yml

For add: check if timestamp type → may need to add to `skip_columns`.
For delete: check if column is in `skip_columns` → may need to remove it.
For rename: check if old name is in `skip_columns` → update to new name if so.

```
[STEP 7/8] .github/workflows/e2e_test.yml
  Assessment : {change needed / no change needed (reason)}
  {show diff if change is needed}
Proceed? (y/n/back/all back)
```

`back` → `git checkout -- .github/workflows/e2e_test.yml` and return to STEP 6.

---

### STEP 8/8: Commit & open PR

```
[STEP 8/8] Commit & open PR
  Changed files ({N}):
    {list of modified files}
  Commit message : {Conventional Commits format}
  PR title       : {same as commit message}
  Target branch  : dev

  PR description (auto-generated):
    ## Changes
    {operation type: add / delete / rename / both}
    {column name(s), type, position}

    ## Modified Files
    {bullet list of each file and what changed}

    ## Automated Tests
    Opening this PR against dev will automatically trigger:
    - unit_test.yml (schema evolution logic, Python 3.11 + 3.12)
    - e2e_test.yml  (Athena query results vs. expected output) — runs only if unit tests pass

  ⚠️ Creating this PR will trigger unit_test and e2e_test automatically.
Proceed? (y/n/back/all back)
```

`back` → return to STEP 7 (no git action needed, nothing committed yet).

If y → run `git add` → `commit` → `push` → `gh pr create` and display the PR URL.

```
✅ Schema change workflow complete
  PR: {URL}
  GitHub Actions (unit_test → e2e_test) is now running.
  Check the PR page for results.
```

---

## Post-Workflow: Improvement Suggestions

After the PR is created, review the session log.

**If there were no custom inputs:** skip this section entirely.

**If there were any custom inputs:** display the following and ask for approval:

```
💡 Workflow Improvement Suggestions

The following custom inputs were made during this session:
  [STEP {N}] "{custom request}" → {action taken}
  ...

Based on these, the following improvements are proposed:

  [A] Add to .claude/commands/update-schema.md:
      {specific addition — e.g. a new flag, a new step, a new check}
      Reason: {why this should be a permanent part of the workflow}

  [B] Add to CLAUDE.md:
      {specific addition — e.g. a project convention, a recurring pattern}
      Reason: {why this belongs in the project guide}

Apply these improvements? (y / n / select: A B / describe changes)
```

- `y`            → apply all proposed improvements
- `n`            → skip, no changes made
- `select: A B`  → apply only selected items (space-separated letters)
- Any other text → treat as revision instructions and re-propose

If improvements are applied, commit them separately:
```
chore: improve /update-schema workflow based on session feedback
```

```
✅ Workflow guide updated. Changes will apply from the next /update-schema session.
```
