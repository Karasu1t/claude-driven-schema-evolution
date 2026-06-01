スキーマ変更ワークフローを開始します。

---

## フェーズ1：変更内容の確定

実行前にすべての情報を確定させる。ここで決めた内容をもとにフェーズ2の全ファイル修正を行う。

### 1-1. 操作の確認

引数: $ARGUMENTS

引数の有無にかかわらず、必ず以下を表示して確認する：

```
どのような変更ですか？
  1. カラムの追加    → -add {カラム名} {DataType}  例: -add video_category StringType
  2. カラムの削除    → -delete {カラム名}           例: -delete video_duration_minutes
  3. 追加と削除の両方 → -add ... -delete ...

変更内容を教えてください:
```

引数がある場合は内容を表示して確認を取る。ない場合は入力を待つ。

---

### 1-2. 挿入位置の確認（追加がある場合のみ）

`src/glue/glue_job.py` の StructType 定義（schema = StructType([...])）を読み込み、**生データのカラムのみ**を番号付きで表示する。
※ `processed_at` / `glue_job_run_id` / `ver_date` などジョブが付加するメタカラムは表示しない。

```
📌 挿入位置の確認
  現在のカラム順（src/glue/glue_job.py の StructType 基準 ※メタカラム除く）:
    1. video_title
    2. views
    ...（StructType に定義された生データカラムのみ表示）

  {追加するカラム名} をどこに挿入しますか？
  → 番号を入力（例: 3 と入力 → 3番目のカラムの後に挿入）
  → l または +last で末尾に追加
```

入力を受け取り、挿入位置を確定する。

---

### 1-3. 変更内容の最終確認

上記で確定した内容をまとめて表示し、実行前に一度だけ確認を取る：

```
📋 変更内容の確認
  ─────────────────────────────
  操作:   追加 / 削除 / 両方
  追加:   {カラム名}（{DataType}）  ← 追加がある場合
  削除:   {カラム名}               ← 削除がある場合
  挿入位置: {前のカラム名} の後    ← 追加がある場合（末尾なら「末尾」）
  ─────────────────────────────
  修正対象ファイル（{N}件）:
    - terraform/modules/aws/glue_table/main.tf   … Glueカタログのテーブル定義（Athenaで参照される列情報）
    - test_data/sns_advertisement_yyyymmdd.csv   … E2Eテスト用の入力CSVテンプレート
    - test_data/expected_output.csv              … E2Eテストの期待値（Athenaクエリ結果と比較）
    - src/glue/glue_job.py                       … Glue Job本体のスキーマ定義・変換ロジック
    - tests/test_glue_job.py                     … ユニットテスト（スキーマ進化ロジックの検証）
    - .github/workflows/e2e_test.yml             … E2E自動テストのワークフロー定義（条件付き）

この内容でよいですか？ (y/n)
```

y なら フェーズ2 へ進む。n なら 1-1 に戻る。

---

## フェーズ2：実行（1ステップずつ確認）

**各ステップで差分を提示し `実施してよいですか？ (y/n)` と確認を取る。**
y のみ次に進む。n なら修正して同じステップを再提示する。まとめて実施しない。

---

### STEP 1/8：作業ブランチ作成

ブランチ名ルール：
- 追加のみ → `feature/{YYYYMMDD}/add_{カラム名}`
- 削除のみ → `feature/{YYYYMMDD}/remove_{カラム名}`
- 両方 → `feature/{YYYYMMDD}/schema_update`

`git branch --show-current` で現在のブランチを確認し、`dev` でなければ先に切り替える。

```
[STEP 1/8] ブランチ作成
  現在のブランチ: {現在のブランチ名}
  → {devでない場合: "dev に切り替えてから作成" / devの場合: "そのまま作成"}
  作成するブランチ: feature/{YYYYMMDD}/{操作}_{カラム名}
実施してよいですか？ (y/n)
```

---

### STEP 2/8：terraform/modules/aws/glue_table/main.tf

```
[STEP 2/8] terraform/modules/aws/glue_table/main.tf
  操作: {挿入位置の前後カラム名を明示}
  差分:
    {追加 or 削除する columns ブロックの差分}
実施してよいですか？ (y/n)
```

DataType → Terraform型：StringType→string / LongType・IntegerType→bigint / DoubleType→double / BooleanType→boolean / DateType→date

---

### STEP 3/8：test_data/sns_advertisement_yyyymmdd.csv

```
[STEP 3/8] test_data/sns_advertisement_yyyymmdd.csv
  変更前ヘッダー: {現在のヘッダー}
  変更後ヘッダー: {新しいヘッダー}
  {追加の場合: サンプル値（5行）を表示}
  {削除の場合: 削除される列の現在値を表示}
実施してよいですか？ (y/n)
```

---

### STEP 4/8：test_data/expected_output.csv

```
[STEP 4/8] test_data/expected_output.csv
  変更前ヘッダー: {現在のヘッダー}
  変更後ヘッダー: {新しいヘッダー}
  {追加の場合: 期待値（5行）を表示}
実施してよいですか？ (y/n)
```

---

### STEP 5/8：src/glue/glue_job.py

```
[STEP 5/8] src/glue/glue_job.py
  変更箇所: StructType 定義（行番号を明示）
  差分:
    {追加 or 削除する StructField の差分}
実施してよいですか？ (y/n)
```

---

### STEP 6/8：tests/test_glue_job.py

```
[STEP 6/8] tests/test_glue_job.py
  変更箇所（{N}箇所）:
    1. {関数名}（行番号）
    2. {関数名}（行番号）
    ...
  差分:
    {全差分を表示}
実施してよいですか？ (y/n)
```

---

### STEP 7/8：.github/workflows/e2e_test.yml

追加→タイムスタンプ型なら skip_columns に追加が必要か判定。
削除→skip_columns に該当カラムがあれば削除が必要か判定。

```
[STEP 7/8] .github/workflows/e2e_test.yml
  判定: {修正必要 / 修正不要（理由を明示）}
  {修正必要の場合は差分を表示}
実施してよいですか？ (y/n)
```

---

### STEP 8/8：コミット & PR 作成

変更されたファイル一覧・コミットメッセージ・PR概要を提示する。
PR を dev に向けて作成することで **GitHub Actions が自動的に unit_test / e2e_test を実行する**。

```
[STEP 8/8] コミット & PR 作成
  変更ファイル（{N}件）:
    {変更されたファイルの一覧}
  コミットメッセージ: {Conventional Commits 形式}
  PR タイトル: {同上}
  マージ先: dev

  PR 概要（本文に記載）:
    ## 変更内容
    {操作の種別（追加 / 削除 / 両方）}
    {カラム名・型・挿入位置}

    ## 修正ファイル
    {変更したファイルと各ファイルで何を変えたかを箇条書き}

    ## 自動テスト
    このPRのdev向けマージにより以下が自動実行されます:
    - unit_test.yml（スキーマ進化ロジックのユニットテスト）
    - e2e_test.yml（Athenaクエリ結果と期待値の突合）

  ⚠️ PR を作成すると unit_test / e2e_test が自動で走ります。
実施してよいですか？ (y/n)
```

y なら `git add` → `commit` → `push` → `gh pr create` を実行し PR の URL を表示する。

```
✅ スキーマ変更ワークフロー完了
  PR: {URL}
  GitHub Actions（unit_test / e2e_test）が自動実行中です。
  PR ページで結果を確認してください。
```
