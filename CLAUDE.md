# Claude-Driven Schema Evolution - Project Guide

## プロジェクト概要

CSV スキーマ変更（カラム追加）を半自動化するパイプライン。
- AWS Glue（PySpark）→ Apache Iceberg → Athena
- インフラ：Terraform
- CI/CD：GitHub Actions（unit_test / e2e_test / terraform_apply / terraform_destroy）

## ブランチ戦略

- `main`：本番
- `dev`：開発統合ブランチ
- `test_YYYYMMDD` or `feature/xxx`：作業ブランチ（dev から切る）

---

## カラム追加時のワークフロー（最重要）

カラム追加を依頼されたら、**以下の手順を必ず1ステップずつ実施すること。**
各ステップで「何をするか・何が変わるか・成果物」を明示し、**必ず雅に確認（y/n）を取ってから実施する。**
y が返ってきた場合のみ次に進む。n の場合は内容を修正してから再提示する。

### STEP 1：作業ブランチの作成

**提示する内容：**
- 作成するブランチ名
- ベースブランチ（dev）

**確認フォーマット：**
```
[STEP 1] ブランチ作成
  作成するブランチ: feature/add-{カラム名}
  ベース: dev
実施してよいですか？ (y/n)
```

---

### STEP 2：test_data/sns_advertisement_yyyymmdd.csv の修正

**提示する内容：**
- 変更前後のヘッダー行
- 追加するサンプルデータ（5行分）

**確認フォーマット：**
```
[STEP 2] test_data/sns_advertisement_yyyymmdd.csv
  変更箇所: ヘッダーに {カラム名} を追加
  変更前: video_title,views,...
  変更後: video_title,views,...,{カラム名}
  追加サンプル値（5行）: [値の一覧]
実施してよいですか？ (y/n)
```

---

### STEP 3：test_data/expected_output.csv の修正

**提示する内容：**
- 変更前後のヘッダー行
- 追加する期待値（5行分）
- partition_date と processed_at の位置関係

**確認フォーマット：**
```
[STEP 3] test_data/expected_output.csv
  変更箇所: ヘッダーに {カラム名} を追加（partition_date の前に挿入）
  変更前: ...,partition_date,processed_at
  変更後: ...,{カラム名},partition_date,processed_at
  追加期待値（5行）: [値の一覧]
実施してよいですか？ (y/n)
```

---

### STEP 4：src/glue/glue_job.py の修正

**提示する内容：**
- 追加する StructField の定義
- 変更する行番号と差分

**確認フォーマット：**
```
[STEP 4] src/glue/glue_job.py
  変更箇所: StructType 定義（~134行目）に StructField を追加
  追加内容: StructField("{カラム名}", {DataType}(), True)
  差分:
    + StructField("{カラム名}", {DataType}(), True),
実施してよいですか？ (y/n)
```

---

### STEP 5：tests/test_glue_job.py の修正

**提示する内容：**
- 変更する箇所（テスト関数名・行番号）
- 差分

**確認フォーマット：**
```
[STEP 5] tests/test_glue_job.py
  変更箇所:
    - required_columns リストに "{カラム名}" を追加（複数箇所）
    - テストデータに対応する値を追加
  差分:
    [変更前後の差分を表示]
実施してよいですか？ (y/n)
```

---

### STEP 6：.github/workflows/e2e_test.yml の確認

**判断基準：**
- タイムスタンプ型（値が毎回変わる）→ skip_columns に追加が必要
- 通常カラム → 修正不要

**確認フォーマット：**
```
[STEP 6] .github/workflows/e2e_test.yml
  判定: {修正必要 / 修正不要（通常カラムのため）}
  ※修正必要の場合のみ差分を提示
実施してよいですか？ (y/n)  ← 修正不要の場合も確認を取る
```

---

### STEP 7：ローカルユニットテスト実行

**提示する内容：**
- 実行コマンド
- 実行後に結果を表示

**確認フォーマット：**
```
[STEP 7] ローカルユニットテスト
  実行コマンド: pytest tests/test_glue_job.py -v
実施してよいですか？ (y/n)
```

テスト結果を表示後：
```
[STEP 7 結果]
  {テスト結果}
  → PASSED / FAILED
次のステップ（PR作成）に進んでよいですか？ (y/n)
```

---

### STEP 8：コミット & PR 作成

**提示する内容：**
- コミットメッセージ
- PR タイトル・説明
- 対象ブランチ（feature → dev）

**確認フォーマット：**
```
[STEP 8] コミット & PR 作成
  コミットメッセージ: feat: Add {カラム名} column to schema
  PR タイトル: feat: Add {カラム名} column
  PR 説明: [変更内容の要約]
  マージ先: dev
実施してよいですか？ (y/n)
```

PR 作成後：
```
[STEP 8 完了]
  PR URL: {URL}
  GitHub Actions が自動で以下を実行中:
    - unit_test.yml
    - e2e_test.yml
  結果を確認してください。
```

---

## ステップ進行の原則

- **各ステップは独立して確認を取る。まとめて実施しない。**
- n が返ってきたら内容を修正して同じステップを再提示する。
- ステップ途中でエラーが発生したら即座に報告し、次に進まない。
- 確認なしにファイルを変更しない。

## データ型マッピング（参考）

| 用途     | Spark StructField  |
|----------|--------------------|
| テキスト | StringType()       |
| 整数     | LongType()         |
| 小数     | DoubleType()       |
| 日付     | DateType()         |
| 真偽値   | BooleanType()      |
