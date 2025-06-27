# fx_PF

FX trading analysis system that identifies high-expectation entry points by day of the week.

## 環境要件

- Python 3.13以上
- pandas

## セットアップ

1. 仮想環境を作成する:
   ```
   python3.13 -m venv venv_py313
   ```

2. 仮想環境を有効化する:
   ```
   source venv_py313/bin/activate  # macOS/Linux
   # または
   # venv_py313\Scripts\activate  # Windows
   ```

3. 必要なパッケージをインストールする:
   ```
   pip install -r requirements.txt
   ```

## 使用方法

1. 分析対象のCSVファイルを配置する:
   - `週刊アノマリーFXレポート_YYYY年MM月DD日 - 分析レポート.csv`
   - `アノマリーFXポイント別損益明細 - ポイント別損益明細(上位20位).csv`

2. スクリプトを実行する:
   ```
   python fx_analysis_python.py
   ```

3. 結果は `output_YYYY-MM-DD.csv` に出力されます
