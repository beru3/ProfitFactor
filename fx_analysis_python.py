#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
曜日別・高期待値エントリーポイント選定システム - Python版
要件定義書に基づいた分析ロジック + 指定フォーマット出力

ディレクトリ構成:
- main.py (このファイル)
- 週刊アノマリーFXレポート_YYYY年MM月DD日 - 分析レポート.csv (レポート1)
- アノマリーFXポイント別損益明細 - ポイント別損益明細(上位20位).csv (レポート2)
- {基準日}/ (実行時に作成、CSVファイルを移動)
- output_{基準日}.txt (分析結果出力)
"""

import os
import shutil
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
import logging
import csv
import traceback
import re
import sys

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FXAnalysisEngine:
    """FX曜日別エントリーポイント選定システム"""
    
    def __init__(self, analysis_weeks: int = 26, pf_threshold: float = 1.3, max_results: int = 20):
        """
        初期化
        
        Args:
            analysis_weeks: 分析対象期間（週数）
            pf_threshold: プロフィットファクター閾値
            max_results: 最大表示件数（各曜日・各パターンごとの表示ポイント数）
        """
        self.settings = {
            'analysis_weeks': analysis_weeks,
            'pf_threshold': pf_threshold,
            'max_results': max_results  # 各曜日・各パターンごとの最大表示件数
        }
        
        # 評価パターンの名称マッピング（レポート1 → レポート2）
        self.pattern_mapping = {
            '勝率重視ポイント': '勝率重視ポイント',
            '利益効率ポイント': '利益効率_STD_ポイント',
            '時間効率ポイント': '時間効率ポイント',
            '最大利益ポイント': '最大利益ポイント',
            '勝率重視ポイントUS': '勝率重視ポイントUS',
            '利益効率ポイントUS': '利益効率_STD_ポイントUS',
            '時間効率ポイントUS': '時間効率ポイントUS',
            '最大利益ポイントUS': '最大利益ポイントUS'
        }
        
        # 出力フォーマット用の評価パターン表示名
        self.pattern_display_names = {
            '利益効率ポイント': '◾️🐝利益効率',
            '勝率重視ポイント': '◾️🐟勝率重視', 
            '時間効率ポイント': '◾️⏰時間効率',
            '最大利益ポイント': '◾️🦏最大利益'
        }
        
        # 基本4パターン（USは除外）
        self.target_patterns = ['利益効率ポイント', '勝率重視ポイント', '時間効率ポイント', '最大利益ポイント']
        
    def find_csv_files(self, base_date: str) -> Tuple[str, str]:
        """
        指定パターンのCSVファイルを検索
        
        Args:
            base_date: 基準日
            
        Returns:
            (レポート1ファイルパス, レポート2ファイルパス)
        """
        # 基準日からディレクトリ名を生成
        base_dir = f"{base_date}"
        
        if not os.path.exists(base_dir):
            raise FileNotFoundError(f"基準日のディレクトリが見つかりません: {base_dir}")
        
        # 固定のファイル名を使用
        report1_file = os.path.join(base_dir, "週刊アノマリーFXレポート_2025年06月22日 - 分析レポート.csv")
        report2_file = os.path.join(base_dir, "アノマリーFXポイント別損益明細 - ポイント別損益明細(上位20位).csv")
        
        # ファイルの存在確認
        if not os.path.exists(report1_file):
            raise FileNotFoundError(f"レポート1ファイルが見つかりません: {report1_file}")
        if not os.path.exists(report2_file):
            raise FileNotFoundError(f"レポート2ファイルが見つかりません: {report2_file}")
        
        logger.info(f"CSVファイル検索完了: report1={report1_file}, report2={report2_file}")
        return report1_file, report2_file

    def load_csv_files(self, base_date: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        CSVファイルを読み込み
        
        Args:
            base_date: 基準日
            
        Returns:
            (レポート1データフレーム, レポート2データフレーム)
        """
        try:
            # ファイル検索
            report1_file, report2_file = self.find_csv_files(base_date)
            
            # レポート1読み込み
            report1_df = pd.read_csv(report1_file, encoding='utf-8-sig')
            logger.info(f"レポート1読み込み完了: {len(report1_df)}行")
            
            # レポート2読み込み
            report2_df = pd.read_csv(report2_file, encoding='utf-8-sig')
            logger.info(f"レポート2読み込み完了: {len(report2_df)}行")
            
            return report1_df, report2_df
            
        except Exception as e:
            logger.error(f"CSVファイル読み込みエラー: {str(e)}")
            raise
    
    def create_directory_and_move_files(self, base_date: str, report1_file: str, report2_file: str) -> str:
        """
        基準日のディレクトリを作成し、ファイルを移動
        
        Args:
            base_date: 基準日
            report1_file: レポート1ファイルパス
            report2_file: レポート2ファイルパス
            
        Returns:
            作成したディレクトリパス
        """
        # 既にディレクトリが存在する場合は何もしない
        directory = base_date
        if os.path.exists(directory):
            logger.info(f"ディレクトリは既に存在します: {directory}")
            return directory
            
        # ディレクトリ作成
        os.makedirs(directory, exist_ok=True)
        logger.info(f"ディレクトリを作成しました: {directory}")
        
        # ファイル移動（既に移動済みの場合はスキップ）
        for file_path in [report1_file, report2_file]:
            if not file_path.startswith(directory):
                filename = os.path.basename(file_path)
                new_path = os.path.join(directory, filename)
                shutil.copy2(file_path, new_path)
                logger.info(f"ファイルをコピーしました: {file_path} -> {new_path}")
        
        return directory
    
    def filter_analysis_period(self, report2_df: pd.DataFrame, weeks: int) -> pd.DataFrame:
        """
        分析対象期間のデータをフィルタリング
        
        Args:
            report2_df: レポート2のDataFrame
            weeks: 分析対象期間（週数）
            
        Returns:
            フィルタリングされたDataFrame
        """
        cutoff_date = datetime.now() - timedelta(weeks=weeks)
        report2_df['取引日'] = pd.to_datetime(report2_df['取引日'])
        filtered_df = report2_df[report2_df['取引日'] >= cutoff_date]
        logger.info(f"分析対象期間: {weeks}週間, フィルタ後データ件数: {len(filtered_df)}件")
        return filtered_df
    
    def extract_points_from_report1(self, report1_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        レポート1から評価パターン別のランキングポイントを抽出
        クロス円（JPYペア）のみを対象とする
        
        Args:
            report1_df: レポート1のDataFrame
            
        Returns:
            ポイント情報のリスト
        """
        points = []
        
        for idx, row in report1_df.iterrows():
            # クロス円（JPYペア）のみをフィルタリング
            currency = str(row['銘柄'])  # 文字列に変換
            if not (currency.endswith('JPY') or currency.startswith('JPY')):
                continue
                
            for pattern in self.target_patterns:
                if pattern in row and pd.notna(row[pattern]):
                    ranking = float(row[pattern])
                    if 1 <= ranking <= 20:
                        # 日方向カラムがない場合は方向カラムを使用
                        day_direction = row.get('日方向', row['方向'])
                        
                        # パーセント文字列を浮動小数点に変換する関数
                        def parse_percent(val):
                            if pd.isna(val):
                                return 0.0
                            if isinstance(val, str) and '%' in val:
                                return float(val.strip('%')) / 100.0
                            return float(val)
                        
                        point_info = {
                            'row_index': idx,
                            'point_name': pattern,
                            'report2_point_name': self.pattern_mapping[pattern],
                            'ranking': int(ranking),
                            'currency': currency,
                            'direction': row['方向'],
                            'entry_time': row['エントリー時刻'],
                            'close_time': row['クローズ時刻'],
                            'details': f"{day_direction}_{currency}_{row['エントリー時刻']}_{row['クローズ時刻']}",
                            'win_rate_30': parse_percent(row.get('勝率_30日', 0)),
                            'win_rate_90': parse_percent(row.get('勝率_90日', 0)),
                            'win_rate_365': parse_percent(row.get('勝率_365日', 0)),
                            'win_rate_avg': parse_percent(row.get('勝率_平均', 0))
                        }
                        points.append(point_info)
        
        # ランキング順でソート
        points.sort(key=lambda x: x['ranking'])
        logger.info(f"レポート1から抽出したポイント数: {len(points)}件")
        logger.info(f"クロス円（JPYペア）のみをフィルタリングしました")
        return points
    
    def calculate_weekly_profit_factor(self, report2_df: pd.DataFrame, weeks: int) -> Dict[str, Any]:
        """
        曜日別プロフィットファクター計算（第1検証フェーズ）
        
        Args:
            report2_df: レポート2のDataFrame
            weeks: 分析対象期間（週数）
            
        Returns:
            曜日別PFデータ構造
        """
        filtered_df = self.filter_analysis_period(report2_df, weeks)
        weekly_pf = {}
        
        for _, row in filtered_df.iterrows():
            point_name = row['ポイント名']
            day_of_week = row['取引日_曜日']
            point_value = int(row['ポイント値'])
            profit_pips = float(row['損益pipsのSUM']) if pd.notna(row['損益pipsのSUM']) else 0.0
            
            # データ構造の初期化
            if point_name not in weekly_pf:
                weekly_pf[point_name] = {}
            if day_of_week not in weekly_pf[point_name]:
                weekly_pf[point_name][day_of_week] = {}
            if point_value not in weekly_pf[point_name][day_of_week]:
                weekly_pf[point_name][day_of_week][point_value] = {
                    'total_profit': 0.0,
                    'total_loss': 0.0,
                    'trades': 0,
                    'pf': 0.0,
                    'profit_pips': profit_pips
                }
            
            target = weekly_pf[point_name][day_of_week][point_value]
            target['trades'] += 1
            target['profit_pips'] = profit_pips
            
            if profit_pips > 0:
                target['total_profit'] += profit_pips
            else:
                target['total_loss'] += abs(profit_pips)
            
            # プロフィットファクター計算
            if target['total_loss'] == 0:
                target['pf'] = 999.9 if target['total_profit'] > 0 else 0
            else:
                target['pf'] = target['total_profit'] / target['total_loss']
        
        logger.info("曜日別プロフィットファクター計算完了")
        return weekly_pf
    
    def select_optimal_points(self, report1_points: List[Dict], weekly_pf: Dict) -> Dict[str, Dict]:
        """
        曜日別最適エントリーポイント選定（第2検証フェーズ）
        
        Args:
            report1_points: レポート1から抽出したポイント
            weekly_pf: 曜日別PFデータ
            
        Returns:
            曜日別最適ポイント
        """
        days_of_week = ['月', '火', '水', '木', '金']
        results = {}
        
        for day in days_of_week:
            results[day] = {pattern: [] for pattern in self.target_patterns}
            
            for point in report1_points:
                point_name = point['report2_point_name']
                ranking = point['ranking']
                
                if (point_name in weekly_pf and 
                    day in weekly_pf[point_name] and 
                    ranking in weekly_pf[point_name][day]):
                    
                    ranking_data = weekly_pf[point_name][day][ranking]
                    
                    if ranking_data['pf'] >= self.settings['pf_threshold']:
                        optimal_point = {
                            'currency': point['currency'],
                            'entry_time': point['entry_time'],
                            'close_time': point['close_time'],
                            'direction': point['direction'],
                            'ranking': point['ranking'],
                            'profit_pips': ranking_data['profit_pips'],
                            'pf': ranking_data['pf'],
                            'trades': ranking_data['trades'],
                            'total_profit': ranking_data['total_profit'],
                            'total_loss': ranking_data['total_loss'],
                            'point_details': point
                        }
                        results[day][point['point_name']].append(optimal_point)
            
            # 各評価パターンでエントリー時間順にソート
            for pattern in self.target_patterns:
                # まずPF順にソート（降順）
                results[day][pattern].sort(key=lambda x: x['pf'], reverse=True)
                # PF閾値以上のポイントを最大表示件数まで選択
                filtered_points = results[day][pattern][:self.settings['max_results']]
                
                # エントリー時間を数値化して比較するための関数
                def time_to_minutes(time_str):
                    hours, minutes, seconds = map(int, time_str.split(':'))
                    # 0時台は24時台として扱う（日付をまたぐケース）
                    if hours == 0:
                        hours = 24
                    return hours * 60 + minutes
                
                # 選択したポイントをエントリー時間順にソート（昇順）
                filtered_points.sort(key=lambda x: time_to_minutes(x['entry_time']))
                results[day][pattern] = filtered_points
        
        logger.info("曜日別最適エントリーポイント選定完了")
        return results
    
    def calculate_weekly_summary(self, day_results: Dict) -> Dict[str, float]:
        """
        週間合計損益計算
        
        Args:
            day_results: 曜日別結果
            
        Returns:
            週間合計損益
        """
        weekly_summary = {pattern: 0.0 for pattern in self.target_patterns}
        
        for day_data in day_results.values():
            for pattern in self.target_patterns:
                for point in day_data[pattern]:
                    weekly_summary[pattern] += point['profit_pips']
        
        logger.info("週間合計損益計算完了")
        return weekly_summary
    
    def format_results(self, optimal_points: Dict[str, Dict]) -> Dict[str, List[Dict]]:
        """
        結果を出力用にフォーマット
        
        Args:
            optimal_points: 曜日別最適ポイント
            
        Returns:
            フォーマット済み結果
        """
        formatted_results = {}
        days_of_week = ['月', '火', '水', '木', '金']
        
        for day in days_of_week:
            formatted_results[day] = []
            
            for pattern, points in optimal_points[day].items():
                if points:
                    for point in points:
                        formatted_point = {
                            '通貨ペア': point['currency'],
                            'エントリー': point['entry_time'],
                            'クローズ': point['close_time'],
                            '方向': point['direction'],
                            '順位': point['ranking'],
                            'PF': f"{point['pf']:.2f}",
                            '結果': '',
                            'pattern': pattern  # パターン情報を追加
                        }
                        formatted_results[day].append(formatted_point)
        
        logger.info("結果のフォーマット完了")
        return formatted_results
    
    def save_output(self, formatted_results: Dict[str, List[Dict]], base_date: str) -> str:
        """
        結果をCSVファイルに保存
        
        Args:
            formatted_results: フォーマット済み結果
            base_date: 基準日
            
        Returns:
            保存したファイルパス
        """
        output_file = f"output_{base_date}.csv"
        
        with open(output_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            
            # 基準日の情報
            writer.writerow([f"{base_date}"])
            writer.writerow([])  # 空行
            
            # 曜日別データ
            day_names = {
                '月': '月曜日',
                '火': '火曜日',
                '水': '水曜日', 
                '木': '木曜日',
                '金': '金曜日'
            }
            
            # 基準日から各曜日の日付を計算
            base_date_obj = datetime.strptime(base_date, '%Y-%m-%d')
            base_weekday = base_date_obj.weekday()  # 0=月, 1=火, 2=水, 3=木, 4=金, 5=土, 6=日
            
            weekday_dates = {}
            weekday_map = {'月': 0, '火': 1, '水': 2, '木': 3, '金': 4}
            
            # 基準日が日曜日の場合、翌日（月曜日）から始まる週を計算
            if base_weekday == 6:  # 日曜日
                for day, day_idx in weekday_map.items():
                    day_date = base_date_obj + timedelta(days=day_idx+1)
                    weekday_dates[day] = day_date.strftime('%Y/%m/%d')
            else:
                # 基準日の週の月曜日を見つける
                monday = base_date_obj - timedelta(days=base_weekday)
                for day, day_idx in weekday_map.items():
                    day_date = monday + timedelta(days=day_idx)
                    weekday_dates[day] = day_date.strftime('%Y/%m/%d')
            
            # パターン名の表示順
            pattern_order = ['利益効率ポイント', '勝率重視ポイント', '時間効率ポイント', '最大利益ポイント']
            pattern_display = {
                '利益効率ポイント': '利益効率',
                '勝率重視ポイント': '勝率重視',
                '時間効率ポイント': '時間効率',
                '最大利益ポイント': '最大利益'
            }
            
            for day in ['月', '火', '水', '木', '金']:
                # 曜日の前に空白行を追加
                writer.writerow([])
                
                # 曜日ヘッダー
                day_date = weekday_dates[day]
                writer.writerow(['', f"{day_names[day]}({day_date})"])
                
                # 列ヘッダー
                headers = ['通貨ペア', 'エントリー', 'クローズ', '方向', '順位', 'PF', '結果']
                header_row = ['']
                for pattern in pattern_order:
                    display_name = pattern_display[pattern]
                    header_row.extend([display_name, '', '', '', '', '', ''])
                writer.writerow(header_row)
                
                # 列ヘッダー（項目名）
                column_header = ['']
                for _ in range(4):  # 4つのパターン
                    column_header.extend(headers)
                writer.writerow(column_header)
                
                # パターン別にデータを取得
                pattern_data = {}
                for pattern in pattern_order:
                    pattern_data[pattern] = []
                    for point in formatted_results[day]:
                        if point.get('pattern') == pattern:
                            pattern_data[pattern].append(point)
                
                # 最大行数を計算
                max_rows = max(len(pattern_data[pattern]) for pattern in pattern_order)
                
                # データ行
                for i in range(max_rows):
                    row = [i + 1]
                    for pattern in pattern_order:
                        if i < len(pattern_data[pattern]):
                            point = pattern_data[pattern][i]
                            row.extend([
                                point.get('通貨ペア', ''),
                                point.get('エントリー', ''),
                                point.get('クローズ', ''),
                                point.get('方向', ''),
                                point.get('順位', ''),
                                point.get('PF', ''),
                                point.get('結果', '')
                            ])
                        else:
                            row.extend(['', '', '', '', '', '', ''])
                    writer.writerow(row)
        
        logger.info(f"結果をCSVファイルに保存しました: {output_file}")
        return output_file
    
    def perform_analysis(self, base_date: str = None) -> Dict[str, Any]:
        """
        メイン分析処理
        
        Args:
            base_date: 基準日（YYYY-MM-DD形式）
            
        Returns:
            分析結果
        """
        try:
            # 1. 基準日の設定
            if base_date is None:
                base_date = self.get_base_date()
            
            # 2. CSVファイルの検索
            report1_file, report2_file = self.find_csv_files(base_date)
            
            # 3. データ読み込み
            report1_df, report2_df = self.load_csv_files(base_date)
            
            # 4. レポート1からポイント抽出
            report1_points = self.extract_points_from_report1(report1_df)
            
            # 5. レポート2から週間PF計算
            weekly_pf = self.calculate_weekly_profit_factor(report2_df, self.settings['analysis_weeks'])
            
            # 6. 曜日別最適ポイント選定
            day_results = self.select_optimal_points(report1_points, weekly_pf)
            
            # 7. 結果をフォーマット
            formatted_results = self.format_results(day_results)
            
            # 8. ファイル保存
            output_file = self.save_output(formatted_results, base_date)
            
            # 9. ディレクトリ作成とファイル移動
            created_dir = self.create_directory_and_move_files(base_date, report1_file, report2_file)
            
            # 10. 出力ファイルを整理
            self.organize_output_files(base_date, output_file)
            
            # 11. 整理後の出力ファイルパスを更新
            output_file_path = os.path.join(created_dir, os.path.basename(output_file))
            
            return {
                'success': True,
                'base_date': base_date,
                'report1_file': report1_file,
                'report2_file': report2_file,
                'created_directory': created_dir,
                'output_file': output_file_path,
                'day_results': day_results,
                'weekly_summary': self.calculate_weekly_summary(day_results),
                'formatted_output': formatted_results,
                'stats': {
                    'report1_records': len(report1_df),
                    'report2_records': len(report2_df),
                    'extracted_points': len(report1_points),
                    'analysis_weeks': self.settings['analysis_weeks']
                }
            }
        except Exception as e:
            logger.error(f"分析処理エラー: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'output_file': f"error_{base_date if base_date else 'unknown'}.log"
            }

    def get_base_date(self) -> str:
        """
        基準日を取得
        
        Returns:
            基準日（YYYY-MM-DD形式）
        """
        # ディレクトリ名から基準日を取得
        dirs = [d for d in os.listdir() if os.path.isdir(d) and re.match(r'\d{4}-\d{2}-\d{2}', d)]
        
        if dirs:
            # 最新の日付を使用
            latest_dir = max(dirs)
            return latest_dir
        else:
            # デフォルト値
            return "2025-06-22"

    def main(self) -> str:
        """
        メイン処理
        
        Returns:
            出力ファイルパス
        """
        try:
            # 分析実行
            result = self.perform_analysis()
            
            # 出力ファイルパスを返す
            return result['output_file']
            
        except Exception as e:
            logger.error(f"処理中にエラーが発生しました: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def organize_output_files(self, base_date: str, output_file: str) -> None:
        """
        出力ファイルを整理
        
        Args:
            base_date: 基準日
            output_file: 出力ファイルパス
        """
        directory = base_date
        
        # 出力ファイルが基準日ディレクトリにない場合は移動
        logger.info(f"出力ファイルの整理を開始: {output_file}")
        
        if not output_file.startswith(directory):
            filename = os.path.basename(output_file)
            new_path = os.path.join(directory, filename)
            
            logger.info(f"出力ファイルを移動します: {output_file} -> {new_path}")
            
            # ファイルが既に存在する場合は上書き
            if os.path.exists(new_path):
                os.remove(new_path)
                logger.info(f"既存の出力ファイルを削除しました: {new_path}")
            
            # 出力ファイルをコピーして元のファイルを削除
            shutil.copy2(output_file, new_path)
            logger.info(f"出力ファイルをコピーしました: {output_file} -> {new_path}")
            os.remove(output_file)
            logger.info(f"元の出力ファイルを削除しました: {output_file}")
            
            # 元のパスを更新
            output_file = new_path
        
        # 古いtxtファイルを削除
        txt_pattern = f"output_{base_date}.txt"
        if os.path.exists(txt_pattern):
            os.remove(txt_pattern)
            logger.info(f"古いtxtファイルを削除しました: {txt_pattern}")
        
        # 基準日ディレクトリ内のtxtファイルも削除
        txt_pattern = os.path.join(directory, f"output_{base_date}.txt")
        if os.path.exists(txt_pattern):
            os.remove(txt_pattern)
            logger.info(f"古いtxtファイルを削除しました: {txt_pattern}")
            
        logger.info(f"出力ファイルの整理が完了しました")


def main():
    """
    エントリーポイント
    """
    try:
        analyzer = FXAnalysisEngine()
        output_file = analyzer.main()
        print(f"分析完了。結果は {output_file} に保存されました。")
        print("入力ファイルと出力ファイルは基準日のディレクトリに整理されました。")
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()