# data_quality_rules.py

import pandas as pd
import numpy as np
import re
from typing import Dict, Any
import logging
from datetime import datetime
import json

class DataQualityChecker:

    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.quality_issues = {}

    def load_data(self, file_path: str) -> pd.DataFrame:
        try:
            df = pd.read_excel(file_path)
            self.logger.info(f"Data loaded successfully. Shape: {df.shape}")
            return df
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
            raise

    def check_completeness(self, df: pd.DataFrame) -> pd.DataFrame:
        """Measure 1: Data Completeness"""
        # Define critical columns based on your dataset
        critical_columns = [
            'timevalue', 'providerkey', 'companynameofficial', 
            'fiscalperiodend', 'operationstatustype', 'ipostatustype',
            'geonameen', 'industrycode', 'REVENUE', 'unit_REVENUE'
        ]
        
        # Filter to only existing columns
        existing_critical = [col for col in critical_columns if col in df.columns]
        
        if not existing_critical:
            self.logger.warning("No critical columns found, using first 4 columns")
            existing_critical = df.columns[:min(4, len(df.columns))].tolist()

        # Check for missing values in any critical column
        missing_mask = df[existing_critical].isnull().any(axis=1)
        df['flag_completeness'] = missing_mask.astype(int)

        issues_count = missing_mask.sum()
        self.quality_issues['completeness'] = {
            'total_issues': int(issues_count),
            'percentage': float(issues_count / len(df) * 100),
            'description': f'Missing values in critical columns: {existing_critical}'
        }
        
        self.logger.info(f"Completeness check: {issues_count} issues found ({issues_count/len(df)*100:.2f}%)")
        return df

    def check_consistency(self, df: pd.DataFrame) -> pd.DataFrame:
        """Measure 2: Data Consistency """
        consistency_flags = []
        
        for _, row in df.iterrows():
            flag = 0
            
            # Check company name consistency
            if 'companynameofficial' in df.columns and pd.notna(row['companynameofficial']):
                company_name = str(row['companynameofficial'])
                # Check for inconsistent capitalization or special characters
                if not (company_name.isupper() or company_name.istitle()):
                    if not re.match(r'^[A-Z][A-Z\s&\.\-\(\),]*$', company_name):
                        flag = 1
            
            # Check operation status consistency
            if 'operationstatustype' in df.columns and pd.notna(row['operationstatustype']):
                status = str(row['operationstatustype']).upper()
                if status not in ['ACTIVE', 'INACTIVE', 'DORMANT', 'LIQUIDATION']:
                    flag = 1
            
            # Check IPO status consistency
            if 'ipostatustype' in df.columns and pd.notna(row['ipostatustype']):
                ipo_status = str(row['ipostatustype']).upper()
                if ipo_status not in ['PUBLIC', 'PRIVATE', 'SUBSIDIARY']:
                    flag = 1
            
            # Check currency unit consistency
            if 'unit_REVENUE' in df.columns and pd.notna(row['unit_REVENUE']):
                currency = str(row['unit_REVENUE']).upper()
                if not re.match(r'^[A-Z]{3}$', currency):  # 3-letter currency code
                    flag = 1
            
            consistency_flags.append(flag)

        df['flag_consistency'] = consistency_flags
        issues_count = sum(consistency_flags)
        self.quality_issues['consistency'] = {
            'total_issues': issues_count,
            'percentage': float(issues_count / len(df) * 100),
            'description': 'Inconsistent formatting in company names, status types, or currency codes'
        }
        
        self.logger.info(f"Consistency check: {issues_count} issues found ({issues_count/len(df)*100:.2f}%)")
        return df

    def check_validity(self, df: pd.DataFrame) -> pd.DataFrame:
        """Measure 3: Data Validity """
        validity_flags = []
        
        for _, row in df.iterrows():
            flag = 0
            
            # Check timevalue (year) validity
            if 'timevalue' in df.columns and pd.notna(row['timevalue']):
                try:
                    year = int(row['timevalue'])
                    if not (1900 <= year <= 2030):
                        flag = 1
                except (ValueError, TypeError):
                    flag = 1
            
            # Check REVENUE validity
            if 'REVENUE' in df.columns and pd.notna(row['REVENUE']):
                try:
                    revenue = float(row['REVENUE'])
                    # Revenue should be non-negative and within reasonable bounds
                    if revenue < 0 or revenue > 1e15:  # 1 quadrillion limit
                        flag = 1
                except (ValueError, TypeError):
                    flag = 1
            
            # Check fiscal period end format (should be valid date format)
            if 'fiscalperiodend' in df.columns and pd.notna(row['fiscalperiodend']):
                fiscal_date = str(row['fiscalperiodend'])
                # Check for common date patterns like "30-Jun", "31-Dec", etc.
                if not re.match(r'^\d{1,2}-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)$', fiscal_date):
                    flag = 1
            
            # Check industry code format
            if 'industrycode' in df.columns and pd.notna(row['industrycode']):
                industry = str(row['industrycode'])
                # Industry code should follow pattern like "7010 - Description"
                if not re.match(r'^\d{4}\s*-\s*.+', industry):
                    flag = 1
            
            validity_flags.append(flag)

        df['flag_validity'] = validity_flags
        issues_count = sum(validity_flags)
        self.quality_issues['validity'] = {
            'total_issues': issues_count,
            'percentage': float(issues_count / len(df) * 100),
            'description': 'Invalid data ranges, formats, or values detected'
        }
        
        self.logger.info(f"Validity check: {issues_count} issues found ({issues_count/len(df)*100:.2f}%)")
        return df

    def check_uniqueness(self, df: pd.DataFrame) -> pd.DataFrame:
        """Measure 4: Data Uniqueness """
        # Define key columns for uniqueness check
        key_columns = ['providerkey', 'timevalue', 'fiscalperiodend']
        
        # Filter to only existing columns
        existing_key_columns = [col for col in key_columns if col in df.columns]
        
        if not existing_key_columns:
            self.logger.warning("No key columns found for uniqueness check, using all non-flag columns")
            existing_key_columns = [col for col in df.columns if not col.startswith('flag_')]

        # Check for duplicates
        duplicate_mask = df.duplicated(subset=existing_key_columns, keep=False)
        df['flag_uniqueness'] = duplicate_mask.astype(int)

        issues_count = duplicate_mask.sum()
        self.quality_issues['uniqueness'] = {
            'total_issues': int(issues_count),
            'percentage': float(issues_count / len(df) * 100),
            'description': f'Duplicate records based on columns: {existing_key_columns}'
        }
        
        self.logger.info(f"Uniqueness check: {issues_count} issues found ({issues_count/len(df)*100:.2f}%)")
        return df

    def run_all_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Starting data quality checks...")
        
        # Run all quality measures
        df = self.check_completeness(df)
        df = self.check_consistency(df)
        df = self.check_validity(df)
        df = self.check_uniqueness(df)
        
        # Add overall quality flag (1 if any issue exists)
        flag_columns = [col for col in df.columns if col.startswith('flag_')]
        df['flag_overall'] = df[flag_columns].max(axis=1)
        
        self.logger.info("All data quality checks completed.")
        return df

    def generate_quality_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        summary = {
            'total_records': len(df),
            'quality_measures': self.quality_issues,
            'overall_quality_score': 0,
            'timestamp': datetime.now().isoformat(),
            'dataset_info': {
                'columns': list(df.columns),
                'data_types': {col: str(dtype) for col, dtype in df.dtypes.items()}
            }
        }

        # Calculate overall quality score
        total_issues = sum(measure.get('total_issues', 0) for measure in self.quality_issues.values())
        total_possible_issues = len(df) * len(self.quality_issues)
        if total_possible_issues > 0:
            summary['overall_quality_score'] = max(0, 100 - (total_issues / total_possible_issues * 100))

        return summary

    def save_results(self, df: pd.DataFrame, output_path: str = 'quality_checked_data.xlsx'):
        try:
            df.to_excel(output_path, index=False)
            self.logger.info(f"Results saved to {output_path}")
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")
            raise

    def save_summary_report(self, summary: Dict[str, Any], output_path: str = 'quality_summary.json'):
        """Save the quality summary to a JSON file."""
        try:
            # Convert numpy and pandas types to Python types for JSON serialization
            def convert_types(obj):
                if isinstance(obj, np.integer):
                    return int(obj)
                elif isinstance(obj, np.floating):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                elif str(type(obj)).startswith("<class 'pandas."):  # Handle pandas data types
                    return str(obj)
                elif hasattr(obj, '__module__') and obj.__module__ and 'pandas' in obj.__module__:
                    return str(obj)
                elif isinstance(obj, dict):
                    return {k: convert_types(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_types(item) for item in obj]
                return obj
            
            summary_converted = convert_types(summary)
            
            with open(output_path, 'w') as f:
                json.dump(summary_converted, f, indent=2)
            self.logger.info(f"Summary report saved to {output_path}")
        except Exception as e:
            self.logger.error(f"Error saving summary: {e}")
            raise

# Example usage
if __name__ == "__main__":
    # Initialize the data quality checker
    dq_checker = DataQualityChecker()
    
    # Load the data
    df = dq_checker.load_data('CaseStudy_Quality_sample25.xlsx')
    
    # Run all quality checks
    df_with_flags = dq_checker.run_all_checks(df)
    
    # Generate summary
    summary = dq_checker.generate_quality_summary(df_with_flags)
    
    # Save results
    dq_checker.save_results(df_with_flags, 'CaseStudy_Quality_sample25_checked.xlsx')
    dq_checker.save_summary_report(summary, 'quality_summary_report.json')
    
    # Print summary
    print("\n=== DATA QUALITY SUMMARY ===")
    print(f"Total Records: {summary['total_records']}")
    print(f"Overall Quality Score: {summary['overall_quality_score']:.2f}%")
    print("\nIssues by Category:")
    for measure, details in summary['quality_measures'].items():
        print(f"  {measure.capitalize()}: {details['total_issues']} issues ({details['percentage']:.2f}%)")