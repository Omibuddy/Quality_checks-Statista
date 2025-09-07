# Data Quality Analysis Framework

## Overview
This project implements automated data quality checks for financial company data extracted from various sources. The framework ensures accuracy and consistency through rule-based, deterministic quality assessments.

## Features
- **4 Core Quality Measures**: Completeness, Consistency, Validity, and Uniqueness
- **Automated Flagging**: Binary flags for each quality issue
- **Comprehensive Reporting**: JSON summary with detailed metrics
- **Financial-Specific Validations**: Custom rules for revenue, dates, currencies, and company status

## Results Summary
- **Total Records**: 372
- **Overall Quality Score**: 86.36%
- **Issues Found**: 203 total issues across all quality measures

### Quality Issues Breakdown
| Quality Measure | Issues | Percentage | Status |
|----------------|--------|------------|--------|
| Completeness   | 93     | 25.00%     | Critical |
| Validity       | 70     | 18.82%     | Warning |
| Consistency    | 40     | 10.75%     | Warning |
| Uniqueness     | 0      | 0.00%      | Good |

## Files
- `data_quality.py` - Main implementation with DataQualityChecker class
- `CaseStudy_Quality_sample25.xlsx` - Input dataset
- `CaseStudy_Quality_sample25_checked.xlsx` - Output with quality flags
- `quality_summary_report.json` - Detailed quality metrics
- `data_quality_presentation.html` - Interactive presentation

## Usage
```bash
python data_quality.py
```

## Key Technical Challenge
**Problem**: JSON serialization error with pandas data types (Int64DType)
**Solution**: Convert pandas data types to strings before JSON serialization
```python
# Fixed implementation
'data_types': {col: str(dtype) for col, dtype in df.dtypes.items()}
```

## Quality Measures Explained

### 1. Completeness
Checks for missing values in critical financial columns:
- timevalue, providerkey, companynameofficial
- fiscalperiodend, operationstatustype, ipostatustype
- geonameen, industrycode, REVENUE, unit_REVENUE

### 2. Consistency
Validates formatting standards:
- Company name capitalization patterns
- Operation status values (ACTIVE, INACTIVE, DORMANT, LIQUIDATION)
- IPO status values (PUBLIC, PRIVATE, SUBSIDIARY)
- Currency code format (3-letter ISO codes)

### 3. Validity
Checks data ranges and formats:
- Year validation (1900-2030)
- Revenue bounds (0 to 1e15)
- Fiscal period date format
- Industry code pattern matching

### 4. Uniqueness
Detects duplicate records based on key columns:
- providerkey, timevalue, fiscalperiodend

## Recommendations
1. Address completeness issues (25% of records have missing values)
2. Validate data ranges and formats
3. Standardize formatting conventions
4. Implement data validation at source systems

## Dependencies
- pandas
- numpy
- openpyxl (for Excel file handling)
