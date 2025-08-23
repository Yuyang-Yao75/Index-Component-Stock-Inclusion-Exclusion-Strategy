# Index Constituents Forecast & Strategy Backtest

[![EN](https://img.shields.io/badge/lang-English-blue.svg)](./README.en.md)
[![CN](https://img.shields.io/badge/lang-中文-red.svg)](./README.md)

> **License**
> This repository is released under the **PolyForm Noncommercial License 1.0.0**. It is strictly limited to non-commercial use (learning, research, evaluation).
> **Any commercial usage requires a separate license.** For business licensing inquiries, please contact: [yyyao75@163.com](mailto:yyyao75@163.com)
> See [LICENSE](./LICENSE) and [COMMERCIAL.md](./COMMERCIAL.md) for details.

---

## 📘 Project Overview

This repository focuses on **forecasting index constituent changes** and **backtesting adjustment strategies**. It is designed to help researchers and investors understand the quantitative implementation of index methodology and evaluate the impact of constituent changes on portfolio performance. Core capabilities include:

* Forecasting CSI 300 index constituent changes based on the official index methodology.
* Strategy construction inspired by Guoyuan Securities' report *"Harley Covenant: Stock Selection Strategy Based on Index Adjustment"*, including backtesting, parameter optimization, and weight allocation.
* Providing transparent data processing pipelines and backtest outputs.

---

## ✨ Features

### 1) **Automated Data Retrieval & Caching**

* Wrapper around **iFinDPy** APIs for batch retrieval of **CSI 300** and **A-share universe** data.
* Automatic handling of missing values and consistency checks.
* Results cached into **Excel** to reduce redundant API calls and accelerate iteration.

### 2) **Rigorous Universe Construction & Screening**

* Sample construction aligned with index methodology:

  * Exclusion of \**ST/*ST** stocks.
  * Filters on **listing age** and **suspension days**.
  * Scoring by **market cap** and **turnover**.
  * Liquidity screen (50%–60% percentile) to retain tradable names.

### 3) **CSI 300 Buffer & Inclusion Logic**

* Reproduction of CSI rules with separation of **incumbents vs. candidates**.
* Implements **240/360 rank buffer zones** with **priority add/remove** labels.
* Dynamically maintains exactly **300 constituents**, faithfully simulating index add/drop events.

### 4) **Forecast Evaluation & Export**

* Auto-compare forecasts with **official add/drop lists**.
* Compute **hit rate** and produce **difference reports** with explanations.
* Export results to **Excel**, supporting batch replays and accuracy analysis.

### 5) **Event-Driven Backtest Engine**

* Configurable **announcement-day (N)** / **effective-day (M)** offsets.
* Portfolio construction modes: **equal-weight**, **total market-cap**, **free-float market-cap**.
* Daily NAV tracking with performance metrics: **return, max drawdown, Sharpe, Calmar**, etc.

### 6) **Parameter Optimization & Visualization**

* Built-in **grid search** for timing and weighting hyperparameters.
* Generates **heatmaps, box plots, sensitivity curves** for robust configuration selection.

### 7) **One-Click Weight Scheme Generator**

* Standalone script to compute target weights under **equal-weight**, **total cap**, and **free-float cap**.
* Exports **trade guidance** with suggested **lot sizes**, ready for live or simulated execution.

---

## 📂 Repository Structure

```plaintext
│  README.md
│  000300_Index_Methodology_cn.pdf
│  20221212-GuoyuanSecurities-HarleyCovenant-IndexStrategy.pdf
│
├─Index Constituents Forecast
│      CSI300_constituent_forecast.py
│      a_stock_20211031.xlsx
│      a_stock_20220430.xlsx
│      ...
│      hs300_20211031.xlsx
│      hs300_20220430.xlsx
│      ...
│      download_stock_data20211031.csv
│      download_stock_data20220430.csv
│      ...
│      a_stock_20211031_processed.xlsx
│      a_stock_20220430_processed.xlsx
│      ...
│      forecast_results_2021_12.xlsx
│      forecast_results_2022_06.xlsx
│      ...
└─Strategy Backtest
    │  strategy_backtest.py
    │  000300.SH_backtest_201406-202412_N1_M-4_A_float_cap.xlsx
    │  000300.SH_backtest_201406-202412_N1_M-4_equal.xlsx
    │  000300.SH_backtest_201406-202412_N1_M-4_market_cap.xlsx
    │      ...
    │  000300.SH_strategy_vs_index_201406-202412_N1_M-4_A_float_cap.png
    │  000300.SH_strategy_vs_index_201406-202412_N1_M-4_equal.png
    │  000300.SH_strategy_vs_index_201406-202412_N1_M-4_market_cap.png
    │      ...
    │  000300.SH_merged_details_201406-202412_N1_M-4.xlsx
    │  000300.SH_constituents_cap_201406-202412.xlsx
    │  000300.SH_constituents_openprice_201406-202412.xlsx
    │  000300.SH_index_openprice_201406-202412.xlsx
    │  000300.SH_add_drop_201406-202412.xlsx
    │      ...
    ├─Weight Prediction
    │        000300.SH_add_drop_latest.xlsx
    │        weight_calculator.py
    │        weight_results_20250530.xlsx
    │        
    └─Parameter Optimization Results
            optimization_summary.xlsx
            sensitivity_analysis.png
            weight_comparison_boxplot.png
            heatmap_A_float_cap_annual_return.png
            heatmap_equal_annual_return.png
            heatmap_market_cap_annual_return.png
```

---

## 📑 Core Files

### Main Directory

* **README.md**: Project documentation.

### Subdirectory 1: Index Constituents Forecast

Contains core forecasting scripts and input/output data. It applies index methodology, filtering, tagging, and processing to generate predictions. Further improvement could integrate ML classifiers.

* Key script: `CSI300_constituent_forecast.py`

### Subdirectory 2: Strategy Backtest

Provides backtesting, parameter optimization, and weight calculation functionality. Supports auto-download of required datasets and outputs backtest results, heatmaps, and sensitivity analyses.

* Key script: `strategy_backtest.py`

---

## 🚀 Quick Start

1. **Prepare Data**: Ensure all Excel/CSV files are placed in the correct directories. Some historical constituent data may need manual download from iFinD.
2. **Run Forecast Script**:

   ```bash
   python Index Constituents Forecast/CSI300_constituent_forecast.py
   ```

   Enter the target year and month when prompted. The script outputs processed data and forecast results.
3. **Run Backtest Script**:

   ```bash
   python Strategy Backtest/strategy_backtest.py
   ```

   Configure index code, backtest period, M/N offsets, and weight schemes in the script. Comment/uncomment features as needed.

---

## 📊 Outputs

* **Forecast Module**: Processed A-share datasets and forecast vs. official comparison tables.
* **Backtest Module**: Backtest results, strategy vs. index comparison charts, parameter optimization heatmaps, and sensitivity analysis visuals.

---

## 🙌 Acknowledgements & Citation

If you use this project in your research, please cite it as described in `CITATION.cff`. Suggestions and PRs are welcome. Some referenced files or code may not be uploaded to GitHub; please contact the author via email for access if needed.

> Note: This project is currently in a stage of partial completion, and there will be no updates in the short term. Honestly, I am well aware that the code still has many shortcomings in terms of structure, efficiency, and complexity. If you notice any issues while reading or using it, I sincerely welcome your feedback and criticism. I firmly believe that open exchange and genuine sharing are the foundation of mutual growth, and that is the true value of this work.