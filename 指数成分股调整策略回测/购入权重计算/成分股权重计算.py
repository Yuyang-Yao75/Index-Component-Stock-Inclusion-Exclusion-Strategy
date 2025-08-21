# Copyright (c) 2025 Yuyang Yao
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0
from iFinDPy import *
import pandas as pd
import numpy as np
import os
from datetime import datetime

def ths_login():
    """登录同花顺"""
    ret = THS_iFinDLogin('aaaaa', 'aaaaa')
    print(ret)
    if ret != 0:
        raise RuntimeError("登陆失败")
    print("登陆成功")

def ensure_ifind_login():
    """确保同花顺已登录"""
    if os.getenv("IS_LOGIN")=="1":
        return
    ths_login()
    os.environ["IS_LOGIN"]="1"

def get_last_trading_day(current_date=None):
    """获取指定日期的前一个交易日"""
    ensure_ifind_login()
    if current_date is None:
        current_date = datetime.now().strftime("%Y-%m-%d")
    return pd.to_datetime(THS_Date_Offset('212001', 'dateType:0,period:D,offset:-1,dateFormat:0,output:singledate', current_date).data)

def get_stock_market_caps(stock_codes: list, date: datetime) -> pd.DataFrame:
    """获取指定股票在指定日期的市值数据"""
    ensure_ifind_login()
    date_str = date.strftime("%Y-%m-%d")
    data_result = THS_DS(stock_codes, 'ths_market_value_stock;ths_ashare_mv_barring_ls_stock', ';', '', date_str, date_str)
    if data_result.errorcode != 0:
        print('error:{}'.format(data_result.errmsg))
        raise ValueError(f"获取市值数据失败")
    
    df = data_result.data.rename(columns={
        "thscode": "stock_code",
        "ths_market_value_stock": "market_cap",
        "ths_ashare_mv_barring_ls_stock": "A_float_cap"
    })
    df["time"] = pd.to_datetime(df["time"])
    return df

def get_stock_close_prices(stock_codes: list) -> pd.DataFrame:
    """获取指定股票在前一个交易日的收盘价数据"""
    ensure_ifind_login()
    last_trading_day = get_last_trading_day()
    date_str = last_trading_day.strftime("%Y-%m-%d")
    data_result = THS_DS(stock_codes, 'ths_close_price_stock', '', '', date_str, date_str)
    if data_result.errorcode != 0:
        print('error:{}'.format(data_result.errmsg))
        raise ValueError(f"获取收盘价数据失败")
    
    df = data_result.data.rename(columns={
        "thscode": "stock_code",
        "ths_close_price_stock": "close_price"
    })
    df["time"] = pd.to_datetime(df["time"])
    return df

def calculate_weights(method: str, stocks: list, cap_data: pd.DataFrame) -> dict:
    """计算每只股票的权重"""
    weights = {}
    if method == 'equal':
        if len(stocks) == 0:
            return {}
        w = 1.0/len(stocks)
        for code in stocks:
            weights[code] = w
    elif method in ('market_cap', 'A_float_cap'):
        cap_field = method
        filtered_data = cap_data[cap_data["stock_code"].isin(stocks)]
        total_cap = filtered_data[cap_field].sum()
        if total_cap > 0:
            w_series = filtered_data.set_index('stock_code')[cap_field]/total_cap
            weights = {code: float(w_series.get(code, 0.0)) for code in stocks}
        else:
            weights = {code: 0.0 for code in stocks}
    else:
        raise ValueError(f"不支持的权重计算方式：{method}")
    return weights

def process_index_weights(input_file: str, market_cap_date: datetime, initial_capital: float = 1000000):
    """处理指数成分股权重计算
    
    Args:
        input_file: 输入文件名
        market_cap_date: 市值数据日期，默认为2025年5月30日
        initial_capital: 初始资金
    """
    # 读取调入调出数据
    adjustments = pd.read_excel(input_file)
    adjustments["execute_day"] = pd.to_datetime(adjustments["execute_day"])
    
    # 获取所有股票代码
    stock_codes = adjustments["stock_code"].unique().tolist()
    
    # 获取市值和收盘价数据
    cap_data = get_stock_market_caps(stock_codes, market_cap_date)
    price_data = get_stock_close_prices(stock_codes)  # 使用前一交易日收盘价
    
    # 合并价格和市值数据
    stock_data = pd.merge(cap_data, price_data[["stock_code", "close_price"]], on="stock_code", how="left")
    
    # 计算不同权重方法的结果
    weight_methods = ['equal', 'market_cap', 'A_float_cap']
    all_weights = {}
    
    for method in weight_methods:
        weights = calculate_weights(method, stock_codes, stock_data)
        
        # 计算各项指标
        details = []
        for code in stock_codes:
            if code not in weights:
                continue
                
            price = float(stock_data[stock_data["stock_code"] == code]["close_price"].iloc[0])
            weight = weights[code]
            weight_pct = weight * 100
            market_value = initial_capital * weight
            shares_lot = round(market_value / (price * 100))
            actual_value = shares_lot * price * 100
            
            details.append({
                "股票代码": code,
                f"权重(%：{method})": round(weight_pct, 2),
                "目标市值": round(market_value, 2),
                "收盘价": round(price, 2),
                "建议手数": shares_lot,
                "实际市值": round(actual_value, 2)
            })
        
        all_weights[method] = pd.DataFrame(details).set_index("股票代码")
    
    # 合并所有权重方法的结果
    dfs = [df for df in all_weights.values()]
    merged_df = pd.concat(dfs, axis=1)
    
    # 计算合计行
    sums = merged_df.sum(numeric_only=True)
    sum_df = pd.DataFrame(sums).T
    sum_df.index = ['合计']
    final_df = pd.concat([merged_df, sum_df])
    
    # 保存结果
    output_file = f"成分股权重计算结果_{market_cap_date.strftime('%Y%m%d')}.xlsx"
    with pd.ExcelWriter(output_file) as writer:
        final_df.to_excel(writer, sheet_name=market_cap_date.strftime("%Y%m"))
    
    print(f"权重计算完成，结果已保存至：{output_file}")
    return final_df

if __name__ == "__main__":
    # 设置参数
    INPUT_FILE = "000300.SH调入调出最新.xlsx"  # 输入文件名
    MARKET_CAP_DATE = pd.to_datetime("2025-05-30")  # 市值数据日期
    INITIAL_CAPITAL = 1000000  # 初始资金100万
    
    # 执行权重计算
    result_df = process_index_weights(
        input_file=INPUT_FILE,
        market_cap_date=MARKET_CAP_DATE,
        initial_capital=INITIAL_CAPITAL
    ) 