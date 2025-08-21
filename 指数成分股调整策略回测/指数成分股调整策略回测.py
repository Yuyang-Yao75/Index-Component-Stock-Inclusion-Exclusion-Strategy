ANNUAL_RF = 0.02
TRADING_DAYS = 244

from iFinDPy import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.rcParams['font.family']='SimHei'
matplotlib.rcParams['axes.unicode_minus']=False
import os


#============定义登陆函数==============
def ths_login():
    ret = THS_iFinDLogin('aaaaa', 'aaaaa')
    print(ret)
    if ret != 0:
        raise  RuntimeError("登陆失败")
    print("登陆成功")

def ensure_ifind_login():
    if os.getenv("IS_LOGIN")=="1":
        return
    ths_login()
    os.environ["IS_LOGIN"]="1"
#============数据获取=================
def get_index_adjustment(index_code:str,start_year:int,start_month:int,end_year:int,end_month:int)->pd.DataFrame:
    file_path=f"{index_code}调入调出_{start_year}{start_month:02d}-{end_year}{end_month:02d}.xlsx"
    if os.path.exists(file_path):
        data_df=pd.read_excel(file_path)
        print(f"{file_path}文件已存在，读取备份文件")
        return data_df
    # 指数进出记录,输入参数:指数代码(iv_zsdm)、起始日期(iv_sdate)、截止日期(iv_edate)、状态(iv_zt)-iFinD数据接口
    ensure_ifind_login()
    start_date=f"{start_year}{(start_month-1):02d}01"
    end_date=f"{end_year}{end_month:02d}31"
    data_result=THS_DR('p03316',f'iv_zsdm={index_code};iv_sdate={start_date};iv_edate={end_date};iv_zt=全部','p03316_f001:Y,p03316_f002:Y,p03316_f003:Y,p03316_f004:Y','format:dataframe')
    if data_result.errorcode != 0:
        print('error:{}'.format(data_result.errmsg))
        raise ValueError(f"{index_code}调取数据失败")
    else:
        data_df = data_result.data.rename(columns={
            "p03316_f001":"execute_day",
            "p03316_f002":"stock_code",
            "p03316_f003":"stock_name",
            "p03316_f004":"in_out"
        })
        data_df["execute_day"]=pd.to_datetime(data_df["execute_day"])
        data_df["declaration_day"]=data_df["execute_day"]-pd.Timedelta(days=17)
        data_df.to_excel(file_path,index=False)
        print(f"{file_path}文件读取备份成功")
        return data_df

def get_price_data(adjustments:pd.DataFrame,index_code:str,start_year:int,start_month:int,end_year:int,end_month:int)->(pd.DataFrame,pd.DataFrame):
    file_path_stock = f"{index_code}成分股股票开盘价_{start_year}{start_month:02d}-{end_year}{end_month:02d}.xlsx"
    file_path_index = f"{index_code}指数开盘价_{start_year}{start_month:02d}-{end_year}{end_month:02d}.xlsx"
    data_df_stock=pd.DataFrame()
    data_df_index=pd.DataFrame()
    if os.path.exists(file_path_stock) :
        data_df_stock = pd.read_excel(file_path_stock)
        print(f"{file_path_stock}文件已存在，读取备份文件")
    else:
        ensure_ifind_login()
        tar_data = adjustments
        grouped_data=tar_data.groupby(['declaration_day','execute_day'])['stock_code'].apply(list).reset_index()
        for _,row in grouped_data.iterrows():
            start_dt=row["declaration_day"].replace(day=1)
            end_dt=row["execute_day"]+pd.offsets.MonthEnd(0)
            data_result=THS_DS(row["stock_code"],'ths_open_price_stock','0','',start_dt.strftime("%Y-%m-%d"),end_dt.strftime("%Y-%m-%d"))
            if data_result.errorcode != 0:
                print('error:{}'.format(data_result.errmsg))
                raise ValueError(f"{row['stock_code']}调取数据失败")
            else:
                data_df=data_result.data.rename(columns={
                    "thscode":"stock_code",
                    "ths_open_price_stock":"open_price"
                })
                data_df["time"] = pd.to_datetime(data_df["time"])
                data_df_stock=pd.concat([data_df_stock,data_df],axis=0,ignore_index=True)
        data_df_stock.to_excel(file_path_stock,index=False)
        print(f"{file_path_stock}文件读取备份成功")
    if os.path.exists(file_path_index):
        data_df_index = pd.read_excel(file_path_index)
        print(f"{file_path_index}文件已存在，读取备份文件")
    else:
        ensure_ifind_login()
        tar_data = adjustments
        grouped_data = tar_data.groupby(['declaration_day', 'execute_day'])['stock_code'].apply(list).reset_index()
        for _, row in grouped_data.iterrows():
            start_dt = row["declaration_day"].replace(day=1)
            end_dt = row["execute_day"] + pd.offsets.MonthEnd(0)
            data_result=THS_DS(index_code, 'ths_open_price_index', '', '', start_dt.strftime("%Y-%m-%d"),end_dt.strftime("%Y-%m-%d"))
            if data_result.errorcode != 0:
                print('error:{}'.format(data_result.errmsg))
                raise ValueError(f"指数{start_dt.strftime('%Y%m%d')}-{end_dt.strftime('%Y%m%d')}调取数据失败")
            else:
                data_df=data_result.data.rename(columns={
                    "thscode":"index_code",
                    "ths_open_price_index":"open_price"
                })
                data_df["time"]=pd.to_datetime(data_df["time"])
                data_df_index=pd.concat([data_df_index,data_df],axis=0,ignore_index=True)
        data_df_index.to_excel(file_path_index,index=False)
        print(f"{file_path_index}文件读取备份成功")
    return data_df_stock, data_df_index

def get_stock_market_caps(adjustments:pd.DataFrame,index_code:str,start_year:int,start_month:int,end_year:int,end_month:int)->pd.DataFrame:
    file_path = f"{index_code}成分股市值_{start_year}{start_month:02d}-{end_year}{end_month:02d}.xlsx"
    if os.path.exists(file_path):
        data_df=pd.read_excel(file_path)
        print(f"{file_path}文件已存在，读取备份文件")
        return data_df
    data_df=pd.DataFrame()
    ensure_ifind_login()
    tar_data = adjustments
    grouped=tar_data.groupby(["declaration_day","execute_day"])["stock_code"].apply(list).reset_index()
    for _,row in grouped.iterrows():
        start_dt = row["declaration_day"].replace(day=1)
        end_dt = row["execute_day"] + pd.offsets.MonthEnd(0)
        data_result = THS_DS(row["stock_code"],'ths_market_value_stock;ths_ashare_mv_barring_ls_stock',';','', start_dt.strftime("%Y-%m-%d"),end_dt.strftime("%Y-%m-%d"))
        if data_result.errorcode != 0:
            print('error:{}'.format(data_result.errmsg))
            raise ValueError(f"成分股市值{start_dt.strftime('%Y%m%d')}-{end_dt.strftime('%Y%m%d')}调取数据失败")
        else:
            df = data_result.data.rename(columns={
                "thscode": "stock_code",
                "ths_market_value_stock": "market_cap",
                "ths_ashare_mv_barring_ls_stock":"A_float_cap"
            })
            data_df = pd.concat([data_df, df], axis=0, ignore_index=True)
    data_df["time"] = pd.to_datetime(data_df["time"])
    data_df.to_excel(file_path,index=False)
    print(f"{file_path}文件读取备份成功")
    return data_df
#============辅助函数=================
def get_trading_day(dt,N):
    ensure_ifind_login()
    return pd.to_datetime(THS_Date_Offset('212001', f'dateType:0,period:D,offset:{N},dateFormat:0,output:singledate', dt.strftime("%Y-%m-%d")).data)
def calculate_weights(method,stocks,buy_date,cap_data):
    """
    根据给定的权重计算方法计算每只股票在买入日的目标权重
    :param method: 目前有三个选择，分别为"equal","market_cap","A_float_cap"
    :param stocks: 股票代码列表
    :param buy_date: 买入日（datetime)
    :param cap_data: 市值数据DataFrame（列:'time','stock_code','market_cap','A_float_cap'）
    :return: {stock_code:weight}
    """
    weights={}
    if method=='equal':
        if len(stocks)==0:
            return {}
        w=1.0/len(stocks)
        for code in stocks:
            weights[code]=w
    elif method in ('market_cap','A_float_cap'):
        #总市值或流通市值加权
        # 先筛选出符合股票代码条件的数据,然后找到buy_date之前最近的一个交易日的数据
        filtered_data = cap_data[cap_data["stock_code"].isin(stocks)]
        cap_on_date = filtered_data[filtered_data["time"] < buy_date].sort_values("time", ascending=False).groupby("stock_code").head(1)
        cap_field=method
        total_cap=cap_on_date[cap_field].sum()
        if total_cap>0:
            w_series=cap_on_date.set_index('stock_code')[cap_field]/total_cap
            weights={code:float(w_series.get(code,0.0)) for code in stocks}
        else:
            weights={code:0.0 for code in stocks}
    else:
        raise ValueError(f"不支持的权重计算方式：{method}")
    return weights

def calculate_performance_metrics(dates,net_values,daily_returns,rf_rate=ANNUAL_RF,trading_days_per_year=TRADING_DAYS):
    """
    计算绩效指标：最大回撤、日波动率、年化收益率、夏普比率、卡玛比率
    """
    net_values=np.array(net_values,dtype=float)
    daily_returns=np.array(daily_returns,dtype=float)
    #最大回撤
    peak=-np.inf
    max_drawdown=0.0
    for v in net_values:
        if v>peak:
            peak=v
        if peak>0:
            drawdown=(peak-v)/peak
        else:
            drawdown=0
        if drawdown>max_drawdown:
            max_drawdown=drawdown
    #日波动率
    daily_volatility=float(np.std(daily_returns,ddof=0))
    #年化收益率
    total_days=len(dates)
    if total_days>1:
        total_return=net_values[-1]/net_values[0]-1.0
    else:
        total_return=0.0
    years=total_days/trading_days_per_year
    annual_return=((net_values[-1]/net_values[0])**(1/years)-1.0) if years>0 and net_values[0]>0 else 0.0
    #年化波动率
    annual_volatility=daily_volatility*np.sqrt(trading_days_per_year)
    #夏普比率
    sharpe_ratio=(annual_return-rf_rate)/annual_volatility if annual_volatility!=0 else 0.0
    #卡玛比率
    calmer_ratio=(annual_return/max_drawdown) if max_drawdown!=0 else 0.0
    return {
        "总收益率":total_return,
        "最大回撤":max_drawdown,
        "日波动率":daily_volatility,
        "年化收益率":annual_return,
        "夏普比率":sharpe_ratio,
        "卡玛比率":calmer_ratio
    }
#============回测函数=================
def backtest_index_adjust_strategy(index_code:str,start_year:int,start_month:int,end_year:int,end_month:int,N:int,M:int,weight_method:str='equal'):
    """
    回测沪深300指数策略
    :param index_code:指数代码（如："000300.SH"）
    :param start_year: 回测起始年
    :param start_month: 回测起始月
    :param end_year: 回测结束年
    :param end_month: 回测结束月
    :param N: 公告日后第N个交易日买入
    :param M: 执行日后第N个交易日买入
    :param weight_method: 权重分配方法，可选'equal','market_cap','A_flaot_cap'
    :return: net_values:DataFrame，包含回测期每天的净值（index为日期，列为净值）
            trades:List，每个元素为一次交易的摘要信息
    """
    #输出文件名称设定
    output_excel = f"{index_code}_backtest_{start_year}{start_month:02d}-{end_year}{end_month:02d}_N{N}_M{M}_{weight_method}.xlsx"
    output_chart = f"{index_code}_strategy_vs_index_{start_year}{start_month:02d}-{end_year}{end_month:02d}_N{N}_M{M}_{weight_method}.png"
    #原始数据读取及简单处理
    adjustments=get_index_adjustment(index_code,start_year,start_month,end_year,end_month)
    adjustments.sort_values(by="declaration_day", inplace=True)  # 按列排序
    in_adjustments=adjustments[adjustments["in_out"]=="纳入"]
    in_adjustments=in_adjustments.groupby("declaration_day").filter(lambda x:len(x)>=5)#todo主要目的时把临时调整给筛选出去
    stock_price,index_price=get_price_data(in_adjustments,index_code,start_year,start_month,end_year,end_month)
    index_price.sort_values('time',inplace=True)
    index_price.set_index('time',inplace=True)
    cap_data=get_stock_market_caps(in_adjustments,index_code,start_year,start_month,end_year,end_month)

    #构建事件列表
    events=[]
    trading_days = pd.DatetimeIndex([])
    position_details = []  # 用于存储每期持仓明细
    
    for decl_day, group in in_adjustments.groupby('declaration_day'):
        exec_day=group["execute_day"].iloc[0]
        stock_list=group['stock_code'].unique().tolist()
        open_pos_day=get_trading_day(decl_day,N)
        close_pos_day=get_trading_day(exec_day,M)
        #获取每期的交易日
        possible_dates=index_price.index[(index_price.index>=open_pos_day) & (index_price.index<=close_pos_day)]
        if len(possible_dates)==0:
            continue #如果某一期无交易日则直接跳过
        events.append({
            "declaration_day":decl_day,
            "execute_day":exec_day,
            "open_position_day":open_pos_day,
            "close_position_day":close_pos_day,
            "stocks":stock_list
        })
        trading_days=trading_days.union(possible_dates)
    events.sort(key=lambda x:x['open_position_day'])
    if not events:
        print("无调整事件可回测")
        return
    #初始化变量
    current_net=1.0#策略的当前净值
    index_net=1.0#基准指数的当前净值
    portfolio_shares={}#当前持仓股票以及份额
    last_index_price=None
    last_portfolio_value=None
    last_event_index_net = 1.0  # 记录上一个事件结束时的指数净值
    in_trading_period = False   # 标记是否在交易期间
    records=[]#保存每日记录
    close_position_dates = []  # 记录平仓日期
    #准备买入/卖出事件的快速查询字典
    buy_events={ev["open_position_day"]:ev for ev in events}
    sell_events={ev["close_position_day"]:ev for ev in events}

    #遍历每个交易日，计算当日指数收益、判断是否为开仓或平仓日，记录每日数据
    for date in trading_days:
        #基准指数当日表现更新
        if date in index_price.index:
            idx_price_today=float(index_price.loc[date,'open_price'])
        else:
            continue #若当天无指数数据则跳过
            
        #买入事件：在买入日按权重开盘建仓
        if date in buy_events:
            in_trading_period = True
            event=buy_events[date]
            stock_list=event['stocks']
            weights=calculate_weights(weight_method,stock_list,date,cap_data)
            
            # 记录本期持仓明细
            period_details = []
            initial_capital = 1000000  # 初始资金100万
            
            #按计算得到的权重买入股票（分配资金按开盘价计算持仓股数
            portfolio_shares.clear()
            for code,w in weights.items():
                price=stock_price.loc[(stock_price["stock_code"]==code)&(stock_price["time"]==date),"open_price"]
                if price.empty:
                    continue
                price=float(price.values[0])
                
                # 计算各项指标
                weight_pct = w * 100  # 转换为百分数
                market_value = initial_capital * w  # 市值
                shares_lot = round(market_value / (price * 100))  # 手数
                actual_value = shares_lot * price * 100  # 实际市值
                
                period_details.append({
                    "股票代码": code,
                    f"权重(%：{weight_method})": round(weight_pct, 2),
                    "目标市值": round(market_value, 2),
                    "开盘价": round(price, 2),
                    "购入手数": shares_lot,
                    "实际市值": round(actual_value, 2)
                })
                
                shares=(w*current_net)/price if price>0 else 0.0
                portfolio_shares[code]=shares
                
            # 将本期明细添加到总列表中
            position_details.append({
                "date": event["close_position_day"],
                "details": pd.DataFrame(period_details).set_index("股票代码")
            })
            
            last_portfolio_value=current_net#记录买入组合时的价值
            # 重置指数价格，以便从新事件开始计算
            last_index_price = idx_price_today
            index_net = last_event_index_net  # 使用上一个事件结束时的净值
            idx_return = 0.0

        else:
            if last_index_price is not None and in_trading_period:
                idx_return=idx_price_today/last_index_price-1.0 if last_index_price !=0 else 0
                index_net*=(1+idx_return)
            else:
                idx_return=0.0
        
        last_index_price=idx_price_today
        
        #更新持仓组合市值以及计算策略的当日收益
        if portfolio_shares:
            #计算当日持仓股票总市值（按照当日开盘价）
            portfolio_value=0.0
            for code, shares in portfolio_shares.items():
                price_today=stock_price.loc[(stock_price["stock_code"]==code) & (stock_price["time"]==date),"open_price"]
                if price_today.empty:
                    continue
                portfolio_value+=shares*float(price_today.values[0])
            #计算组合收益率（与上一日组合市值比较）
            if last_portfolio_value is not None and last_portfolio_value>0:
                strat_return=portfolio_value/last_portfolio_value-1.0
            else:
                strat_return=0.0
            current_net=portfolio_value
            last_portfolio_value=portfolio_value
        else:
            strat_return=0.0#无持仓则收益为0，净值不变
            
        #卖出事件：在执行日开盘卖出清仓(本质上是在清除portfolio_shares中的数据)
        if date in sell_events:
            in_trading_period = False
            portfolio_shares.clear()
            last_portfolio_value=None
            last_event_index_net = index_net  # 记录当前事件结束时的指数净值
            close_position_dates.append(date)  # 记录平仓日期
            
        #记录当天结果
        records.append({
            "date":date,
            "strategy_return":round(strat_return,6),
            "strategy_net":round(current_net,6),
            "index_return":round(idx_return,6),
            "index_net":round(index_net,6)
        })
    #转换每天记录为DataFrame
    result_df=pd.DataFrame(records)
    result_df.sort_values("date",inplace=True)
    result_df.reset_index(drop=True,inplace=True)
    #计算每期及整体策略与基准指标
    stats_list=[]
    for i,ev in enumerate(events,start=1):
        period_df=result_df[(result_df["date"]>=ev['open_position_day'])&(result_df["date"]<=ev["close_position_day"])]
        if period_df.empty:
            continue
        dates=period_df["date"].values
        strat_net_vals=period_df["strategy_net"].values
        strat_rets=period_df["strategy_return"].values
        strat_metrics=calculate_performance_metrics(dates,strat_net_vals,strat_rets)
        #基准指标（将该期首日净值归为1后计算）
        index_start_val=period_df["index_net"].iloc[0]
        index_net_vals=period_df["index_net"].values
        index_rets=period_df["index_return"].values
        index_net_vals_adj=index_net_vals/index_start_val if index_start_val!=0 else index_net_vals
        bench_metrics=calculate_performance_metrics(dates,index_net_vals_adj,index_rets)
        stats_list.append({
            "Period":f"Period{i}",
            "Start":ev["open_position_day"],
            "End":ev["close_position_day"],
            "策略总收益率":round(strat_metrics["总收益率"], 4),
            "策略最大回撤":round(strat_metrics["最大回撤"], 4),
            "策略年化收益率":round(strat_metrics["年化收益率"], 4),
            "策略日波动率":round(strat_metrics["日波动率"], 4),
            "策略夏普比率":round(strat_metrics["夏普比率"], 4),
            "策略卡玛比率":round(strat_metrics["卡玛比率"], 4),
            "基准总收益率": round(bench_metrics["总收益率"], 4),
            "基准最大回撤": round(bench_metrics["最大回撤"], 4),
            "基准年化收益率": round(bench_metrics["年化收益率"], 4),
            "基准日波动率": round(bench_metrics["日波动率"], 4),
            "基准夏普比率": round(bench_metrics["夏普比率"], 4),
            "基准卡玛比率": round(bench_metrics["卡玛比率"], 4),
        })
    #整段时间总体指标
    full_dates=result_df['date'].values
    strat_full_net=result_df["strategy_net"].values
    strat_full_ret=result_df["strategy_return"].values
    strat_full_metrics=calculate_performance_metrics(full_dates,strat_full_net,strat_full_ret)
    index_full_net=result_df['index_net'].values
    index_full_ret=result_df['index_return'].values
    bench_full_metrics=calculate_performance_metrics(full_dates,index_full_net,index_full_ret)
    stats_list.append({
        "Period": "Overall", 
        "Start": f"{start_year}{start_month}",
        "End": f"{end_year}{end_month}",
        "策略总收益率": round(strat_full_metrics["总收益率"], 4),
        "策略最大回撤": round(strat_full_metrics["最大回撤"], 4),
        "策略年化收益率": round(strat_full_metrics["年化收益率"], 4),
        "策略日波动率": round(strat_full_metrics["日波动率"], 4),
        "策略夏普比率": round(strat_full_metrics["夏普比率"], 4),
        "策略卡玛比率": round(strat_full_metrics["卡玛比率"], 4),
        "基准总收益率": round(bench_full_metrics["总收益率"], 4),
        "基准最大回撤": round(bench_full_metrics["最大回撤"], 4),
        "基准年化收益率": round(bench_full_metrics["年化收益率"], 4),
        "基准日波动率": round(bench_full_metrics["日波动率"], 4),
        "基准夏普比率": round(bench_full_metrics["夏普比率"], 4),
        "基准卡玛比率": round(bench_full_metrics["卡玛比率"], 4),
    })
    stats_df=pd.DataFrame(stats_list)
    period_returns=stats_df.iloc[:-1]["策略总收益率"]
    bench_returns=stats_df.iloc[:-1]["基准总收益率"]
    period_avg_retun=period_returns.mean()
    adj_annual_return=(1+period_avg_retun)**2-1
    win_vs_bench = float(np.mean(period_returns > bench_returns))
    win_positive=float(np.mean(period_returns>0))
    ws={
        "指标": [
            "胜率（超过基准）",
            "胜率（大于0）",
            "单次持仓平均收益率", 
            "调整年化收益率"
        ],
        "值": [
            round(win_vs_bench, 4),
            round(win_positive, 4),
            round(period_avg_retun, 4),
            round(adj_annual_return, 4)
        ]
    }
    win_df=pd.DataFrame(ws)
    
    # 输出到Excel
    with pd.ExcelWriter(output_excel) as writer:
        result_df.to_excel(writer,sheet_name="每日表现",index=False)
        stats_df.to_excel(writer,sheet_name="策略评价指标",index=False)
        win_df.to_excel(writer,sheet_name="胜率及调整指标",index=False)
        # 将每期持仓明细输出到Excel文件
        for period in position_details:
            # 获取原始数据
            details_df = period["details"]
            
            # 计算各列的总和并添加汇总行（除了股票代码列）
            sums = details_df.sum(numeric_only=True)
            sum_df = pd.DataFrame(sums).T
            sum_df.index = ['合计']
            
            # 将原数据和汇总行合并
            final_df = pd.concat([details_df, sum_df])
            period["details"]=final_df
            sheet_name = period["date"].strftime("%Y%m")
            final_df.to_excel(writer, sheet_name=sheet_name)

    #绘制策略月基准净值曲线图
    plt.figure(figsize=(18,6))
    
    # 绘制曲线
    plt.plot(result_df["date"].dt.strftime("%Y-%m-%d"), result_df["strategy_net"], label='策略')
    plt.plot(result_df['date'].dt.strftime("%Y-%m-%d"), result_df['index_net'], label=f"指数{index_code}")
    
    # 标注平仓点
    close_dates_str = [d.strftime("%Y-%m-%d") for d in close_position_dates]
    for close_date in close_dates_str:
        idx = result_df["date"].dt.strftime("%Y-%m-%d") == close_date
        if np.any(idx):
            plt.plot(close_date, result_df.loc[idx, "strategy_net"].iloc[0], 'r^', markersize=10, label='平仓点' if close_date == close_dates_str[0] else "")
            plt.plot(close_date, result_df.loc[idx, "index_net"].iloc[0], 'r^', markersize=10)
    
    # 设置x轴标签
    plt.xticks(result_df["date"].dt.strftime("%Y-%m-%d"), rotation=60, ha='right')
    # 调整布局以防止标签被切割
    plt.gcf().autofmt_xdate()
    
    plt.xlabel('日期')
    plt.ylabel('净值')
    plt.title(f"策略与指数净值曲线(N={N},M={M},权重={weight_method})")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_chart)
    plt.close()

    return result_df, stats_df, win_df, position_details

def parameter_optimization_study(index_code:str, start_year:int, start_month:int, end_year:int, end_month:int):
    """
    参数优化研究函数
    """
    # 参数范围设定
    N_range = range(0, 6)  # 买入时点范围：0到5
    M_range = range(-5, 1)  # 卖出时点范围：-5到0
    weight_methods = ['equal', 'market_cap', 'A_float_cap']
    
    # 存储结果
    results = []
    
    # 创建结果保存目录
    result_dir = "参数优化研究结果"
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
    
    # 遍历所有参数组合
    total_combinations = len(N_range) * len(M_range) * len(weight_methods)
    current_count = 0
    
    for n in N_range:
        for m in M_range:
            for weight_method in weight_methods:
                current_count += 1
                print(f"正在测试第 {current_count}/{total_combinations} 组参数: N={n}, M={m}, 权重方式={weight_method}")
                
                try:
                    # 运行回测并获取结果
                    result = backtest_index_adjust_strategy(index_code, start_year, start_month, end_year, end_month, 
                                                        n, m, weight_method)
                    
                    # 提取关键指标
                    stats_df = pd.DataFrame(result[1])  # 假设result[1]是stats_list
                    win_df = pd.DataFrame(result[2])    # 假设result[2]是win_stats
                    
                    overall_stats = stats_df[stats_df["Period"] == "Overall"].iloc[0]
                    
                    results.append({
                        'N': n,
                        'M': m,
                        'weight_method': weight_method,
                        '年化收益率': overall_stats['策略年化收益率'],
                        '最大回撤': overall_stats['策略最大回撤'],
                        '夏普比率': overall_stats['策略夏普比率'],
                        '超额收益率': overall_stats['策略年化收益率'] - overall_stats['基准年化收益率'],
                        '胜率(超基准)': win_df.iloc[0]['值'],
                        '胜率(正收益)': win_df.iloc[1]['值'],
                        '单次平均收益': win_df.iloc[2]['值']
                    })
                except Exception as e:
                    print(f"参数组合 N={n}, M={m}, weight_method={weight_method} 测试失败: {str(e)}")
                    continue
    
    # 将结果转换为DataFrame
    results_df = pd.DataFrame(results)
    
    # 保存结果
    results_df.to_excel(f"{result_dir}/参数优化结果汇总.xlsx", index=False)
    
    # 绘制参数优化可视化图表
    plot_optimization_results(results_df, result_dir)
    
    return results_df

def plot_optimization_results(results_df: pd.DataFrame, result_dir: str):
    """
    绘制参数优化结果的可视化图表
    """
    # 1. 为每种权重方法创建年化收益率热力图
    for weight_method in results_df['weight_method'].unique():
        df_subset = results_df[results_df['weight_method'] == weight_method]
        pivot_table = df_subset.pivot(index='N', columns='M', values='年化收益率')
        
        plt.figure(figsize=(12, 8))
        plt.imshow(pivot_table, cmap='RdYlGn', aspect='auto')
        plt.colorbar(label='年化收益率')
        
        # 添加数值标注
        for i in range(len(pivot_table.index)):
            for j in range(len(pivot_table.columns)):
                value = pivot_table.iloc[i, j]
                plt.text(j, i, f'{value:.2%}', ha='center', va='center')
        
        plt.title(f'{weight_method} - 年化收益率热力图 (N vs M)')
        plt.xlabel('M (卖出时点)')
        plt.ylabel('N (买入时点)')
        plt.xticks(range(len(pivot_table.columns)), pivot_table.columns)
        plt.yticks(range(len(pivot_table.index)), pivot_table.index)
        plt.tight_layout()
        plt.savefig(f"{result_dir}/热力图_{weight_method}_年化收益率.png")
        plt.close()
    
    # 2. 箱线图比较不同权重方法
    plt.figure(figsize=(15, 6))
    plt.boxplot([results_df[results_df['weight_method'] == method]['年化收益率'] 
                for method in results_df['weight_method'].unique()],
                labels=results_df['weight_method'].unique())
    plt.title('不同权重方法的年化收益率分布')
    plt.xticks(rotation=45)
    plt.ylabel('年化收益率')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"{result_dir}/权重方法比较_箱线图.png")
    plt.close()
    
    # 3. 参数敏感性分析
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # N的敏感性
    N_stats = results_df.groupby('N')['年化收益率'].agg(['mean', 'std']).reset_index()
    ax1.errorbar(N_stats['N'], N_stats['mean'], yerr=N_stats['std'], fmt='o-')
    ax1.set_title('买入时点(N)对年化收益率的影响')
    ax1.set_xlabel('N')
    ax1.set_ylabel('年化收益率')
    ax1.grid(True)
    
    # M的敏感性
    M_stats = results_df.groupby('M')['年化收益率'].agg(['mean', 'std']).reset_index()
    ax2.errorbar(M_stats['M'], M_stats['mean'], yerr=M_stats['std'], fmt='o-')
    ax2.set_title('卖出时点(M)对年化收益率的影响')
    ax2.set_xlabel('M')
    ax2.set_ylabel('年化收益率')
    ax2.grid(True)
    
    plt.tight_layout()
    plt.savefig(f"{result_dir}/参数敏感性分析.png")
    plt.close()
    
    # 4. 找出最优参数组合
    best_result = results_df.loc[results_df['年化收益率'].idxmax()]
    print("\n最优参数组合：")
    print(f"N (买入时点) = {best_result['N']}")
    print(f"M (卖出时点) = {best_result['M']}")
    print(f"权重方法 = {best_result['weight_method']}")
    print(f"年化收益率 = {best_result['年化收益率']:.2%}")
    print(f"夏普比率 = {best_result['夏普比率']:.2f}")
    print(f"最大回撤 = {best_result['最大回撤']:.2%}")
    
    # 5. 输出详细的参数组合排名
    print("\n参数组合排名（前10）：")
    top_10 = results_df.sort_values('年化收益率', ascending=False).head(10)
    for _, row in top_10.iterrows():
        print(f"N={row['N']}, M={row['M']}, 权重={row['weight_method']}: 年化收益率={row['年化收益率']:.2%}, 夏普比率={row['夏普比率']:.2f}")

#============主程序执行区==============
if __name__ == "__main__":
    #参数设置
    INDEX_CODE="000300.SH" #沪深300，可以输入list序列回测多支股票
    START_YEAR=2025#回测开始年份，回测最早总2014年开始，在此之前公布的日期规则有差异
    START_MONTH=2#回测开始月份
    END_YEAR=2025#回测结束年份
    END_MONTH=7#回测结束月份填写一个有31天的月份，是否是未来数据无所谓
    #运行下述任一功能时需要将另一个功能相关代码注释掉
    #=======功能1：进行单一回测===================
    # 回测参数设置
    N=1#以上半年调整为例，5月最后一个周五交易日为公布日，盘后公布。N=1则下一个交易日开盘价买入
    M=0#以上半年调整为例，6月第二个周五后一个交易日为执行日，开盘执行。M=-4意味着6月第二周周二卖出（若无节假日）
    WEIGHT_METHOD='market_cap'#权重分配方式，目前可选：'equal','market_cap','A_float_cap'

    backtest_index_adjust_strategy(INDEX_CODE,START_YEAR,START_MONTH,END_YEAR,END_MONTH,N,M,WEIGHT_METHOD)
    print("回测已完成")

    #======功能2：运行参数优化研究================
    # results_df = parameter_optimization_study(INDEX_CODE, START_YEAR, START_MONTH, END_YEAR, END_MONTH)
    # print("参数优化研究完成")
    #======功能3：合并持仓明细================
    # 设置回测参数
    # N = 1
    # M = -4
    #
    # # 存储不同权重方法的持仓数据
    # weight_methods = ['equal', 'market_cap', 'A_float_cap']
    # all_positions = {}
    #
    # # 获取每种权重方法的持仓数据
    # for method in weight_methods:
    #     _, _, _, position_details = backtest_index_adjust_strategy(
    #         index_code=INDEX_CODE,
    #         start_year=START_YEAR,
    #         start_month=START_MONTH,
    #         end_year=END_YEAR,
    #         end_month=END_MONTH,
    #         N=N,
    #         M=M,
    #         weight_method=method
    #     )
    #
    #     # 将每期数据按日期存储
    #     for period in position_details:
    #         date = period["date"]
    #         if date not in all_positions:
    #             all_positions[date] = {}
    #         # 存储所有数据列
    #         all_positions[date][method] = period["details"]
    #
    # # 合并并输出数据
    # output_file = f"{INDEX_CODE}_merged_details_{START_YEAR}{START_MONTH:02d}-{END_YEAR}{END_MONTH:02d}_N{N}_M{M}.xlsx"
    # with pd.ExcelWriter(output_file) as writer:
    #     for date in sorted(all_positions.keys()):
    #         # 重命名列以区分不同权重方法的数据
    #         dfs = []
    #         for method in weight_methods:
    #             df = all_positions[date][method].copy()
    #             # 重命名列，添加权重方法标识
    #             dfs.append(df)
    #
    #         # 合并该期所有方法的数据
    #         merged_df = pd.concat(dfs, axis=1)
    #
    #         # 保存到对应的sheet
    #         sheet_name = date.strftime("%Y%m")
    #         merged_df.to_excel(writer, sheet_name=sheet_name)

