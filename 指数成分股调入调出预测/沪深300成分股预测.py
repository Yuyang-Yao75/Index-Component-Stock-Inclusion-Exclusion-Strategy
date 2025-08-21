# Copyright (c) 2025 Yuyang Yao
# SPDX-License-Identifier: PolyForm-Noncommercial-1.0.0

from iFinDPy import *
import pandas as pd
import numpy as np
import datetime

# 定义登陆函数
def thslogin():
    thsLogin = THS_iFinDLogin('aaaaa', 'aaaaa')
    print(thsLogin)
    if thsLogin != 0:
        print("登陆失败")
    else:
        print("登陆成功")

def get_start_end_dt(year:int,month:int):
    '''
    根据输入的年份和月份（仅允许6月和12月）返回对应的start_dt和end_dt
    :param year:
    :param month:
    :return:
    '''
    if month==6:
        start_dt=datetime.datetime(year-1,5,1)
        end_dt=datetime.datetime(year,4,30)
    elif month==12:
        start_dt=datetime.datetime(year-1,11,1)
        end_dt=datetime.datetime(year,10,31)
    else:
        raise ValueError("month必须为6月或者12月")

    return start_dt,end_dt

#获取沪深300数据
def get_hs300_data(dt):
    # 获取交易日数据
    data_df = pd.DataFrame()
    data_result = THS_DR('p03291',f'date={dt.strftime("%Y%m%d")};blockname=001005290;iv_type=allcontract','p03291_f001:Y,p03291_f002:Y,p03291_f003:Y,p03291_f004:Y','format:dataframe')
    if data_result.errorcode != 0:
        print('error:{}'.format(data_result.errmsg))
    else:
        data_df = data_result.data
    return data_df

#获取A股对应窗口数据
def get_a_stock_price(start_dt,end_dt):
    '''
    核心功能是读取对应日期当天沪深A股所有相关目标信息
    :return:完整数据
    '''
    data_df = pd.DataFrame()
    data_etfopt = THS_DR('p03291', f'date={end_dt.strftime("%Y%m%d")};blockname=001005345;iv_type=allcontract',
                        'p03291_f001:Y,p03291_f002:Y,p03291_f003:Y,p03291_f004:Y', 'format:dataframe')
    if data_etfopt.errorcode != 0:
        print('error:{}'.format(data_etfopt.errmsg))
    else:
        optcode_sh_list = data_etfopt.data['p03291_f002'].tolist()
        data_result = THS_BD(optcode_sh_list,
                            'ths_stock_short_name_stock;ths_listedsector_stock;ths_listed_days_stock;ths_continuous_suspension_days_stock;ths_daily_avg_amt_int_stock;ths_daily_avg_mv_int_cache_stock',
                            f';;{end_dt.strftime("%Y%m%d")};{end_dt.strftime("%Y%m%d")};{start_dt.strftime("%Y%m%d")},{end_dt.strftime("%Y%m%d")};{start_dt.strftime("%Y%m%d")},{end_dt.strftime("%Y%m%d")}')
        if data_result.errorcode != 0:
            print('error:{}'.format(data_result.errmsg))
        else:
            data_df = data_result.data
    return data_df

def read_or_fetch(year:int,month:int,datatype='hs300')->pd.DataFrame:
    '''
    读取目标数据，如果没有的话通过iFind下载目标数据
    其中datatype只能是hs300或者a_stock
    '''
    df=pd.DataFrame()
    start_dt, end_dt = get_start_end_dt(year, month)
    file_path = f"{datatype}_{end_dt.strftime('%Y%m%d')}.xlsx"
    try:
        df=pd.read_excel(file_path)
        print(f"成功读取文件：{file_path}")
    except FileNotFoundError:
        print(f"未找到文件{file_path}，使用iFind进行调取")
        if datatype=='hs300':
            df=get_hs300_data(end_dt)
            df.to_excel(file_path, index=False)
            print(f"{file_path}已下载")
        elif datatype=='a_stock':
            df=get_a_stock_price(start_dt, end_dt)
            df.to_excel(file_path, index=False)
            print(f"{file_path}已下载")
        else:
            print("数据类型错误")

    return df


#获取个别股票目标窗口日市值及日交易额
def get_stock_market_data(symbol,start_dt,end_dt):
    # 获取交易日数据
    data_df = pd.DataFrame()
    data_result = THS_DS(symbol,'ths_market_value_stock;ths_amt_stock',';','Fill:Blank',f'{start_dt.strftime("%Y-%m-%d")}',f'{end_dt.strftime("%Y-%m-%d")}')
    if data_result.errorcode != 0:
        print('error:{}'.format(data_result.errmsg))
    else:
        data_df = data_result.data
    return data_df

def fill_missing_market_data(a_stock:pd.DataFrame,start_dt:datetime,end_dt:datetime)->pd.DataFrame:
    '''
    实现搜索a_stock中的缺失值并自动填充完整:
    1. 搜索a_stock中
    :param a_stock:
    :param start_dt:
    :param end_dt:
    :return:
    '''
    result=pd.DataFrame()
    if '备注' not in a_stock.columns:
        a_stock['备注']=''

    missing_mask=a_stock['ths_daily_avg_amt_int_stock'].isna() | a_stock['ths_daily_avg_mv_int_cache_stock'].isna()
    symbols=a_stock.loc[missing_mask,'thscode'].unique().tolist()
    a_stock.loc[missing_mask,'备注']=a_stock.loc[missing_mask,'备注'].fillna('').astype(str)+'数据缺失补充'
    missing_rows=missing_mask.sum()

    if missing_rows==0:
        print("未检测到缺失值，无需填补")
        return a_stock

    elif missing_rows<=10:
        orig_data=get_stock_market_data(symbols,start_dt, end_dt)
        orig_data.to_csv(f'download_stock_data{end_dt.strftime("%Y%m%d")}.csv',index=False)

        grouped=(
            orig_data
            .groupby('thscode')[['ths_market_value_stock','ths_amt_stock']]
            .mean()
            .rename(columns={
                'ths_market_value_stock':'fill_market_value',
                'ths_amt_stock':'fill_amt'
            })
        )

        result=a_stock.copy().set_index('thscode')

        result['ths_daily_avg_amt_int_stock']=(
            result['ths_daily_avg_amt_int_stock']
            .fillna(grouped['fill_amt'])
        )
        result['ths_daily_avg_mv_int_cache_stock']=(
            result['ths_daily_avg_mv_int_cache_stock']
            .fillna(grouped['fill_market_value'])
        )

        result=result.reset_index()
        result.to_excel(f"a_stock_{end_dt.strftime('%Y%m%d')}.xlsx",index=False)
        print("下载数据已保存，空缺值已填充完整")

    else:
        print("缺失值异常，请查看")
    result.dropna(subset=['ths_daily_avg_amt_int_stock','ths_daily_avg_mv_int_cache_stock'],how='any',inplace=True)
    return result

#识别所有A股数据中的新老股
def label_new_old(hs300,a_stock):
    is_old=a_stock['thscode'].isin(hs300['p03291_f002'])
    a_stock['新/老股']=np.where(is_old,'老股','新股')
    return a_stock

#根据指定列数据给输入Dataframe排名
def assign_rank(a_stock:pd.DataFrame,col:str)->pd.DataFrame:
    '''
    :param a_stock: A股完整数据
    :param col: 只可以是区间日均市值或者区间日均成交额
    :return: 返回更新过后的A股完整数据
    '''
    if col=='ths_daily_avg_amt_int_stock':
        rank_col='amt_rank'
    elif col=='ths_daily_avg_mv_int_cache_stock':
        rank_col='mv_rank'
    else:
        raise ValueError(f"不支持的列名：{col}")
    a_stock[rank_col]=a_stock[col].rank(method='dense',ascending=False).astype(int)

    return a_stock

def remove_st_and_star_st(a_stock:pd.DataFrame)->pd.DataFrame:
    '''
    删除股票名中包含ST和*ST的股票
    :param a_stock:
    :return:
    '''
    df=a_stock.copy()#防止数据丢失
    remove_mask=df['ths_stock_short_name_stock'].str.contains(r'^\*?ST',na=False)
    return df.loc[~remove_mask].copy()

def remove_science_innovation_under_one_year(a_stock:pd.DataFrame)->pd.DataFrame:
    '''
    移除上市一年以下的科创板公司
    :param a_stock:
    :return:
    '''
    df=a_stock.copy()
    remove_mask=((df['ths_listedsector_stock']=='科创板')| (df['ths_listedsector_stock']=='创业板'))&(df['ths_listed_days_stock']<365)
    return df.loc[~remove_mask].copy()

def remove_mainboard_under_quarter(a_stock:pd.DataFrame)->pd.DataFrame:
    '''
    移除上市一个季度以下的主板公司，在使用前需要先对市值进行排序
    :param a_stock:
    :return:
    '''
    df=a_stock.copy()
    if '备注' not in df.columns:
        df['备注']=''
    mask=(df['ths_listedsector_stock']=='主板')&(df['ths_listed_days_stock']<90)
    keep_mask=mask & (df['mv_rank']<=30)
    df.loc[keep_mask,'备注'] = df.loc[keep_mask,'备注'].fillna('').astype(str)+'主板未满1季度但总市值排名前30保留;'
    remove_mask=mask & (df['mv_rank']>30)
    return df.loc[~remove_mask].copy()

def remove_suspended_low_rank(a_stock:pd.DataFrame)->pd.DataFrame:
    '''
    标注老样本中连续停牌超过25天的股票
    移除新样本中正在停牌且市值排名低于300的股票
    :param a_stock:
    :return:
    '''
    df=a_stock.copy()
    if '备注' not in df.columns:
        df['备注']=''

    suspended=df['ths_continuous_suspension_days_stock']>0
    is_old=df['新/老股']=='老股'
    is_new = df['新/老股'] == '新股'

    old_remark=suspended & is_old & df['ths_continuous_suspension_days_stock']>=25
    df.loc[old_remark,'备注']=df.loc[old_remark,'备注'].fillna('').astype(str)+'停牌天数>=25;优先剔除;'

    new_remark=suspended & is_new & (df['mv_rank']<=300)
    df.loc[new_remark,'备注'] =df.loc[new_remark,'备注'].fillna('').astype(str)+'停牌但总市值排名前300保留;'

    new_remove=suspended & is_new & (df['mv_rank']>300)

    return df.loc[~new_remove].copy()

def filter_by_amt_percentile(a_stock:pd.DataFrame)->tuple[pd.DataFrame, pd.DataFrame]:
    '''
    按照市值排名百分位提出股票，前50%的股票一律保留；50%~60%的老股票保留，新股票剔除
    :param a_stock:
    :return:
    '''
    df=a_stock.copy()
    if '备注' not in df.columns:
        df['备注']=''

    N=len(df)
    thresh50=N*0.5
    thresh60=N*0.6
    remove_mask=(
        (df['amt_rank']>thresh60)
        |((df['amt_rank']>thresh50) & (df['amt_rank']<=thresh60) & (df['新/老股']=='新股'))
    )
    remove_old_mask= remove_mask &  (df['新/老股']=='老股')
    remove_old=df.loc[remove_old_mask,['thscode','ths_stock_short_name_stock','备注']]
    remove_old['纳入/剔除']='剔除'
    remove_old['备注'] =remove_old['备注'].fillna('').astype(str)+'因流动性不足而被剔除；'

    mid_old=(df['amt_rank']>thresh50) & (df['amt_rank']<=thresh60) & (df['新/老股']=='老股')
    df.loc[mid_old,'备注']=df.loc[mid_old,'备注'].fillna('').astype(str)+'市值排名50%-60%区间，老股保留;'

    return df.loc[~remove_mask].copy(),remove_old

def hs300_buffer(a_stock:pd.DataFrame)->pd.DataFrame:
    '''
    实现沪深300成分股缓冲机制
    1. 新股缓冲（mv_rank<=240 且新股）：
        如果候选数小于等于30支，则全部标记为成分股
        如果侯选庶大于30支，则取前30名标记为成分股，并对其与候选标注“优先纳入”
    2. 老股缓冲（mv_rank<=360 且老股）：
        全部标记为成分股
    3. 最终成分股调整到300支：
        如果超过300支
            先剔除备注中包含优先剔除字样的
            若仍大于300支，则按mv_rank保留前300支
        如果不足300支
            先将备注中含优先纳入的补入
            若仍然小于300支，则按mv_rank补足300支
    :param a_stock:
    :return:
    '''
    df=a_stock.copy()
    N=len(df)

    df['是否成分股']=0
    if '备注' not in df.columns:
        df['备注']=''

    mask_new=(df['mv_rank']<240) & (df['新/老股']=='新股')
    new_cands=df.loc[mask_new].sort_values('mv_rank')
    if len(new_cands)<=30:
        df.loc[new_cands.index,'是否成分股']=1
        df.loc[new_cands.index,'备注']=df.loc[new_cands.index,'备注'].fillna('').astype(str)+'新股市值强势优先被纳入'
    else:
        top30=new_cands.iloc[:30]
        rest=new_cands.iloc[30:]
        df.loc[top30.index,'是否成分股']=1
        df.loc[top30.index, '备注'] = df.loc[top30.index, '备注'].fillna('').astype(str) + '新股市值强势优先被纳入'
        df.loc[rest.index,'备注']=df.loc[rest.index,'备注'].fillna('').astype(str)+'优先纳入;'

    mask_old=(df['mv_rank']<=360)&(df['新/老股']=='老股')
    df.loc[mask_old,'是否成分股']=1

    cons=df[df['是否成分股']==1]
    count=len(cons)

    if count>300:
        remove_priority=cons[cons['备注'].str.contains("优先剔除",na=False)]
        df.loc[remove_priority.index,'是否成分股']=0

        cons=df[df['是否成分股']==1]
        if len(cons)>300:
            keep=cons.sort_values('mv_rank').iloc[:300]
            df['是否成分股']=0
            df.loc[keep.index,'是否成分股']=1
        else:
            print("特殊情况")#待完善

    elif count<300:
        add_priority=df[(df['备注'].str.contains("优先纳入",na=False)) & (df['是否成分股']==0)]
        slots=300-len(df[df['是否成分股']==1])
        to_add=add_priority.sort_values('mv_rank').iloc[:slots]
        df.loc[to_add.index,'是否成分股']=1

        cons = df[df['是否成分股'] == 1]
        if len(cons)<300:
            remaining=df[df['是否成分股']==0].sort_values('mv_rank').iloc[:(300-len(cons))]
            df.loc[remaining.index,'是否成分股']=1
        else:
            print("特殊情况")

    return df.copy()

def label_inclusion(a_stock:pd.DataFrame)->pd.DataFrame:
    '''
    根据新老股票和是否成分股，为每列打上标签
    :param a_stock:
    :return:
    '''
    df=a_stock.copy()
    df['纳入/剔除']='暂不考虑'

    mask_new_in=(df['新/老股']=='新股') & (df['是否成分股']==1)
    df.loc[mask_new_in,'纳入/剔除']='纳入'

    mask_old_keep=(df['新/老股']=='老股') & (df['是否成分股']==1)
    df.loc[mask_old_keep,'纳入/剔除']='保持不变'

    mask_old_out = (df['新/老股'] == '老股') & (df['是否成分股'] == 0)
    df.loc[mask_old_out, '纳入/剔除'] = '剔除'
    df.loc[mask_old_out,'备注'] =df.loc[mask_old_out,'备注'].fillna('').astype(str)+'因市值排名靠后被剔除'

    return df.copy()

def update_mkt_value(a_stock,start_dt,end_dt):
    df=a_stock.copy()
    target_mask = (df["新/老股"] == '老股') | ((df["新/老股"] == '新股') & (df["mv_rank"] <= 300))
    df=df[target_mask].drop(columns=['ths_daily_avg_mv_int_cache_stock'])
    target_list=df["thscode"].tolist()
    data_result=THS_DS(target_list, 'ths_ashare_mv_include_ls_stock', '', '', start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))
    if data_result.errorcode != 0:
        print('error:{}'.format(data_result.errmsg))
    else:
        data_df = data_result.data
        data_df.to_excel("A股市值.xlsx",index=False)
        target_result=(
            data_df.groupby('thscode')['ths_ashare_mv_include_ls_stock']
            .mean()
            .reset_index()
            .rename(columns={'ths_ashare_mv_include_ls_stock':'ths_daily_avg_mv_int_cache_stock'})
        )
        target_result=pd.merge(
            df,
            target_result,
            on='thscode',
            how='left'
        )
        return target_result


def calculate_accuracy(a_stock:pd.DataFrame,out_in:pd.DataFrame,remove_old:pd.DataFrame, year:int,month:int):
    '''
    函数能够保存预测结果和真实结果至同一张表格；同时输出预测准确率
    :param a_stock:
    :param out_in:
    :param year:
    :param month:
    :return:
    '''
    mask=a_stock['纳入/剔除'].isin(['纳入','剔除'])
    df_pred=a_stock.loc[mask,['thscode','ths_stock_short_name_stock','纳入/剔除','备注']].copy()
    df_pred=pd.concat([df_pred,remove_old])

    out_in['日期'] = pd.to_datetime(out_in['日期'], errors='coerce')
    df_actual=out_in.loc[
        (out_in['日期'].dt.year==year)&(out_in['日期'].dt.month==month),
        ['证券代码','证券名称','调整类型']
    ].copy()

    actual_in = set(df_actual.loc[df_actual['调整类型']=='纳入','证券代码'])
    actual_out = set(df_actual.loc[df_actual['调整类型'] == '剔除', '证券代码'])

    def pred_label(row):
        code=row['thscode']
        if row['纳入/剔除']=='纳入':
            return '准确' if code in actual_in else '不准确'
        else:
            return '准确' if code in actual_out else '不准确'
    df_pred['预测准确']=df_pred.apply(pred_label,axis=1)

    pred_in = set(df_pred.loc[df_pred['纳入/剔除'] == '纳入', 'thscode'])
    pred_out = set(df_pred.loc[df_pred['纳入/剔除'] == '剔除', 'thscode'])

    def actual_label(row):
        code = row['证券代码']
        if row['调整类型'] == '纳入':
            return '准确' if code in pred_in else '不准确'
        else:
            return '准确' if code in pred_out else '不准确'

    df_actual['预测准确'] = df_actual.apply(actual_label, axis=1)

    predicted_in_total=df_pred[df_pred['纳入/剔除']=='纳入'].shape[0]
    predicted_out_total=df_pred[df_pred['纳入/剔除']=='剔除'].shape[0]
    actual_in_total=df_actual[df_actual['调整类型']=='纳入'].shape[0]
    hit_in=df_pred[(df_pred['纳入/剔除']=='纳入') & (df_pred['预测准确']=='准确')].shape[0]
    hit_out=df_pred[(df_pred['纳入/剔除'] == '剔除') & (df_pred['预测准确']=='准确')].shape[0]

    print(f'预测调入{predicted_in_total}，实际调入{actual_in_total}，命中{hit_in}个；预测调出{predicted_out_total}，实际调出{actual_in_total}，命中{hit_out}')

    filename=f'预测结果_{year}_{month:02d}_new.xlsx'
    with pd.ExcelWriter(filename) as writer:
        df_pred.to_excel(writer,sheet_name='预测结果',index=False)
        df_actual.to_excel(writer,sheet_name='实际情况',index=False)
    print(f'已生成文件：{filename}')

def main():
    thslogin()
    year=2025
    month=6
    #month只能填写6或者12，如果填写别的数会导致get_start_end_dt报错。
    start_dt, end_dt = get_start_end_dt(year, month)

    #基础数据提取及数据预处理
    out_in=pd.read_excel("沪深300(000300.SH)-历史成分股.xlsx")
    hs300_components = read_or_fetch(year,month,'hs300')
    a_stock = read_or_fetch(year,month,'a_stock')

    #数据预处理
    a_stock_nm = fill_missing_market_data(a_stock, start_dt, end_dt)
    a_stock_lno = label_new_old(hs300_components,a_stock_nm)

    #样本空间构建
    print('正在将那进行样本空间构建')
    a_stock_nst = remove_st_and_star_st(a_stock_lno)
    a_stock_siuo = remove_science_innovation_under_one_year(a_stock_nst)
    a_stock_rkst1 = assign_rank(a_stock_siuo, 'ths_daily_avg_amt_int_stock')
    a_stock_rkmv1 = assign_rank(a_stock_rkst1, 'ths_daily_avg_mv_int_cache_stock')
    a_stock_muq = remove_mainboard_under_quarter(a_stock_rkmv1)
    a_stock_rkst2 = assign_rank(a_stock_muq, 'ths_daily_avg_amt_int_stock')
    a_stock_rkmv2 = assign_rank(a_stock_rkst2, 'ths_daily_avg_mv_int_cache_stock')
    a_stock_rslr = remove_suspended_low_rank(a_stock_rkmv2)
    a_stock_rkst3 = assign_rank(a_stock_rslr, 'ths_daily_avg_amt_int_stock')
    a_stock_rkmv3 = assign_rank(a_stock_rkst3, 'ths_daily_avg_mv_int_cache_stock')

    #数据筛选
    print('正在进行数据筛选')
    a_stock_fbmp, remove_old= filter_by_amt_percentile(a_stock_rkmv3)
    a_stock_rkst4 = assign_rank(a_stock_fbmp, 'ths_daily_avg_amt_int_stock')
    a_stock_rkmv4 = assign_rank(a_stock_rkst4, 'ths_daily_avg_mv_int_cache_stock')
    a_stock_c=update_mkt_value(a_stock_rkmv4,start_dt,end_dt)
    a_stock_c.to_excel("flag.xlsx",index=False)
    a_stock_rkst5 = assign_rank(a_stock_c, 'ths_daily_avg_amt_int_stock')
    a_stock_rkmv5 = assign_rank(a_stock_rkst5, 'ths_daily_avg_mv_int_cache_stock')
    a_stock_buffer = hs300_buffer(a_stock_rkmv5)

    #标签更新
    a_stock_li=label_inclusion(a_stock_buffer)
    a_stock_li.to_excel(f"a_stock_{end_dt.strftime('%Y%m%d')}处理数据_new.xlsx")
    print('已保存a_stock处理文件')
    calculate_accuracy(a_stock_li,out_in,remove_old,year,month)


if __name__=='__main__':
    main()