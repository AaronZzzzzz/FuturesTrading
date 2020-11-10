# -*- coding: utf-8 -*-
import datetime
import warnings
from collections import defaultdict

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from rqdatac.client import get_client
from rqdatac.decorators import export_as_api, ttl_cache
from rqdatac.services.basic import Instrument
from rqdatac.share.errors import RQDataError
from rqdatac.utils import to_date, pd_version, is_panel_removed
from rqdatac.validators import (
    ensure_date_str,
    ensure_date_int,
    ensure_list_of_string,
    ensure_string_in,
    check_items_in_container,
    ensure_trading_date,
    ensure_date_range,
    raise_for_no_panel,
    check_type
)


def _get_instrument(order_book_id, market="cn"):
    d = _all_instruments_dict(market)
    return d.get(order_book_id)


@ttl_cache(3 * 3600)
def _all_instruments_list(market):
    return [Instrument(i) for i in get_client().execute("fund.all_instruments", market)]


@ttl_cache(3 * 3600)
def _all_instruments_dict(market):
    all_list = _all_instruments_list(market)
    d = {}
    for i in all_list:
        d[i.order_book_id] = i
        d[i.symbol] = i

    return d


@ttl_cache(3 * 3600)
def _all_main_code_map(market):
    relation_documents = get_client().execute("fund.get_related_code", None,
                                              relation_types=["multi_share", "parent_and_child"],
                                              market=market)
    if not relation_documents:
        return {}
    relation_df = pd.DataFrame(relation_documents).set_index("related_id")

    related_dict = {}

    # 先后添加平级基金的主次关系, 母子基金的主次关系。母子关系优先级比平级关系高
    for relation_type in ["multi_share", "parent_and_child"]:
        df = relation_df[relation_df.type == relation_type]
        if not df.empty:
            related_dict.update(df['order_book_id'].to_dict())

    return related_dict


def _add_children_data(df, order_book_ids, main_code_map):
    """添加次级基金数据， 次级基金与主基金的数据一致

    :param df: 母基金api数据，index.name = ["order_book_id", "datetime"]
    :param order_book_ids: 需要返回的order_book_id，包括主基金，次级基金
    :param main_code_map: 次级基金和主基金的映射
    :return:
    """

    children_map = defaultdict(list)
    for obid in order_book_ids:
        parent_id = main_code_map.get(obid)
        if parent_id and parent_id in df.index.levels[0]:
            children_map[parent_id].append(obid)

    children_groups = defaultdict(list)
    for pid, cids in children_map.items():
        for i, cid in enumerate(cids):
            children_groups[i].append(cid)

    # 分批添加分级基金
    for children_group in children_groups.values():
        parents = [main_code_map.get(cid) for cid in children_group]
        children_df = df.reindex(parents, level=0)
        children_df.index = pd.MultiIndex([children_group, children_df.index.levels[1]],
                                          codes=children_df.index.codes)
        df = pd.concat([df, children_df])
    return df.reindex(order_book_ids, level=0)


def ensure_fund(ob, market="cn"):
    try:
        return _all_instruments_dict(market)[ob].order_book_id
    except KeyError:
        raise ValueError("invalid fund order book id: {}".format(ob))


@export_as_api(namespace="fund")
def all_instruments(date=None, market="cn"):
    """获取全部基金信息

    :param date: 该参数表示过滤掉 listed_date 晚于当日的基金，默认为 None，表示获取全部基金信息，不进行过滤。 (Default value = None)
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    a = _all_instruments_list(market)
    if date is not None:
        date = ensure_date_str(date)
        a = [i for i in a if i.listed_date < date]

    df = pd.DataFrame(
        [
            [
                v.order_book_id,
                v.establishment_date,
                v.listed_date,
                v.transition_time,
                v.amc,
                v.symbol,
                v.fund_type,
                v.fund_manager,
                v.latest_size,
                v.benchmark,
                v.accrued_daily,
                v.de_listed_date,
                v.stop_date,
                v.exchange,
                v.round_lot,
            ]
            for v in a
        ],
        columns=[
            "order_book_id",
            "establishment_date",
            "listed_date",
            "transition_time",
            "amc",
            "symbol",
            "fund_type",
            "fund_manager",
            "latest_size",
            "benchmark",
            "accrued_daily",
            "de_listed_date",
            "stop_date",
            "exchange",
            "round_lot",
        ],
    )
    df = df[6 == df["order_book_id"].str.len()]
    df = df.drop_duplicates().sort_values(['order_book_id', 'listed_date'])
    return df.reset_index(drop=True)


@export_as_api(namespace="fund")
def instruments(order_book_ids, market="cn"):
    """获取基金详细信息

    :param order_book_ids: 基金代码，str 或 list of str
    :param market:  (Default value = "cn")
    :returns: Instrument object or list of Instrument object
            取决于参数是一个 order_book_id 还是多个 order_book_id

    """
    order_book_ids = ensure_list_of_string(order_book_ids)
    if len(order_book_ids) == 1:
        return _get_instrument(order_book_ids[0])
    d = _all_instruments_dict(market)
    return [d[i] for i in order_book_ids if i in d]


NAV_FIELDS = (
    "acc_net_value",
    "unit_net_value",
    "subscribe_status",
    "redeem_status",
    "change_rate",
    "adjusted_net_value",
    "daily_profit",
    "weekly_yield",
)


@export_as_api(namespace="fund")
def get_nav(order_book_ids, start_date=None, end_date=None, fields=None, expect_df=False, market="cn"):
    """获取基金净值数据

    :param order_book_ids: 基金代码，str 或 list of str
    :param start_date: 开始日期 (Default value = None)
    :param end_date: 结束日期 (Default value = None)
    :param fields: str or list of str，例如：'acc_net_value', 'unit_net_value',
                    'subscribe_status', 'redeem_status', 'change_rate' (Default value = None)
    :param expect_df: 返回 MultiIndex DataFrame (Default value = False)
    :param market:  (Default value = "cn")
    :returns: DataFrame or Series or Panel

    """
    order_book_ids = ensure_list_of_string(order_book_ids)

    if len(order_book_ids) > 1:
        ins = instruments(order_book_ids, market=market)
        if len(set([i.accrued_daily for i in ins])) > 1:
            raise RQDataError(
                "ERROR: Only support single accrued type when getting nav at one time."
            )

    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)

    if fields is not None:
        fields = ensure_list_of_string(fields)
        for f in fields:
            if f not in NAV_FIELDS:
                raise ValueError("invalid field: {}".format(f))
    else:
        fields = NAV_FIELDS

    result = get_client().execute(
        "fund.get_nav", order_book_ids, start_date, end_date, fields, market=market
    )
    if not result:
        return
    result = pd.DataFrame(result)

    if not is_panel_removed and not expect_df:
        result = result.set_index(["datetime", "order_book_id"])
        result.reindex(columns=fields)
        result = result.to_panel()
        if len(order_book_ids) == 1:
            result = result.minor_xs(order_book_ids[0])
        if len(fields) == 1:
            return result[fields[0]]
        if len(order_book_ids) != 1 and len(fields) != 1:
            warnings.warn("Panel is removed after pandas version 0.25.0."
                          " the default value of 'expect_df' will change to True in the future.")
        return result
    else:
        result.sort_values(["order_book_id", "datetime"], inplace=True)
        result.set_index(["order_book_id", "datetime"], inplace=True)
        result.reindex(columns=fields)
        if expect_df:
            return result

        if len(order_book_ids) != 1 and len(fields) != 1:
            raise_for_no_panel()

        if len(order_book_ids) == 1:
            result.reset_index(level=0, inplace=True, drop=True)
            if len(fields) == 1:
                result = result[fields[0]]
        else:
            field = result.columns[0]
            result = result.unstack(0)[field]

        return result


@export_as_api(namespace="fund")
def get_holdings(order_book_ids, date=None, market="cn", **kwargs):
    """获取距离指定日期最近发布的基金持仓信息

    :param order_book_ids: 基金代码，str 或 list of str
    :param date: 日期，为空则返回所有持仓 (Default value = None)
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids)
    if date is not None:
        date = ensure_date_int(date)
        start_date = end_date = None
    else:
        if "start_date" in kwargs and "end_date" in kwargs:
            start_date = ensure_date_int(kwargs.pop("start_date"))
            end_date = ensure_date_int(kwargs.pop("end_date"))
        elif "start_date" in kwargs or "end_date" in kwargs:
            raise ValueError('please ensure start_date and end_date exist')
        else:
            start_date = end_date = None
    if kwargs:
        raise ValueError('unknown kwargs: {}'.format(kwargs))

    df = get_client().execute("fund.get_holdings_v4", order_book_ids, date, start_date, end_date, market=market)
    if not df:
        return

    df = pd.DataFrame(data=df)
    fields = ["order_book_id", "type", "weight", "shares", "market_value", "symbol"]
    if "category" in df.columns:
        fields += ["category"]
    if "region" in df.columns:
        fields += ["region"]

    df.sort_values(["fund_id", "date", "type", "order_book_id"], inplace=True)
    df.set_index(["fund_id", "date"], inplace=True)
    return df[fields]


@export_as_api(namespace="fund")
def get_split(order_book_ids, market="cn"):
    """获取基金拆分信息

    :param order_book_ids: 基金代码，str 或 list of str
    :param market:  (Default value = "cn")
    :returns: DataFrame
    """
    order_book_ids = ensure_list_of_string(order_book_ids)
    data = get_client().execute("fund.get_split", order_book_ids, market=market)
    if not data:
        return
    df = pd.DataFrame(data, columns=["order_book_id", "split_ratio", "ex_dividend_date"])
    return df.set_index(["order_book_id", "ex_dividend_date"]).sort_index()


@export_as_api(namespace="fund")
def get_dividend(order_book_ids, market="cn"):
    """获取基金分红信息

    :param order_book_ids: 基金代码，str 或 list of str
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids)
    data = get_client().execute("fund.get_dividend", order_book_ids, market=market)
    if not data:
        return

    df = pd.DataFrame(
        data,
        columns=["order_book_id", "book_closure_date", "payable_date", "dividend_before_tax", "ex_dividend_date"],
    )
    return df.set_index(["order_book_id", "ex_dividend_date"]).sort_index()


@export_as_api(namespace="fund")
def get_manager(order_book_ids, expect_df=False, market="cn"):
    """获取基金经理信息

    :param order_book_ids: 基金代码，str 或 list of str
    :param market:  (Default value = "cn")
    :param expect_df: 返回 MultiIndex DataFrame (Default value = False)
    :returns: DataFrame or Panel

    """
    order_book_ids = ensure_list_of_string(order_book_ids)

    docs = get_client().execute("fund.get_manager", order_book_ids, market=market)
    if not docs:
        return

    if not expect_df and not is_panel_removed:
        data = {}
        fields = []
        for doc in docs:
            data.setdefault(doc["order_book_id"], []).append(doc)
            doc.pop('order_book_id')
            if len(fields) < len(doc.keys()):
                fields = list(doc.keys())
        array = np.full((len(fields), max([len(v) for v in data.values()]), len(order_book_ids)), None)
        for i in range(max([len(v) for v in data.values()])):
            for j, order_book_id in enumerate(order_book_ids):
                try:
                    doc = data.setdefault(order_book_id, [])[i]
                except IndexError:
                    doc = None

                for k, f in enumerate(fields):
                    v = None if doc is None else doc[f]
                    array[k, i, j] = v
        result = pd.Panel(data=array, items=fields, minor_axis=order_book_ids)
        if len(order_book_ids) == 1:
            return result.minor_xs(order_book_ids[0])
        warnings.warn("Panel is removed after pandas version 0.25.0."
                      " the default value of 'expect_df' will change to True in the future.")
        return result
    else:
        df = pd.DataFrame(docs)
        df.sort_values(["order_book_id", "start_date"], inplace=True)
        df.set_index(["order_book_id", "id"], inplace=True)
        if expect_df:
            return df
        if len(order_book_ids) == 1:
            return df.reset_index(level=0, drop=True)

        raise_for_no_panel()


@export_as_api(namespace="fund")
def get_manager_info(manager_id, fields=None, market="cn"):
    """获取基金经理个人信息

    :param manager: 可以使用人员编码（如'101000002'）或姓名（如'江辉'），str 或 list of str
    :param fields: str or list of str，例如："gender", "region", (Default value = None)
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    manager_id = ensure_list_of_string(manager_id)
    # 检查manager中是否同时有人员编码或姓名
    if len(set(map(lambda x: x.isdigit(), manager_id))) > 1:
        raise ValueError("couldn't get manager_id and name at the same time")

    manager_fields = ["gender", "region", "birthdate", "education", "practice_date", "experience_time", "background"]
    if fields is not None:
        fields = ensure_list_of_string(fields, "fields")
        check_items_in_container(fields, manager_fields, "fields")
    else:
        fields = manager_fields
    result = get_client().execute("fund.get_manager_info", manager_id, fields, market=market)
    if not result:
        warnings.warn("manager_id/manager_name does not exist")
        return

    df = pd.DataFrame(result).set_index("id")
    fields.insert(0, "chinesename")
    df.sort_index(inplace=True)
    return df[fields]


@export_as_api(namespace="fund")
def get_asset_allocation(order_book_ids, date=None, market="cn"):
    """获取指定日期最近发布的基金资产配置信息

    :param order_book_ids: 基金代码，str 或 list of str
    :param date: 日期，为空则返回所有时间段的数据 (Default value = None)
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids, market)
    if date is not None:
        date = ensure_date_int(date)

    main_code_map = _all_main_code_map(market)
    parent_ids = [main_code_map.get(obid, obid) for obid in order_book_ids]

    df = get_client().execute("fund.get_asset_allocation_v2", parent_ids, date, market=market)
    if not df:
        return

    columns = ["order_book_id", "datetime", "stock", "bond", "fund", "cash", "other", "nav", "net_asset", "total_asset"]
    df = pd.DataFrame(df, columns=columns)
    df["datetime"] = pd.to_datetime(df["datetime"])
    warnings.warn("'nav' is deprecated. Please use 'net_asset' instead")
    return _add_children_data(df.set_index(["order_book_id", "datetime"]), order_book_ids, main_code_map)


@export_as_api(namespace="fund")
def get_ratings(order_book_ids, date=None, market="cn"):
    """获取距离指定日期最近发布的基金评级信息

    :param order_book_ids: 基金代码，str 或 list of str
    :param date: 日期，为空则返回所有时间段的数据 (Default value = None)
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids)
    if date is not None:
        date = ensure_date_int(date)

    df = get_client().execute("fund.get_ratings_v2", order_book_ids, date, market=market)
    if not df:
        return

    df = pd.DataFrame(df, columns=["order_book_id", "datetime", "zs", "sh3", "sh5", "jajx"])
    df.sort_values(["order_book_id", "datetime"], inplace=True)
    if date is not None:
        df.drop_duplicates(subset=['order_book_id'], keep='last', inplace=True)
    df.set_index(["order_book_id", "datetime"], inplace=True)
    df.fillna(np.nan, inplace=True)
    return df


@export_as_api(namespace="fund")
def get_units_change(order_book_ids, date=None, market="cn"):
    """获取距离指定日期最近发布的基金认购赎回信息

    :param order_book_ids: 基金代码，str 或 list of str
    :param date: 日期，为空则返回所有时间段的数据 (Default value = None)
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids, market)
    if date is not None:
        date = ensure_date_int(date)

    df = get_client().execute("fund.get_units_change_v2", order_book_ids, date, market=market)
    if not df:
        return

    df = pd.DataFrame(df, columns=["subscribe_units", "redeem_units", "units", "order_book_id", "datetime"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.set_index(["order_book_id", "datetime"]).sort_index()


@export_as_api(namespace="fund")
def get_ex_factor(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取公募基金复权因子

    :param order_book_ids: 基金代码，str 或 list of str
    :param start_date: 如 '2013-01-04' (Default value = None)
    :param end_date: 如 '2014-01-04' (Default value = None)
    :param market:  (Default value = "cn")
    :returns: 如果有数据，返回一个DataFrame, 否则返回None

    """
    order_book_ids = ensure_list_of_string(order_book_ids)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)

    if start_date and end_date and end_date < start_date:
        raise ValueError()
    data = get_client().execute("fund.get_ex_factor", order_book_ids, start_date, end_date, market=market)
    if not data:
        return

    df = pd.DataFrame(
        data,
        columns=["order_book_id", "ex_factor", "ex_cum_factor", "ex_end_date", "ex_date"]
    )
    return df.set_index(["order_book_id", "ex_date"]).sort_index()


@export_as_api(namespace="fund")
def get_industry_allocation(order_book_ids, date=None, market="cn"):
    """获取指定日期最近发布的基金行业配置信息

    :param order_book_ids: 基金代码，str 或 list of str
    :param date: 日期，为空则返回所有时间段的数据 (Default value = None)
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids, market)
    if date is not None:
        date = ensure_date_int(date)

    df = get_client().execute("fund.get_industry_allocation_v2", order_book_ids, date, market=market)
    if not df:
        return

    df = pd.DataFrame(df, columns=["industry", "weight", "market_value", "order_book_id", "datetime"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.set_index(["order_book_id", "datetime"]).sort_index()


@export_as_api(namespace="fund")
def get_indicators(order_book_ids, start_date=None, end_date=None, fields=None, rule="deprecated", market="cn"):
    """获取基金衍生数据

    :param order_book_ids: 基金代码，str 或 list of str
    :param start_date: 开始日期 (Default value = None)
    :param end_date: 结束日期 (Default value = None)
    :param fields: str or list of str (Default value = None)
    :param rule:  str, 可选：["ricequant", "deprecated"] (Default value = "deprecated")
    :param market:  (Default value = "cn")
    :returns: DataFrame or Series

    """
    order_book_ids = ensure_list_of_string(order_book_ids)
    check_items_in_container(rule, ["ricequant", "deprecated"], "rule")
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)

    if fields is not None:
        fields = ensure_list_of_string(fields, "fields")
    result = get_client().execute("fund.get_indicators", order_book_ids, start_date, end_date, fields, rule=rule,
                                  market=market)
    if not result:
        return

    df = pd.DataFrame(result).set_index(keys=["order_book_id", "datetime"])
    df.sort_index(inplace=True)
    if fields is not None:
        return df[fields]
    return df


@export_as_api(namespace="fund")
def get_snapshot(order_book_ids, fields=None, market="cn", rule="deprecated"):
    """获取基金的最新数据

    :param order_book_ids: 基金代码，str 或 list of str
    :param fields: str or list of str，例如："last_week_return", "subscribe_status", (Default value = None)
    :param market:  (Default value = "cn")
    :param rule:  str, 可选：["ricequant", "deprecated"] (Default value = "deprecated")
    :returns: DataFrame or Series

    """
    order_book_ids = ensure_list_of_string(order_book_ids)
    check_items_in_container(rule, ["ricequant", "deprecated"], "rule")

    if fields is not None:
        fields = ensure_list_of_string(fields, "fields")
    result = get_client().execute("fund.get_snapshot", order_book_ids, fields, rule=rule, market=market)
    if not result:
        return

    if rule == "ricequant":
        df = pd.DataFrame(result).set_index(["order_book_id", "latest_date"])
        df.index.names = ['order_book_id', 'datetime']
    else:
        df = pd.DataFrame(result).set_index("order_book_id")
    df.sort_index(inplace=True)
    if fields is not None:
        return df[fields]
    return df


@export_as_api(namespace="fund")
def get_related_code(order_book_ids, market="cn"):
    """get_related_code

    :param order_book_ids: 基金代码，str 或 list of str
    :param market:  (Default value = "cn")
    :returns: DataFrame or Series

    """
    order_book_ids = ensure_list_of_string(order_book_ids)

    result = get_client().execute("fund.get_related_code", order_book_ids, market=market)
    if not result:
        return
    df = pd.DataFrame(result, columns=["order_book_id", "related_id", "type", "effective_date", "cancel_date"])
    df.rename(columns={"order_book_id": "main_code", "related_id": "related_code"}, inplace=True)
    return df


@export_as_api(namespace="fund")
def get_etf_components(order_book_ids, trading_date=None, market="cn"):
    """获取etf基金份额数据

    :param order_book_ids: 基金代码，str 或 list of str
    :param trading_date: 交易日期，默认为当天
    :param market: (Default value = "cn")
    :return: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids)
    ids_with_suffix = []
    for order_book_id in order_book_ids:
        if order_book_id.endswith(".XSHG") or order_book_id.endswith(".XSHE"):
            ids_with_suffix.append(order_book_id)
        elif order_book_id.startswith("1"):
            ids_with_suffix.append(order_book_id + ".XSHE")
        elif order_book_id.startswith("5"):
            ids_with_suffix.append(order_book_id + ".XSHG")
    if not ids_with_suffix:
        return

    if trading_date is not None:
        trading_date = to_date(trading_date)
        if trading_date > datetime.date.today():
            return
    else:
        trading_date = datetime.date.today()
    trading_date = ensure_date_int(ensure_trading_date(trading_date))

    result = get_client().execute("fund.get_etf_components_v2", ids_with_suffix, trading_date, market=market)
    if not result:
        return

    columns = ["trading_date", "order_book_id", "stock_code", "stock_amount", "cash_substitute",
               "cash_substitute_proportion", "fixed_cash_substitute"]
    df = pd.DataFrame(result, columns=columns)
    df.sort_values(by=["order_book_id", "trading_date", "stock_code"], inplace=True)
    df.set_index(["order_book_id", "trading_date"], inplace=True)
    return df


@export_as_api(namespace="fund")
def get_stock_change(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取基金报告期内重大股票持仓变动情况

    :param order_book_ids: 基金代码，str 或 list of str
    :param start_date: 开始日期 (Default value = None)
    :param end_date: 结束日期 (Default value = None)
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids, market)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)

    result = get_client().execute("fund.get_stock_change_v2", order_book_ids, start_date, end_date, market=market)
    if not result:
        return
    df = pd.DataFrame(result)
    df = df.set_index(["fund_id", "date"]).sort_index()
    df = df[['order_book_id', 'market_value', 'weight', 'change_type']]
    return df


@export_as_api(namespace="fund")
def get_term_to_maturity(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取货币型基金的持仓剩余期限

    :param order_book_ids: 基金代码，str 或 list of str
    :param start_date: 开始日期 (Default value = None)
    :param end_date: 结束日期 (Default value = None)
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids, market)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)

    result = get_client().execute("fund.get_term_to_maturity", order_book_ids, start_date, end_date, market=market)
    if result:
        result = [i for i in result if i is not None]
    if not result:
        return
    df = pd.DataFrame(result)
    df = df[['order_book_id', 'date', '0_30', '30_60', '60_90', '90_120', '120_397', '90_180', '>180']]
    df.set_index(['order_book_id', 'date'], inplace=True)
    df = df.stack().reset_index()
    if pd_version >= "0.21":
        df.set_axis(['order_book_id', 'date', 'term', 'weight'], axis=1, inplace=True)
    else:
        df.set_axis(1, ['order_book_id', 'date', 'term', 'weight'])
    df = df.set_index(['order_book_id', 'date']).sort_index()
    df.dropna(inplace=True)
    return df


@export_as_api(namespace="fund")
def get_bond_stru(order_book_ids, date=None, market="cn"):
    """获取指定日期公募基金债券持仓券种明细信息

    :param order_book_ids: 基金代码，str 或 list of str
    :param date: 日期，为空则返回所有时间段的数据 (Default value = None)
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids)
    if date is not None:
        date = ensure_date_int(date)

    data = get_client().execute("fund.get_bond_stru_v2", order_book_ids, date, market=market)
    if not data:
        return
    df = pd.DataFrame(data)
    df = df.set_index(["order_book_id", "date"]).sort_index()
    df = df[['bond_type', 'weight_nv', 'weight_bond_mv', 'market_value']]
    return df


AMC_FIELDS = ["amc_id", "amc", "establishment_date", "reg_capital"]


@export_as_api(namespace="fund")
def get_amc(amc_ids=None, fields=None, market="cn"):
    """获取基金公司详情信息

    :param amc_ids: 基金公司id或简称，默认为 None
    :param fields: 可选参数。默认为所有字段。 (Default value = None)
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    if fields is None:
        fields = AMC_FIELDS
    else:
        fields = ensure_list_of_string(fields)
        check_items_in_container(fields, AMC_FIELDS, "fields")

    result = get_client().execute("fund.get_amc", market=market)
    if amc_ids:
        amc_ids = ensure_list_of_string(amc_ids)
        amcs = tuple(amc_ids)
        result = [i for i in result if i["amc_id"] in amc_ids or i["amc"].startswith(amcs)]

    if not result:
        return
    return pd.DataFrame(result)[fields]


@export_as_api(namespace="fund")
def get_credit_quality(order_book_ids, date=None, market="cn"):
    """获取基金信用风险数据信息

    :param order_book_ids: 基金代码，str 或 list of str
    :param date: 交易日期，默认返回所有
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids)
    if date:
        date = ensure_date_int(date)

    result = get_client().execute("fund.get_credit_quality", order_book_ids, date, market=market)
    if not result:
        return

    df = pd.DataFrame(result)
    df.sort_values(["order_book_id", "date", "t_type", "credit_rating"], inplace=True)
    df.set_index(["order_book_id", "date"], inplace=True)
    return df


@export_as_api(namespace="fund")
def get_irr_sensitivity(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取基金利率风险敏感性分析数据

    :param order_book_ids: 基金代码，str 或 list of str
    :param start_date: 开始日期, 如'2013-01-04'
    :param end_date: 结束日期, 如'2014-01-04'；在 start_date 和 end_date 都不指定的情况下，默认为最近6个月
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids)
    start_date, end_date = ensure_date_range(start_date, end_date, delta=relativedelta(months=6))
    result = get_client().execute("fund.get_irr_sensitivity_v2", order_book_ids, start_date, end_date, market=market)
    if not result:
        return

    df = pd.DataFrame(result)
    df = df.set_index(["order_book_id", "date"]).sort_index()
    return df


@export_as_api(namespace="fund")
def get_etf_cash_components(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取现金差额数据

    :param order_book_ids: 基金代码，str 或 list of str
    :param start_date: 开始日期, 如'2013-01-04', 如果不传入, 则默认不限制开始日期
    :param end_date: 结束日期, 如'2014-01-04', 如果不传入, 则默认为今天
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids)

    # 用户可能传入不带后缀的id, 这里统一处理成带后缀的id.
    for indx in range(len(order_book_ids)):
        if order_book_ids[indx].endswith(".XSHG") or order_book_ids[indx].endswith(".XSHE"):
            pass
        elif order_book_ids[indx].startswith("1"):
            order_book_ids[indx] = order_book_ids[indx] + ".XSHE"
        elif order_book_ids[indx].startswith("5"):
            order_book_ids[indx] = order_book_ids[indx] + ".XSHG"
        else:
            pass

    if end_date is None:
        end_date = datetime.date.today()
    end_date = ensure_date_int(end_date)

    if start_date is not None:
        start_date = ensure_date_int(start_date)

    result = get_client().execute(
        "fund.get_etf_cash_components", order_book_ids, start_date, end_date, market=market
    )
    if not result:
        return None
    df = pd.DataFrame.from_records(result, index=["order_book_id", "date"])
    df.sort_index(inplace=True)
    return df


AMC_TYPES = ['total', 'equity', 'hybrid', 'bond', 'monetary', 'shortbond', 'qdii']


@export_as_api(namespace="fund")
def get_amc_rank(amc_ids, date=None, type=None, market="cn"):
    """获取基金公司排名

    :param amc_ids: 基金公司代码，str or list
    :param date: 规模截止时间
    :param type: 基金类型，str or list
    :param market: (Default value = "cn")
    :return: DataFrame
    """
    amc_ids = ensure_list_of_string(amc_ids)
    if date:
        date = ensure_date_int(date)
    if type is not None:
        type = ensure_list_of_string(type)
        check_items_in_container(type, AMC_TYPES, "type")

    result = get_client().execute("fund.get_amc_rank", amc_ids, date, type, market=market)
    if not result:
        return
    df = pd.DataFrame(result)
    df = df.set_index(keys=['amc_id', 'type'])
    df.sort_values('date', inplace=True)
    return df


@export_as_api(namespace="fund")
def get_holder_structure(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取基金持有人结构

    :param order_book_ids: 基金代码，str 或 list of str
    :param start_date: 开始日期, 如'2013-01-04', 如果不传入, 则默认不限制开始日期
    :param end_date: 结束日期, 如'2014-01-04', 如果不传入, 则默认为今天
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids)

    if end_date is not None:
        end_date = ensure_date_int(end_date)

    if start_date is not None:
        start_date = ensure_date_int(start_date)

    result = get_client().execute(
        "fund.get_holder_structure", order_book_ids, start_date, end_date, market=market)
    if not result:
        return None
    df = pd.DataFrame.from_records(result, index=["order_book_id", "date"])
    df.sort_index(inplace=True)
    return df


@export_as_api(namespace="fund")
def get_qdii_scope(order_book_ids, start_date=None, end_date=None, market="cn"):
    """获取QDII地区配置

    :param order_book_ids: 基金代码，str 或 list of str
    :param start_date: 开始日期, 如'2013-01-04', 如果不传入, 则默认不限制开始日期
    :param end_date: 结束日期, 如'2014-01-04', 如果不传入, 则默认为今天
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids)

    if end_date is not None:
        end_date = ensure_date_int(end_date)

    if start_date is not None:
        start_date = ensure_date_int(start_date)

    result = get_client().execute(
        "fund.get_qdii_scope", order_book_ids, start_date, end_date, market=market)
    if not result:
        return None
    df = pd.DataFrame.from_records(result, index=["order_book_id", "date"])
    df.sort_index(inplace=True)
    return df


@export_as_api(namespace="fund")
def get_benchmark(order_book_ids, market="cn"):
    """获取基金基准

    :param order_book_ids: 基金代码，str 或 list of str
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids)

    result = get_client().execute(
        "fund.get_benchmark", order_book_ids, market=market)
    if not result:
        return None
    df = pd.DataFrame.from_records(result, index=["order_book_id", "start_date"])
    df.sort_index(inplace=True)
    return df


@export_as_api(namespace='fund')
def get_instrument_category(order_book_ids, date=None, category_type=None, source='gildata', market="cn"):
    """获取合约所属风格分类

    :param order_book_ids: 单个合约字符串或合约列表，如 '000001' 或 ['000001', '000003']
    :param date: 日期字符串，格式如 '2015-01-07' 或 '20150107'，若不指定，则为当天
    :param category_type: 可传入list，不指定则返回全部。可选：价值风格-value，规模风格-size，操作风格-operating_style，
                          久期分布-duration，券种配置-bond_type, 基金行业板块-industry_citics, 基金概念板块-concept,
                          基金投资风格-investment_style, 基金属性-universe, 分级基金标签-structured_fund, 基金分类-fund_type
    :param source: 分类来源。gildata: 聚源
    :param market:  (Default value = "cn")
    :returns: DataFrame
        返回合约指定的日期中所属风格分类
    """

    order_book_ids = ensure_list_of_string(order_book_ids)

    if date:
        date = ensure_date_int(date)

    category_types = {
        'value', 'size', 'operating_style', 'duration', 'bond_type',
        'industry_citics', 'concept', 'investment_style', 'universe', 'structured_fund'
    }

    base_category_types = category_types.copy()
    base_category_types.add("fund_type")

    if category_type is None:
        category_type = category_types
    category_type = ensure_list_of_string(category_type)

    if 1 < len(category_type) and "fund_type" in category_type:
        raise ValueError("'fund_type' can only be searched independently.")

    check_items_in_container(category_type, base_category_types, 'category_type')

    source = ensure_string_in(source, {'gildata'}, 'source')

    result = get_client().execute('fund.get_instrument_category', order_book_ids, date, category_type, source,
                                  market=market)

    if not result:
        return

    if "fund_type" in category_type:
        c = [
            "order_book_id", "category_type", "first_type_code", "first_type", "second_type_code", "second_type",
            "third_type_code", "third_type"
        ]
        df = pd.DataFrame.from_records(result, index=['order_book_id', 'category_type'], columns=c)
    else:
        df = pd.DataFrame.from_records(result, index=['order_book_id', 'category_type'])
    return df


@export_as_api(namespace='fund')
def get_category(category, date=None, source='gildata', market='cn'):
    """获取指定分类下所属基金列表

    :param category: 风格类型映射, 如{"concept": ["食品饮料", "国防军工"], "investment_style": "沪港深混合型"}
    :param date: 如 '2015-01-07' 或 '20150107' (Default value = None)
    :param source: 分类来源。gildata: 聚源
    :param market:  (Default value = "cn")
    :returns: DataFrame
    """
    category_keys = {
        'value', 'size', 'operating_style', 'duration', 'bond_type', 'fund_type',
        'industry_citics', 'concept', 'investment_style', 'universe', 'structured_fund'
    }

    if date:
        date = ensure_date_int(date)

    check_type(category, dict, "category")
    source = ensure_string_in(source, {'gildata'}, 'source')

    check_items_in_container(category, category_keys, 'category')
    for k in category:
        category[k] = ensure_list_of_string(category[k])

    category_types_map = defaultdict(list)

    category_type_copy = list(category.keys())
    if "fund_type" in category_type_copy:
        fund_type_df = get_category_mapping(category_type="fund_type")
        category_types_map["fund_type"].extend(fund_type_df.values.flatten().tolist())
        category_type_copy.remove("fund_type")

    if category_type_copy:
        df = get_category_mapping(category_type=category_type_copy)
        unique_index = df.index.unique()
        for idx in unique_index:
            category_types_map[idx].extend([_ for _ in df.loc[idx].values.flatten() if _ is not None])

    for c in category:
        if not set(category[c]).issubset(set(category_types_map[c])):
            raise ValueError("Unexpected category.")
    return get_client().execute('fund.get_category_v2', category, date, source, market=market)


@export_as_api(namespace='fund')
def get_category_mapping(source='gildata', category_type=None, market="cn"):
    """获取风格分类列表概览

    :param source: 分类来源。gildata: 聚源
    :param category_type: 风格类型, (Default value = ['value', 'size', 'universe', 'bond_type', 'concept', 'duration', 'structured_fund', 'operating_style', 'industry_citics', 'investment_style'])
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """

    source = ensure_string_in(source, {'gildata'}, 'source')

    # 参数category_type为None时默认取除fund_type以外的风格, 因为fund_type不能和其他风格共存
    category_types = {
        'value', 'size', 'operating_style', 'duration', 'bond_type',
        'industry_citics', 'concept', 'investment_style', 'universe', 'structured_fund'
    }

    if category_type is None:
        category_type = category_types
    category_type = ensure_list_of_string(category_type)

    if "fund_type" in category_type:
        assert len(category_type) == 1, "'fund_type' can only be searched independently."
    else:
        check_items_in_container(category_type, category_types, 'category_type')

    result = get_client().execute("fund.get_category_mapping", source, market=market)
    if not result:
        return
    if "fund_type" in category_type:
        columns = [
            "first_type_code", "first_type", "second_type_code", "second_type", "third_type_code", "third_type",
            "category_type"
        ]
        df = pd.DataFrame(result, columns=columns)
        df = df["fund_type" == df["category_type"]]
    else:
        columns = ["category", "category_index", "category_type"]
        df = pd.DataFrame(result, columns=columns)
        df = df[df["category_type"].isin(category_type)]

    df.set_index(keys=["category_type"], inplace=True)
    return df


@export_as_api(namespace="fund")
def get_benchmark_price(order_book_ids, start_date=None, end_date=None, market="cn"):
    """
    获取基金 benchmark 价格

    :param order_book_ids: 基金order_book_id, str or list
    :param start_date: 开始日期，不指定则不限制开始日期
    :param end_date: 结束日期，不指定则不限制结束日期
    :param market: (Default value = "cn")
    :return: DataFrame
    """
    order_book_ids = ensure_list_of_string(order_book_ids)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)

    result = get_client().execute(
        "fund.get_benchmark_price", order_book_ids, start_date, end_date, market=market)
    if not result:
        return

    df = pd.DataFrame(result)
    df = df.set_index(["order_book_id", "date"]).sort_index()
    return df


FINANCIALS_FIELDS = [
    'accts_payable',
    'accts_receivable',
    'cash_equivalent',
    'deferred_expense',
    'deferred_income_tax_assets',
    'deferred_income_tax_liabilities',
    'dividend_receivable',
    'financial_asset_held_for_trading',
    'financial_liabilities',
    'interest_payable',
    'interest_receivable',
    'leverage',
    'management_fee_payable',
    'other_accts_payable',
    'other_accts_receivable',
    'other_assets',
    'other_equity',
    'other_liabilities',
    'paid_in_capital',
    'profit_payable',
    'redemption_fee_payable',
    'redemption_money_payable',
    'sales_fee_payable',
    'stock_cost',
    'stock_income',
    'tax_payable',
    'total_asset',
    'total_equity',
    'total_equity_and_liabilities',
    'total_liabilities',
    'transaction_fee_payable',
    'trust_fee_payable',
    'undistributed_profit'
]


@export_as_api(namespace="fund")
def get_financials(order_book_ids, start_date=None, end_date=None, fields=None, market="cn"):
    """
    获取基金财务数据

    :param order_book_ids: 基金order_book_id, str or list
    :param start_date: 开始日期，默认为 None
    :param end_date: 结束日期，默认为 None
    :param fields: 字段, 默认为None
    :param market: (Default value = "cn")
    :return: DataFrame
    """
    order_book_ids = ensure_list_of_string(order_book_ids)
    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)

    if fields is None:
        fields = FINANCIALS_FIELDS
    else:
        fields = ensure_list_of_string(fields)
        check_items_in_container(fields, FINANCIALS_FIELDS, "fields")
    result = get_client().execute(
        "fund.get_financials", order_book_ids, start_date, end_date, fields, market=market)
    if not result:
        return

    df = pd.DataFrame(result)
    df = df.set_index(["order_book_id", "date"]).sort_index()
    return df


FEE_FIELDS = [
    "purchase_fee",
    "subscription_fee",
    "redemption_fee",
    "management_fee",
    "custodian_fee",
    "sales_service_fee",
]


@export_as_api(namespace="fund")
def get_fee(order_book_ids, fee_type=None, charge_type="front", date=None, market="cn"):
    """
    获取基金费率信息

    :param order_book_ids: 基金order_book_id, str or list
    :param charge_type: 前后端费率(front, back), default=front
    :param fee_type: 费率类型，默认返回所有
    :param date: 日期, 默认为None
    :param market:  (Default value = "cn")
    :return: DataFrame
    """
    order_book_ids = ensure_list_of_string(order_book_ids)
    if date:
        date = ensure_date_int(date)
    if fee_type is None:
        fee_type = FEE_FIELDS
    else:
        fee_type = ensure_list_of_string(fee_type)
        check_items_in_container(fee_type, FEE_FIELDS, "fields")

    check_items_in_container(charge_type, ["front", "back"], "charge_type")

    result = get_client().execute("fund.get_fee_v2", order_book_ids, fee_type, charge_type, date, market=market)
    if not result:
        return
    df = pd.DataFrame(result)
    df.set_index(["order_book_id", "fee_type"], inplace=True)
    df.sort_index(inplace=True)
    columns = [
        'fee_ratio', 'fee_value',
        'inv_floor', 'inv_cap',
        'share_floor', 'share_cap',
        'holding_period_floor', 'holding_period_cap',
        'return_floor', 'return_cap'
    ]
    df = df[columns]
    return df


@export_as_api(namespace="fund")
def get_transition_info(order_book_ids, market="cn"):
    """
    获取基金转型信息

    :param order_book_ids: 基金order_book_id, str or list
    :param market:  (Default value = "cn")
    :return: DataFrame
    """

    def _handler(group):
        if 1 < len(group):
            group.sort_values("transition_time", inplace=True)
            group.drop_duplicates(subset=["order_book_id"], inplace=True)
            return group
        return group

    order_book_ids = ensure_list_of_string(order_book_ids)
    result = get_client().execute("fund.get_transition_info", order_book_ids, market=market)
    if not result:
        return
    df = pd.DataFrame(result)
    df["order_book_id"] = df["order_book_id"].apply(lambda x: x[0:6])
    df = df.groupby(["order_book_id", "establishment_date"], as_index=False, group_keys=False).apply(_handler)
    # 未曾转型的基金不需要返回
    df = df.groupby(["order_book_id"], as_index=False, group_keys=False).apply(
        lambda g: g if 1 < len(g) else pd.DataFrame())
    df.dropna(inplace=True)
    if 0 == len(df):
        return
    df["transition_time"] = df["transition_time"].astype(int)
    df.set_index(["order_book_id", "transition_time"], inplace=True)
    return df


TRANSACTION_STATUS_FIELDS = [
    "subscribe_status",
    "redeem_status",
    "subscribe_upper_limit",
    "subscribe_lower_limit",
    "redeem_lower_limit",
]


@export_as_api(namespace="fund")
def get_transaction_status(order_book_ids, start_date=None, end_date=None, fields=None, investor="institution",
                           market="cn"):
    """获取基金净值数据

    :param order_book_ids: 基金代码，str 或 list of str
    :param start_date: 开始日期 (Default value = None)
    :param end_date: 结束日期 (Default value = None)
    :param fields: str or list of str，例如：'subscribe_status', 'redeem_status',
                    'subscribe_upper_limit', 'subscribe_lower_limit', 'redeem_lower_limit' (Default value = None)
    :param investor: 投资者身份(Default value = institution) institution/retail
    :param market:  (Default value = "cn")
    :returns: DataFrame

    """
    order_book_ids = ensure_list_of_string(order_book_ids)

    if start_date:
        start_date = ensure_date_int(start_date)
    if end_date:
        end_date = ensure_date_int(end_date)

    if fields is not None:
        fields = ensure_list_of_string(fields)
        check_items_in_container(fields, TRANSACTION_STATUS_FIELDS, 'invalid field')
    else:
        fields = TRANSACTION_STATUS_FIELDS

    result = get_client().execute(
        "fund.get_transaction_status", order_book_ids, start_date, end_date, fields, investor=investor, market=market
    )
    if not result:
        return
    result = pd.DataFrame(result)
    result.set_index(["order_book_id", "datetime"], inplace=True)
    result.sort_index(inplace=True)
    result = result[fields]
    result = result.where(result.notnull(), None)
    return result
